// Package tcpext provides supervised, bounded TCP streams to Pulp cells.
// The host owns listeners, connections, deadlines, buffers, and teardown;
// cells receive connection lifecycle/data events through the step loop.
package tcpext

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	EventAccepted   = "tcp.accepted"
	EventData       = "tcp.data"
	EventHalfClosed = "tcp.half_closed"
	EventClosed     = "tcp.closed"
	defaultChunk    = 32 * 1024
)

type listenRequest struct {
	Addr string `msgpack:"addr"`
}
type listenResponse struct {
	ListenerID uint64 `msgpack:"listener_id"`
	Addr       string `msgpack:"addr"`
}
type connectRequest struct {
	Addr      string `msgpack:"addr"`
	TimeoutMS int    `msgpack:"timeout_ms,omitempty"`
}
type connectResponse struct {
	ConnectionID uint64 `msgpack:"connection_id"`
	LocalAddr    string `msgpack:"local_addr"`
	RemoteAddr   string `msgpack:"remote_addr"`
}
type writeRequest struct {
	ConnectionID uint64 `msgpack:"connection_id"`
	Payload      []byte `msgpack:"payload"`
	TimeoutMS    int    `msgpack:"timeout_ms,omitempty"`
}
type writeResponse struct {
	BytesWritten int `msgpack:"bytes_written"`
}
type closeRequest struct {
	ID uint64 `msgpack:"id"`
}
type halfCloseRequest struct {
	ConnectionID uint64 `msgpack:"connection_id"`
	Direction    string `msgpack:"direction"`
}

type acceptedEvent struct {
	ListenerID   uint64 `msgpack:"listener_id"`
	ConnectionID uint64 `msgpack:"connection_id"`
	LocalAddr    string `msgpack:"local_addr"`
	RemoteAddr   string `msgpack:"remote_addr"`
	AcceptedAt   int64  `msgpack:"accepted_at"`
}
type dataEvent struct {
	ConnectionID uint64 `msgpack:"connection_id"`
	Payload      []byte `msgpack:"payload"`
	ReceivedAt   int64  `msgpack:"received_at"`
}
type closedEvent struct {
	ConnectionID uint64 `msgpack:"connection_id"`
	Reason       string `msgpack:"reason"`
	ClosedAt     int64  `msgpack:"closed_at"`
}
type halfClosedEvent struct {
	ConnectionID uint64 `msgpack:"connection_id"`
	Direction    string `msgpack:"direction"`
	At           int64  `msgpack:"at"`
}

type queuedEvent struct {
	kind, cellID string
	value        any
}
type listener struct {
	id     uint64
	cellID string
	conn   *net.TCPListener
	cancel context.CancelFunc
}
type connection struct {
	id     uint64
	cellID string
	conn   *net.TCPConn
	cancel context.CancelFunc
	once   sync.Once
}

type manager struct {
	logger               *slog.Logger
	mu                   sync.Mutex
	listeners            map[uint64]*listener
	connections          map[uint64]*connection
	perCell              map[string]int
	total                int
	nextID               atomic.Uint64
	events               chan queuedEvent
	maxPerCell, maxTotal int
}

var active *manager

func init() {
	ext.Register(ext.Capability{Name: "network.tcp", Provider: "github.com/BananaLabs-OSS/Pulp-ext-tcp", Register: register, Stub: stub, Setup: setup, Teardown: teardown, TeardownCell: teardownCell, Poll: poll})
}

func envInt(name string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func newManager(logger *slog.Logger) *manager {
	return &manager{logger: logger, listeners: map[uint64]*listener{}, connections: map[uint64]*connection{}, perCell: map[string]int{}, events: make(chan queuedEvent, envInt("TCP_EVENT_QUEUE", 4096)), maxPerCell: envInt("TCP_MAX_CONNECTIONS_PER_CELL", 1024), maxTotal: envInt("TCP_MAX_CONNECTIONS_TOTAL", 4096)}
}

func (m *manager) reserve(cellID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.total >= m.maxTotal {
		return fmt.Errorf("global TCP connection limit reached")
	}
	if m.perCell[cellID] >= m.maxPerCell {
		return fmt.Errorf("cell TCP connection limit reached")
	}
	m.perCell[cellID]++
	m.total++
	return nil
}

func (m *manager) releaseLocked(cellID string) {
	if m.perCell[cellID] <= 1 {
		delete(m.perCell, cellID)
	} else {
		m.perCell[cellID]--
	}
	if m.total > 0 {
		m.total--
	}
}

func (m *manager) emit(ctx context.Context, ev queuedEvent) bool {
	select {
	case m.events <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

func checkBind(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if port == "0" || host == "127.0.0.1" || host == "::1" || host == "localhost" {
		return nil
	}
	allowed := os.Getenv("TCP_BIND_ALLOW")
	if allowed == "" {
		allowed = "0.0.0.0,::"
	}
	if host == "" {
		host = "0.0.0.0"
	}
	for _, item := range strings.Split(allowed, ",") {
		if strings.TrimSpace(item) == host {
			return nil
		}
	}
	return fmt.Errorf("TCP bind %q is not allowlisted", addr)
}

func resolveConnect(addr string) (*net.TCPAddr, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, fmt.Errorf("invalid TCP destination port")
	}
	for _, ip := range ips {
		if connectIPAllowed(ip, os.Getenv("TCP_CONNECT_ALLOW")) {
			return &net.TCPAddr{IP: ip, Port: portNumber}, nil
		}
	}
	return nil, fmt.Errorf("TCP destination %q has no permitted address", host)
}

func connectIPAllowed(ip net.IP, allowlist string) bool {
	for _, raw := range strings.Split(allowlist, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if exact := net.ParseIP(raw); exact != nil && exact.Equal(ip) {
			return true
		}
		if _, network, err := net.ParseCIDR(raw); err == nil && network.Contains(ip) {
			return true
		}
	}
	return !(ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsMulticast() || ip.IsPrivate())
}

func (m *manager) listen(cellID string, req listenRequest) (listenResponse, error) {
	if err := checkBind(req.Addr); err != nil {
		return listenResponse{}, err
	}
	addr, err := net.ResolveTCPAddr("tcp", req.Addr)
	if err != nil {
		return listenResponse{}, err
	}
	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return listenResponse{}, err
	}
	id := m.nextID.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	l := &listener{id: id, cellID: cellID, conn: ln, cancel: cancel}
	m.mu.Lock()
	m.listeners[id] = l
	m.mu.Unlock()
	go m.acceptLoop(ctx, l)
	return listenResponse{ListenerID: id, Addr: ln.Addr().String()}, nil
}

func (m *manager) acceptLoop(ctx context.Context, l *listener) {
	for {
		_ = l.conn.SetDeadline(time.Now().Add(time.Second))
		conn, err := l.conn.AcceptTCP()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		if err := m.reserve(l.cellID); err != nil {
			_ = conn.Close()
			continue
		}
		c := m.addConnection(l.cellID, conn)
		if !m.emit(ctx, queuedEvent{kind: EventAccepted, cellID: l.cellID, value: acceptedEvent{ListenerID: l.id, ConnectionID: c.id, LocalAddr: conn.LocalAddr().String(), RemoteAddr: conn.RemoteAddr().String(), AcceptedAt: time.Now().UnixNano()}}) {
			m.closeConnection(c, "listener stopped")
		}
	}
}

func (m *manager) addConnection(cellID string, conn *net.TCPConn) *connection {
	id := m.nextID.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	c := &connection{id: id, cellID: cellID, conn: conn, cancel: cancel}
	m.mu.Lock()
	m.connections[id] = c
	m.mu.Unlock()
	go m.readLoop(ctx, c)
	return c
}

func (m *manager) connect(cellID string, req connectRequest) (connectResponse, error) {
	destination, err := resolveConnect(req.Addr)
	if err != nil {
		return connectResponse{}, err
	}
	if err := m.reserve(cellID); err != nil {
		return connectResponse{}, err
	}
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout <= 0 || timeout > 30*time.Second {
		timeout = 5 * time.Second
	}
	dialer := net.Dialer{Timeout: timeout}
	raw, err := dialer.Dial("tcp", destination.String())
	if err != nil {
		m.mu.Lock()
		m.releaseLocked(cellID)
		m.mu.Unlock()
		return connectResponse{}, err
	}
	c := m.addConnection(cellID, raw.(*net.TCPConn))
	return connectResponse{ConnectionID: c.id, LocalAddr: raw.LocalAddr().String(), RemoteAddr: raw.RemoteAddr().String()}, nil
}

func (m *manager) readLoop(ctx context.Context, c *connection) {
	buf := make([]byte, defaultChunk)
	for {
		n, err := c.conn.Read(buf)
		if n > 0 {
			payload := append([]byte(nil), buf[:n]...)
			if !m.emit(ctx, queuedEvent{kind: EventData, cellID: c.cellID, value: dataEvent{ConnectionID: c.id, Payload: payload, ReceivedAt: time.Now().UnixNano()}}) {
				return
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) && ctx.Err() == nil {
				m.emit(ctx, queuedEvent{kind: EventHalfClosed, cellID: c.cellID, value: halfClosedEvent{ConnectionID: c.id, Direction: "read", At: time.Now().UnixNano()}})
				return
			}
			reason := "connection closed"
			if ctx.Err() == nil {
				reason = err.Error()
			}
			m.closeConnection(c, reason)
			return
		}
	}
}

func (m *manager) owned(cellID string, id uint64) (*connection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.connections[id]
	if c == nil {
		return nil, fmt.Errorf("unknown TCP connection")
	}
	if c.cellID != cellID {
		return nil, fmt.Errorf("TCP connection not owned by caller")
	}
	return c, nil
}

func (m *manager) write(cellID string, req writeRequest) (int, error) {
	c, err := m.owned(cellID, req.ConnectionID)
	if err != nil {
		return 0, err
	}
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout <= 0 || timeout > 30*time.Second {
		timeout = 5 * time.Second
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(timeout))
	written := 0
	for written < len(req.Payload) {
		n, err := c.conn.Write(req.Payload[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (m *manager) halfClose(cellID string, req halfCloseRequest) error {
	c, err := m.owned(cellID, req.ConnectionID)
	if err != nil {
		return err
	}
	switch req.Direction {
	case "read":
		return c.conn.CloseRead()
	case "write":
		return c.conn.CloseWrite()
	default:
		return fmt.Errorf("direction must be read or write")
	}
}

func (m *manager) closeConnection(c *connection, reason string) {
	c.once.Do(func() {
		c.cancel()
		_ = c.conn.Close()
		m.mu.Lock()
		delete(m.connections, c.id)
		m.releaseLocked(c.cellID)
		m.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		m.emit(ctx, queuedEvent{kind: EventClosed, cellID: c.cellID, value: closedEvent{ConnectionID: c.id, Reason: reason, ClosedAt: time.Now().UnixNano()}})
	})
}

func (m *manager) close(cellID string, id uint64) error {
	c, err := m.owned(cellID, id)
	if err != nil {
		return err
	}
	m.closeConnection(c, "closed by cell")
	return nil
}

func (m *manager) closeListener(cellID string, id uint64) error {
	m.mu.Lock()
	l := m.listeners[id]
	if l == nil || l.cellID != cellID {
		m.mu.Unlock()
		return fmt.Errorf("unknown or unowned TCP listener")
	}
	delete(m.listeners, id)
	m.mu.Unlock()
	l.cancel()
	return l.conn.Close()
}

func (m *manager) closeCell(cellID string) {
	m.mu.Lock()
	var ls []*listener
	var cs []*connection
	for id, l := range m.listeners {
		if l.cellID == cellID {
			ls = append(ls, l)
			delete(m.listeners, id)
		}
	}
	for _, c := range m.connections {
		if c.cellID == cellID {
			cs = append(cs, c)
		}
	}
	m.mu.Unlock()
	for _, l := range ls {
		l.cancel()
		_ = l.conn.Close()
	}
	for _, c := range cs {
		m.closeConnection(c, "cell stopped")
	}
}

func setup(env ext.SetupEnv) error { active = newManager(env.Logger); return nil }
func teardown(ctx context.Context) error {
	if active == nil {
		return nil
	}
	active.mu.Lock()
	cells := map[string]bool{}
	for _, l := range active.listeners {
		cells[l.cellID] = true
	}
	for _, c := range active.connections {
		cells[c.cellID] = true
	}
	active.mu.Unlock()
	for cell := range cells {
		active.closeCell(cell)
	}
	active = nil
	return nil
}
func teardownCell(_ context.Context, cellID string) error {
	if active != nil {
		active.closeCell(cellID)
	}
	return nil
}
func poll() (ext.StepEvent, bool) {
	if active == nil {
		return ext.StepEvent{}, false
	}
	select {
	case ev := <-active.events:
		payload, err := msgpack.Marshal(ev.value)
		if err != nil {
			return ext.StepEvent{}, false
		}
		return ext.StepEvent{Kind: ev.kind, Payload: payload, CellID: ev.cellID}, true
	default:
		return ext.StepEvent{}, false
	}
}

func request(m api.Module, ptr, len uint32, out any) uint32 {
	if len == 0 {
		return 1
	}
	b, ok := m.Memory().Read(ptr, len)
	if !ok {
		return 2
	}
	if msgpack.Unmarshal(b, out) != nil {
		return 3
	}
	return 0
}
func respond(ctx context.Context, m api.Module, value any, outPtr, outLen uint32) uint32 {
	b, err := msgpack.Marshal(value)
	if err != nil {
		return 5
	}
	alloc := m.ExportedFunction("pulp_alloc")
	if alloc == nil {
		return 7
	}
	result, err := alloc.Call(ctx, uint64(len(b)))
	if err != nil || len(result) == 0 || result[0] == 0 {
		return 7
	}
	ptr := uint32(result[0])
	if !m.Memory().Write(ptr, b) || !m.Memory().WriteUint32Le(outPtr, ptr) || !m.Memory().WriteUint32Le(outLen, uint32(len(b))) {
		return 8
	}
	return 0
}

func register(b wazero.HostModuleBuilder, cell ext.Cell) error {
	cellID := ext.CellIDOf(cell)
	b.NewFunctionBuilder().WithFunc(func(ctx context.Context, m api.Module, p, n, op, ol uint32) uint32 {
		if active == nil {
			return 10
		}
		var req listenRequest
		if c := request(m, p, n, &req); c != 0 {
			return c
		}
		v, e := active.listen(cellID, req)
		if e != nil {
			return 4
		}
		return respond(ctx, m, v, op, ol)
	}).Export("tcp_listen")
	b.NewFunctionBuilder().WithFunc(func(ctx context.Context, m api.Module, p, n, op, ol uint32) uint32 {
		if active == nil {
			return 10
		}
		var req connectRequest
		if c := request(m, p, n, &req); c != 0 {
			return c
		}
		v, e := active.connect(cellID, req)
		if e != nil {
			return 4
		}
		return respond(ctx, m, v, op, ol)
	}).Export("tcp_connect")
	b.NewFunctionBuilder().WithFunc(func(ctx context.Context, m api.Module, p, n, op, ol uint32) uint32 {
		if active == nil {
			return 10
		}
		var req writeRequest
		if c := request(m, p, n, &req); c != 0 {
			return c
		}
		v, e := active.write(cellID, req)
		if e != nil {
			return 4
		}
		return respond(ctx, m, writeResponse{BytesWritten: v}, op, ol)
	}).Export("tcp_write")
	b.NewFunctionBuilder().WithFunc(func(_ context.Context, m api.Module, p, n uint32) uint32 {
		if active == nil {
			return 10
		}
		var req halfCloseRequest
		if c := request(m, p, n, &req); c != 0 {
			return c
		}
		if active.halfClose(cellID, req) != nil {
			return 4
		}
		return 0
	}).Export("tcp_half_close")
	b.NewFunctionBuilder().WithFunc(func(_ context.Context, m api.Module, p, n uint32) uint32 {
		if active == nil {
			return 10
		}
		var req closeRequest
		if c := request(m, p, n, &req); c != 0 {
			return c
		}
		if active.close(cellID, req.ID) != nil {
			return 4
		}
		return 0
	}).Export("tcp_close")
	b.NewFunctionBuilder().WithFunc(func(_ context.Context, m api.Module, p, n uint32) uint32 {
		if active == nil {
			return 10
		}
		var req closeRequest
		if c := request(m, p, n, &req); c != 0 {
			return c
		}
		if active.closeListener(cellID, req.ID) != nil {
			return 4
		}
		return 0
	}).Export("tcp_listener_close")
	return nil
}
func stub(b wazero.HostModuleBuilder, _ ext.Cell) error {
	nop4 := func(context.Context, api.Module, uint32, uint32, uint32, uint32) uint32 { return 99 }
	nop2 := func(context.Context, api.Module, uint32, uint32) uint32 { return 99 }
	b.NewFunctionBuilder().WithFunc(nop4).Export("tcp_listen")
	b.NewFunctionBuilder().WithFunc(nop4).Export("tcp_connect")
	b.NewFunctionBuilder().WithFunc(nop4).Export("tcp_write")
	b.NewFunctionBuilder().WithFunc(nop2).Export("tcp_half_close")
	b.NewFunctionBuilder().WithFunc(nop2).Export("tcp_close")
	b.NewFunctionBuilder().WithFunc(nop2).Export("tcp_listener_close")
	return nil
}
