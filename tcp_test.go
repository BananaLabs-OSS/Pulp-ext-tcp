package tcpext

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func nextEvent(t *testing.T, m *manager, kind string) queuedEvent {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-m.events:
			if ev.kind == kind {
				return ev
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", kind)
		}
	}
}

func TestManagerAcceptReadWriteOwnershipAndClose(t *testing.T) {
	m := newManager(slog.Default())
	opened, err := m.listen("cell-a", listenRequest{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("tcp", opened.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	accepted := nextEvent(t, m, EventAccepted).value.(acceptedEvent)
	if accepted.RemoteAddr == "" || accepted.ConnectionID == 0 {
		t.Fatalf("accepted=%#v", accepted)
	}
	if err := m.startRead("cell-a", accepted.ConnectionID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.write("cell-b", writeRequest{ConnectionID: accepted.ConnectionID, Payload: []byte("forbidden")}); err == nil {
		t.Fatal("cross-cell write succeeded")
	}
	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	data := nextEvent(t, m, EventData).value.(dataEvent)
	if string(data.Payload) != "hello" {
		t.Fatalf("payload=%q", data.Payload)
	}
	if _, err := m.write("cell-a", writeRequest{ConnectionID: accepted.ConnectionID, Payload: []byte("world")}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(client, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "world" {
		t.Fatalf("reply=%q", buf)
	}
	if err := client.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	half := nextEvent(t, m, EventHalfClosed).value.(halfClosedEvent)
	if half.ConnectionID != accepted.ConnectionID || half.Direction != "read" {
		t.Fatalf("half-close=%#v", half)
	}
	if err := m.close("cell-a", accepted.ConnectionID); err != nil {
		t.Fatal(err)
	}
	nextEvent(t, m, EventClosed)
	if err := m.closeListener("cell-a", opened.ListenerID); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptedConnectionDoesNotDeliverDataBeforeStartRead(t *testing.T) {
	m := newManager(slog.Default())
	opened, err := m.listen("cell-a", listenRequest{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("tcp", opened.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("eager handshake")); err != nil {
		t.Fatal(err)
	}
	accepted := nextEvent(t, m, EventAccepted).value.(acceptedEvent)
	select {
	case ev := <-m.events:
		t.Fatalf("event arrived before read start: %s", ev.kind)
	case <-time.After(50 * time.Millisecond):
	}
	if err := m.startRead("cell-a", accepted.ConnectionID); err != nil {
		t.Fatal(err)
	}
	data := nextEvent(t, m, EventData).value.(dataEvent)
	if string(data.Payload) != "eager handshake" {
		t.Fatalf("payload=%q", data.Payload)
	}
}

func TestBindGuardFailsClosed(t *testing.T) {
	t.Setenv("TCP_BIND_ALLOW", "127.0.0.1")
	if err := checkBind("0.0.0.0:9000"); err == nil {
		t.Fatal("public bind succeeded without allowlist")
	}
	if err := checkBind("127.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionCapsReserveBeforeOpeningResources(t *testing.T) {
	m := newManager(slog.Default())
	m.maxPerCell, m.maxTotal = 1, 1
	if err := m.reserve("cell-a"); err != nil {
		t.Fatal(err)
	}
	if err := m.reserve("cell-a"); err == nil {
		t.Fatal("per-cell cap was not enforced")
	}
	if err := m.reserve("cell-b"); err == nil {
		t.Fatal("global cap was not enforced")
	}
	m.mu.Lock()
	m.releaseLocked("cell-a")
	m.mu.Unlock()
	if err := m.reserve("cell-b"); err != nil {
		t.Fatal(err)
	}
}

func TestConnectGuardRequiresExplicitPrivateAndLoopbackAccess(t *testing.T) {
	t.Setenv("TCP_CONNECT_ALLOW", "")
	for _, addr := range []string{"127.0.0.1:9000", "10.0.0.1:9000", "169.254.169.254:80"} {
		if _, err := resolveConnect(addr); err == nil {
			t.Fatalf("destination %s was allowed by default", addr)
		}
	}
	t.Setenv("TCP_CONNECT_ALLOW", "127.0.0.1/32,10.0.0.0/8")
	for _, addr := range []string{"127.0.0.1:9000", "10.0.0.1:9000"} {
		resolved, err := resolveConnect(addr)
		if err != nil || resolved.Port != 9000 {
			t.Fatalf("resolve %s = %v, %v", addr, resolved, err)
		}
	}
}
