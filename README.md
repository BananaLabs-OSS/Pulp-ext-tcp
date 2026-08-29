# Pulp TCP extension

Host-supervised `network.tcp` capability for Pulp cells. It owns listeners,
accepted and outbound connections, bounded event delivery, write deadlines,
half-close, connection limits, ownership enforcement, and lifecycle teardown.

Configuration:

- `TCP_BIND_ALLOW`: comma-separated listener hosts; defaults to wildcard binds.
- `TCP_CONNECT_ALLOW`: comma-separated IPs or CIDRs allowed for private,
  loopback, link-local, unspecified, or multicast destinations. These ranges
  are blocked by default. Hostnames are resolved once and the permitted IP is
  dialed directly to prevent a DNS change between policy check and connect.
- `TCP_MAX_CONNECTIONS_PER_CELL`: defaults to 1024.
- `TCP_MAX_CONNECTIONS_TOTAL`: defaults to 4096.
- `TCP_EVENT_QUEUE`: defaults to 4096 and applies backpressure when full.
