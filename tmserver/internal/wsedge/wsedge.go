// Package wsedge exposes the CPSock client edge over WebSocket so a browser can
// play. A browser cannot open a raw TCP socket, and the whole point of the web
// client is running the game on macOS/Linux/Windows without WYD.exe — so the
// transport (and only the transport) changes: the WebSocket payload is the very
// same CPSock byte stream the TCP edge carries, INITCODE included, and the world
// loop cannot tell the two apart.
//
// This package owns no game state: it terminates the HTTP upgrade, turns the
// socket into a net.Conn and hands it to the world through Acceptor.
package wsedge

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// defaultReadLimit caps a single inbound WebSocket message. The largest legacy
// C→S message is well under 4 KiB (protocol-spec.md §4), so this is generous
// while still refusing a client that tries to buffer the server to death.
const defaultReadLimit = 32 * 1024

// Acceptor receives an established CPSock byte stream. It is implemented by
// world.World (AcceptConn), kept as an interface here so the edge never imports
// the world package.
type Acceptor interface {
	AcceptConn(c net.Conn, ip string) bool
}

// Config wires the edge. Acceptor is required.
type Config struct {
	Acceptor Acceptor
	Log      *slog.Logger

	// AllowedOrigins are the browser origins permitted to connect, as host
	// patterns ("play.example.com", "*.example.com"). Empty means same-origin
	// only — the safe default: the page and the socket must share a host, so a
	// hostile site cannot ride a logged-in player's session.
	AllowedOrigins []string

	// ReadLimit caps one inbound message in bytes (0 = defaultReadLimit).
	ReadLimit int64
}

// Handler returns the HTTP handler that upgrades to WebSocket and hands the
// resulting stream to the world. Mount it on its own listener (or behind TLS as
// wss://) — it is a client edge, never an internal link.
func Handler(cfg Config) http.Handler {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	readLimit := cfg.ReadLimit
	if readLimit <= 0 {
		readLimit = defaultReadLimit
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			OriginPatterns: cfg.AllowedOrigins,
		})
		if err != nil {
			// Accept already answered the request with the failure status.
			log.Info("websocket upgrade refused", "ip", r.RemoteAddr, "err", err)
			return
		}
		c.SetReadLimit(readLimit)

		// The stream outlives the handler's request context (which is cancelled
		// as soon as we return), so give the conn its own cancellable context and
		// block here until the session ends — that keeps the hijacked socket
		// owned by exactly one place.
		ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
		defer cancel()

		conn := &sessionConn{
			Conn: websocket.NetConn(ctx, c, websocket.MessageBinary),
			ws:   c,
			done: make(chan struct{}),
		}
		log.Info("websocket cpsock connection", "ip", r.RemoteAddr)
		if !cfg.Acceptor.AcceptConn(conn, r.RemoteAddr) {
			return
		}
		<-conn.done
	})
}

// sessionConn is the accepted stream. It closes done on Close so the HTTP
// handler that owns the hijacked socket returns exactly when the world drops
// the session.
type sessionConn struct {
	net.Conn
	ws     *websocket.Conn
	done   chan struct{}
	mu     sync.Mutex
	closed bool
}

// Close drops the socket abruptly (CloseNow) instead of running the WebSocket
// close handshake: the handshake waits up to five seconds for a peer that, on
// the disconnect paths that matter (dead client, killed session), will never
// answer — and that stall would pin the session's writer goroutine.
//
// Safe from either side: the world closes from its writer goroutine, while the
// edge closes when the handler unwinds.
func (s *sessionConn) Close() error {
	s.mu.Lock()
	first := !s.closed
	s.closed = true
	s.mu.Unlock()
	if !first {
		return nil
	}
	close(s.done)
	return s.ws.CloseNow()
}
