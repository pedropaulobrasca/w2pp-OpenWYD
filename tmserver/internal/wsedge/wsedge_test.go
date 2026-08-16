package wsedge

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// captureAcceptor records the streams the edge hands over, standing in for
// world.World in tests.
type captureAcceptor struct {
	conns chan net.Conn
	ips   chan string
	deny  bool
}

func newCaptureAcceptor() *captureAcceptor {
	return &captureAcceptor{conns: make(chan net.Conn, 1), ips: make(chan string, 1)}
}

func (a *captureAcceptor) AcceptConn(c net.Conn, ip string) bool {
	if a.deny {
		_ = c.Close()
		return false
	}
	a.conns <- c
	a.ips <- ip
	return true
}

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func dialWS(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+srv.URL[len("http"):], nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	return c
}

// TestHandlerStreamsClientBytes asserts a browser's binary WebSocket messages
// reach the accepted net.Conn as a plain byte stream — CPSock framing spans
// messages, so the edge must never impose message boundaries on the reader.
func TestHandlerStreamsClientBytes(t *testing.T) {
	acceptor := newCaptureAcceptor()
	srv := httptest.NewServer(Handler(Config{Acceptor: acceptor, Log: discardLog()}))
	defer srv.Close()

	ws := dialWS(t, srv)
	// CloseNow: an abrupt drop, the shape a closed browser tab produces (a
	// graceful close would wait on a peer handshake the world never performs).
	defer ws.CloseNow()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ws.Write(ctx, websocket.MessageBinary, []byte{0x11, 0xF3}); err != nil {
		t.Fatalf("write first message: %v", err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, []byte{0x11, 0x1F}); err != nil {
		t.Fatalf("write second message: %v", err)
	}

	var conn net.Conn
	select {
	case conn = <-acceptor.conns:
	case <-time.After(3 * time.Second):
		t.Fatal("edge never handed the connection to the acceptor")
	}
	defer conn.Close()

	got := make([]byte, 4)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read stream: %v", err)
	}
	want := []byte{0x11, 0xF3, 0x11, 0x1F}
	if string(got) != string(want) {
		t.Errorf("stream = % x, want % x (the INITCODE split across two messages)", got, want)
	}
}

// TestHandlerStreamsServerBytes asserts frames written to the accepted conn
// arrive at the browser.
func TestHandlerStreamsServerBytes(t *testing.T) {
	acceptor := newCaptureAcceptor()
	srv := httptest.NewServer(Handler(Config{Acceptor: acceptor, Log: discardLog()}))
	defer srv.Close()

	ws := dialWS(t, srv)
	// CloseNow: an abrupt drop, the shape a closed browser tab produces (a
	// graceful close would wait on a peer handshake the world never performs).
	defer ws.CloseNow()

	var conn net.Conn
	select {
	case conn = <-acceptor.conns:
	case <-time.After(3 * time.Second):
		t.Fatal("edge never handed the connection to the acceptor")
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{0x0A, 0x01, 0x02}); err != nil {
		t.Fatalf("write to accepted conn: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typ, data, err := ws.Read(ctx)
	if err != nil {
		t.Fatalf("read from websocket: %v", err)
	}
	if typ != websocket.MessageBinary {
		t.Errorf("message type = %v, want binary", typ)
	}
	if string(data) != string([]byte{0x0A, 0x01, 0x02}) {
		t.Errorf("browser received % x, want 0a 01 02", data)
	}
}

// TestHandlerReportsRemoteIP asserts the session's reported address is the
// browser's, not the loopback of the HTTP server.
func TestHandlerReportsRemoteIP(t *testing.T) {
	acceptor := newCaptureAcceptor()
	srv := httptest.NewServer(Handler(Config{Acceptor: acceptor, Log: discardLog()}))
	defer srv.Close()

	ws := dialWS(t, srv)
	// CloseNow: an abrupt drop, the shape a closed browser tab produces (a
	// graceful close would wait on a peer handshake the world never performs).
	defer ws.CloseNow()

	select {
	case ip := <-acceptor.ips:
		if _, _, err := net.SplitHostPort(ip); err != nil {
			t.Errorf("reported ip = %q, want a host:port address: %v", ip, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("edge never reported an address")
	}
	// Close the stream like the world does when a session ends; the edge holds
	// the hijacked socket until then, and httptest.Server.Close waits for it.
	(<-acceptor.conns).Close()
}

// TestHandlerRejectsPlainHTTP asserts a non-WebSocket request is refused rather
// than becoming a half-open session.
func TestHandlerRejectsPlainHTTP(t *testing.T) {
	acceptor := newCaptureAcceptor()
	srv := httptest.NewServer(Handler(Config{Acceptor: acceptor, Log: discardLog()}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("plain GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Errorf("plain GET status = %d, want an upgrade failure", resp.StatusCode)
	}
	select {
	case <-acceptor.conns:
		t.Error("plain GET was handed to the world as a session")
	case <-time.After(200 * time.Millisecond):
	}
}
