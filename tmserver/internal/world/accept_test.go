package world

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/jeanluca/w2pp-openwyd/tmserver/internal/protocol"
)

// TestAcceptConnDeliversFrames drives a byte stream that did NOT come from the
// TCP accept loop (the shape a WebSocket edge produces) into the world and
// asserts the loop treats it like any other CPSock session: it allocates a
// session with the reported IP and routes the client's frames to the handler.
func TestAcceptConnDeliversFrames(t *testing.T) {
	type received struct {
		header protocol.Header
		conn   int
		ip     string
	}
	frames := make(chan received, 1)
	w := New(Config{GridDim: 16}, slogDiscard(), nil,
		func(_ *World, s *Session, h protocol.Header, _ []byte) {
			frames <- received{header: h, conn: s.Conn, ip: s.IP}
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	client, server := net.Pipe()
	defer client.Close()
	if !w.AcceptConn(server, "10.0.0.7:51000") {
		t.Fatal("AcceptConn returned false on a running world")
	}

	var initCode [4]byte
	binary.LittleEndian.PutUint32(initCode[:], protocol.InitCode)
	if _, err := client.Write(initCode[:]); err != nil {
		t.Fatalf("write INITCODE: %v", err)
	}
	wire, err := protocol.Encode(protocol.Header{Type: protocol.MsgAccountLogin}, make([]byte, 116), 7)
	if err != nil {
		t.Fatalf("encode login: %v", err)
	}
	if _, err := client.Write(wire); err != nil {
		t.Fatalf("write login frame: %v", err)
	}

	select {
	case got := <-frames:
		if got.header.Type != protocol.MsgAccountLogin {
			t.Errorf("handler saw Type = %#x, want MsgAccountLogin (%#x)", got.header.Type, protocol.MsgAccountLogin)
		}
		if got.ip != "10.0.0.7:51000" {
			t.Errorf("session IP = %q, want the address reported by the caller", got.ip)
		}
		if got.conn <= 0 {
			t.Errorf("session Conn = %d, want a positive player slot", got.conn)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler never saw the frame written over the accepted connection")
	}
}

// TestAcceptConnAfterShutdown asserts a connection handed over after the loop
// stopped is refused (and closed) instead of leaking a session.
func TestAcceptConnAfterShutdown(t *testing.T) {
	w := New(Config{GridDim: 16}, slogDiscard(), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	cancel()
	<-done

	client, server := net.Pipe()
	defer client.Close()
	if w.AcceptConn(server, "10.0.0.8:51001") {
		t.Fatal("AcceptConn returned true after the world stopped")
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("refused connection was left open")
	}
}
