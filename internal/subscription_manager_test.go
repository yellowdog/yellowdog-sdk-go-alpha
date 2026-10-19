package internal

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeManagedSSEClient struct {
	ready   chan struct{}
	stopped chan struct{}
	handler func(StreamEvent)
}

func newFakeManagedSSEClient() *fakeManagedSSEClient {
	return &fakeManagedSSEClient{
		ready:   make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

// Stream blocks until the context is cancelled, as the real client does.
// Cancellation is the only teardown path.
func (f *fakeManagedSSEClient) Stream(ctx context.Context, handler func(StreamEvent)) error {
	f.handler = handler
	close(f.ready)
	<-ctx.Done()
	close(f.stopped)
	return ctx.Err()
}

// emit delivers an event as the read loop would.
func (f *fakeManagedSSEClient) emit(event StreamEvent) { f.handler(event) }

type fakeClientFactory struct {
	mu      sync.Mutex
	calls   map[string]int
	clients map[string][]*fakeManagedSSEClient
}

func newFakeClientFactory() *fakeClientFactory {
	return &fakeClientFactory{
		calls:   make(map[string]int),
		clients: make(map[string][]*fakeManagedSSEClient),
	}
}

func (f *fakeClientFactory) newClient(id string) SSEClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[id]++
	c := newFakeManagedSSEClient()
	f.clients[id] = append(f.clients[id], c)
	return c
}

func (f *fakeClientFactory) callCount(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[id]
}

// nth waits for the n-th (1-indexed) fake client created for id and for it to
// have subscribed. Subscription setup runs in a goroutine, so
// tests must wait rather than assume the client exists immediately after
// AddListener returns.
func (f *fakeClientFactory) nth(t *testing.T, id string, n int) *fakeManagedSSEClient {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		f.mu.Lock()
		clients := f.clients[id]
		if len(clients) >= n {
			c := clients[n-1]
			f.mu.Unlock()
			select {
			case <-c.ready:
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for subscription for id %q (instance %d)", id, n)
			}
			return c
		}
		f.mu.Unlock()
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for client #%d for id %q", n, id)
		case <-time.After(time.Millisecond):
		}
	}
}

// latest waits for the first fake client created for id.
func (f *fakeClientFactory) latest(t *testing.T, id string) *fakeManagedSSEClient {
	return f.nth(t, id, 1)
}

func TestSubscriptionManager_SingleListenerReceivesEvents(t *testing.T) {
	factory := newFakeClientFactory()
	mgr := NewSubscriptionManager[valueEvent](factory.newClient)

	received := make(chan *valueEvent, 1)
	unsub := mgr.AddListener("pool-1", func(v *valueEvent) { received <- v })
	defer unsub()

	client := factory.latest(t, "pool-1")
	client.emit(StreamEvent{Data: `{"value":7}`})

	select {
	case v := <-received:
		if v.Value != 7 {
			t.Errorf("got %d, want 7", v.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for listener to be called")
	}
}

func TestSubscriptionManager_MultipleListenersShareOneConnection(t *testing.T) {
	factory := newFakeClientFactory()
	mgr := NewSubscriptionManager[valueEvent](factory.newClient)

	received1 := make(chan *valueEvent, 1)
	received2 := make(chan *valueEvent, 1)
	unsub1 := mgr.AddListener("pool-1", func(v *valueEvent) { received1 <- v })
	defer unsub1()
	unsub2 := mgr.AddListener("pool-1", func(v *valueEvent) { received2 <- v })
	defer unsub2()

	client := factory.latest(t, "pool-1")

	if got := factory.callCount("pool-1"); got != 1 {
		t.Fatalf("expected exactly 1 SSE connection for 2 listeners on the same id, got %d", got)
	}
	client.emit(StreamEvent{Data: `{"value":9}`})

	for i, ch := range []chan *valueEvent{received1, received2} {
		select {
		case v := <-ch:
			if v.Value != 9 {
				t.Errorf("listener %d got %d, want 9", i+1, v.Value)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for listener %d to be called", i+1)
		}
	}
}

func TestSubscriptionManager_DifferentIdsAreIndependent(t *testing.T) {
	factory := newFakeClientFactory()
	mgr := NewSubscriptionManager[valueEvent](factory.newClient)

	received1 := make(chan *valueEvent, 1)
	received2 := make(chan *valueEvent, 1)
	defer mgr.AddListener("pool-1", func(v *valueEvent) { received1 <- v })()
	defer mgr.AddListener("pool-2", func(v *valueEvent) { received2 <- v })()

	client1 := factory.latest(t, "pool-1")
	client1.emit(StreamEvent{Data: `{"value":1}`})

	select {
	case v := <-received1:
		if v.Value != 1 {
			t.Errorf("got %d, want 1", v.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pool-1 listener")
	}

	select {
	case v := <-received2:
		t.Fatalf("pool-2 listener should not have fired, got %v", v)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSubscriptionManager_RemovingLastListenerTearsDownAndReconnects(t *testing.T) {
	factory := newFakeClientFactory()
	mgr := NewSubscriptionManager[valueEvent](factory.newClient)

	unsub := mgr.AddListener("pool-1", func(v *valueEvent) {})
	client := factory.latest(t, "pool-1")

	unsub()

	select {
	case <-client.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the subscription to be cancelled after removing the last listener")
	}

	// Re-adding a listener for the same id must open a fresh connection.
	mgr.AddListener("pool-1", func(v *valueEvent) {})
	factory.nth(t, "pool-1", 2)
	if got := factory.callCount("pool-1"); got != 2 {
		t.Errorf("expected a second SSE connection to be opened, got %d calls", got)
	}
}

func TestSubscriptionManager_RemovingOneOfSeveralListenersKeepsConnectionOpen(t *testing.T) {
	factory := newFakeClientFactory()
	mgr := NewSubscriptionManager[valueEvent](factory.newClient)

	received1 := make(chan *valueEvent, 1)
	received2 := make(chan *valueEvent, 1)
	unsub1 := mgr.AddListener("pool-1", func(v *valueEvent) { received1 <- v })
	defer mgr.AddListener("pool-1", func(v *valueEvent) { received2 <- v })()

	client := factory.latest(t, "pool-1")
	unsub1()

	client.emit(StreamEvent{Data: `{"value":3}`})

	select {
	case v := <-received2:
		if v.Value != 3 {
			t.Errorf("got %d, want 3", v.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for remaining listener to be called")
	}

	select {
	case v := <-received1:
		t.Fatalf("removed listener should not have fired, got %v", v)
	case <-time.After(50 * time.Millisecond):
	}

	if got := factory.callCount("pool-1"); got != 1 {
		t.Errorf("expected the connection to stay open with a listener still registered, got %d calls", got)
	}
}

// Listeners are a fire-and-forget API with nowhere to report an error, so
// heartbeats and undecodable events are dropped rather than surfaced.
func TestSubscriptionManager_HeartbeatsAndUnparsableEventsAreIgnored(t *testing.T) {
	factory := newFakeClientFactory()
	mgr := NewSubscriptionManager[valueEvent](factory.newClient)

	received := make(chan *valueEvent, 4)
	defer mgr.AddListener("pool-1", func(v *valueEvent) { received <- v })()

	client := factory.latest(t, "pool-1")
	client.emit(StreamEvent{})
	client.emit(StreamEvent{Type: EventEntityStable})
	client.emit(StreamEvent{Data: `not json`})
	client.emit(StreamEvent{Data: `{"value":11}`})

	select {
	case v := <-received:
		if v.Value != 11 {
			t.Errorf("got %d, want 11", v.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the valid event to be delivered")
	}

	select {
	case v := <-received:
		t.Fatalf("expected no further deliveries, got %v", v)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSubscriptionManager_CloseTearsDownAllSubscriptions(t *testing.T) {
	factory := newFakeClientFactory()
	mgr := NewSubscriptionManager[valueEvent](factory.newClient)

	mgr.AddListener("pool-1", func(v *valueEvent) {})
	mgr.AddListener("pool-2", func(v *valueEvent) {})
	client1 := factory.latest(t, "pool-1")
	client2 := factory.latest(t, "pool-2")

	mgr.Close()

	for _, c := range []*fakeManagedSSEClient{client1, client2} {
		select {
		case <-c.stopped:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for the subscription to be cancelled after Close")
		}
	}

	mgr.mu.Lock()
	remaining := len(mgr.subscriptions)
	mgr.mu.Unlock()
	if remaining != 0 {
		t.Errorf("expected no subscriptions to remain after Close, got %d", remaining)
	}
}
