package internal

import (
	"context"
	"encoding/json"
	"sync"
)

// SubscriptionManager manages persistent SSE subscriptions with fan-out.
// Multiple listeners can register for the same resource ID; a single SSE connection
// is kept open per ID and torn down automatically when the last listener unsubscribes.
type SubscriptionManager[T any] struct {
	mu            sync.Mutex
	subscriptions map[string]*activeSubscription[T]
	newClient     func(id string) SSEClient
}

type activeSubscription[T any] struct {
	listeners []*listenerEntry[T]
	cancel    context.CancelFunc
}

type listenerEntry[T any] struct {
	fn func(*T)
}

// NewSubscriptionManager creates a new manager. newClient is called to open the SSE stream
// for a given resource ID and must return a client pointed at the resource's update endpoint.
func NewSubscriptionManager[T any](newClient func(id string) SSEClient) *SubscriptionManager[T] {
	return &SubscriptionManager[T]{
		subscriptions: make(map[string]*activeSubscription[T]),
		newClient:     newClient,
	}
}

// AddListener subscribes fn to live updates for the resource with the given ID.
// Returns an unsubscribe function; call it to remove the listener.
// If this is the first listener for the ID a new SSE connection is opened.
func (m *SubscriptionManager[T]) AddListener(id string, fn func(*T)) func() {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := &listenerEntry[T]{fn: fn}

	sub, ok := m.subscriptions[id]
	if !ok {
		ctx, cancel := context.WithCancel(context.Background())
		sub = &activeSubscription[T]{cancel: cancel}
		m.subscriptions[id] = sub
		go m.runSubscription(ctx, id, sub)
	}
	sub.listeners = append(sub.listeners, entry)

	return func() { m.removeListener(id, entry) }
}

// Close tears down all active SSE connections and removes all listeners.
func (m *SubscriptionManager[T]) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, sub := range m.subscriptions {
		sub.cancel()
		delete(m.subscriptions, id)
	}
}

func (m *SubscriptionManager[T]) removeListener(id string, entry *listenerEntry[T]) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sub, ok := m.subscriptions[id]
	if !ok {
		return
	}
	for i, e := range sub.listeners {
		if e == entry {
			sub.listeners = append(sub.listeners[:i], sub.listeners[i+1:]...)
			break
		}
	}
	if len(sub.listeners) == 0 {
		sub.cancel()
		delete(m.subscriptions, id)
	}
}

// runSubscription streams updates for id until ctx is cancelled. Cancelling the
// context is the only teardown path.
func (m *SubscriptionManager[T]) runSubscription(ctx context.Context, id string, sub *activeSubscription[T]) {
	client := m.newClient(id)

	// The return value reports why the stream ended. Listeners are a
	// fire-and-forget API with nowhere to deliver an error, so an ended stream
	// simply stops delivering; a caller needing the reason should use a waiter.
	_ = client.Stream(ctx, func(event StreamEvent) {
		if event.Type == EventEntityStable || event.Data == "" {
			return // heartbeat, no entity to deliver
		}
		var value T
		if err := json.Unmarshal([]byte(event.Data), &value); err != nil {
			return
		}
		// Snapshot listeners under lock, then call them without the lock
		// to avoid deadlocks if a listener unsubscribes.
		m.mu.Lock()
		listeners := make([]*listenerEntry[T], len(sub.listeners))
		copy(listeners, sub.listeners)
		m.mu.Unlock()
		for _, l := range listeners {
			l.fn(&value)
		}
	})
}
