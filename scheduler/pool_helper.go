package scheduler

import (
	"context"
	"fmt"
	"sync"

	"github.com/yellowdog/yellowdog-sdk-go/internal"
	"github.com/yellowdog/yellowdog-sdk-go/model"
	"github.com/yellowdog/yellowdog-sdk-go/optional"
)

// workerPoolConstraint is satisfied by *ConfiguredWorkerPool and *ProvisionedWorkerPool.
// The *T constraint aligns with SSEWaiter[T], which operates on T and returns *T.
type workerPoolConstraint[T any] interface {
	*T
	model.WorkerPoolWithState
}

var configuredWorkerPoolSubscriptions sync.Map

func getConfiguredWorkerPoolSubscriptions(c *WorkerPoolClient) *internal.SubscriptionManager[model.ConfiguredWorkerPool] {
	if v, ok := configuredWorkerPoolSubscriptions.Load(c); ok {
		return v.(*internal.SubscriptionManager[model.ConfiguredWorkerPool])
	}
	mgr := internal.NewSubscriptionManager[model.ConfiguredWorkerPool](func(id string) internal.SSEClient {
		return c.proxy.newWorkerPoolUpdatesSSEClient(id)
	})
	v, _ := configuredWorkerPoolSubscriptions.LoadOrStore(c, mgr)
	return v.(*internal.SubscriptionManager[model.ConfiguredWorkerPool])
}

type WorkerPoolHelper[T any, PT workerPoolConstraint[T]] struct {
	client *WorkerPoolClient
	pool   PT
}

// HelperById fetches the worker pool with the given ID and returns a helper for it. It returns an
// error if the pool is not a ConfiguredWorkerPool.
func (c *WorkerPoolClient) HelperById(ctx context.Context, id string) (*WorkerPoolHelper[model.ConfiguredWorkerPool, *model.ConfiguredWorkerPool], error) {
	wp, err := c.GetWorkerPool(ctx, id)
	if err != nil {
		return nil, err
	}
	pool, ok := wp.(*model.ConfiguredWorkerPool)
	if !ok {
		return nil, fmt.Errorf("worker pool %s is not a ConfiguredWorkerPool", id)
	}
	return NewWorkerPoolHelper[model.ConfiguredWorkerPool, *model.ConfiguredWorkerPool](c, pool), nil
}

func NewWorkerPoolHelper[T any, PT workerPoolConstraint[T]](client *WorkerPoolClient, pool PT) *WorkerPoolHelper[T, PT] {
	return &WorkerPoolHelper[T, PT]{
		client: client,
		pool:   pool,
	}
}

func (h *WorkerPoolHelper[T, PT]) WaitForStatus(ctx context.Context, status model.WorkerPoolStatus) (PT, error) {
	return h.WaitUntil(ctx, func(pool PT) bool {
		s := pool.GetStatus()
		return s == optional.Of(status)
	})
}

// WaitUntil blocks until the predicate returns true or the context is cancelled.
// Updates are streamed from /workerPools/{id}/updates. A streaming failure is
// returned rather than retried by polling; a caller who wants to poll owns that
// loop.
func (h *WorkerPoolHelper[T, PT]) WaitUntil(ctx context.Context, predicate func(PT) bool) (PT, error) {
	sseClient := h.client.proxy.newWorkerPoolUpdatesSSEClient(h.pool.GetId())
	waiter := internal.NewSSEWaiter[T](sseClient).
		WithPrimer(func(ctx context.Context) (*T, error) {
			wp, err := h.client.GetWorkerPool(ctx, h.pool.GetId())
			if err != nil {
				return nil, err
			}
			pool, ok := wp.(PT)
			if !ok {
				return nil, fmt.Errorf("worker pool %s has unexpected type", h.pool.GetId())
			}
			return (*T)(pool), nil
		}).
		WithIdleTimeout(streamIdleTimeout)
	return waiter.WaitUntil(ctx, func(p *T) bool {
		return predicate(PT(p))
	})
}

// AddWorkerPoolListener subscribes to live SSE updates for the given configured worker pool ID.
// The listener is called on each incoming update event.
// Returns an unsubscribe function; call it to remove the listener. The SSE connection is closed
// automatically when the last listener for a given ID unsubscribes.
func (c *WorkerPoolClient) AddWorkerPoolListener(workerPoolId string, listener func(*model.ConfiguredWorkerPool)) func() {
	return getConfiguredWorkerPoolSubscriptions(c).AddListener(workerPoolId, listener)
}

// AddNodeActions adds the specified node actions to be performed for the specified worker pool.
func (c *WorkerPoolClient) AddNodeActions(ctx context.Context, workerPool model.WorkerPool, request *AddNodeActionsRequest) error {
	return c.proxy.addNodeActionsById(ctx, workerPool.GetId(), request)
}
