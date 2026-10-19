package compute

import (
	"context"
	"sync"

	"github.com/yellowdog/yellowdog-sdk-go/internal"
	"github.com/yellowdog/yellowdog-sdk-go/model"
)

var computeReqSubscriptions sync.Map // *ComputeClient -> *internal.SubscriptionManager[model.ComputeRequirement]

func getComputeReqSubscriptions(c *ComputeClient) *internal.SubscriptionManager[model.ComputeRequirement] {
	if v, ok := computeReqSubscriptions.Load(c); ok {
		return v.(*internal.SubscriptionManager[model.ComputeRequirement])
	}
	mgr := internal.NewSubscriptionManager[model.ComputeRequirement](func(id string) internal.SSEClient {
		return c.proxy.newRequirementUpdatesSSEClient(id)
	})
	v, _ := computeReqSubscriptions.LoadOrStore(c, mgr)
	return v.(*internal.SubscriptionManager[model.ComputeRequirement])
}

func (c *ComputeClient) Helper(req *model.ComputeRequirement) *RequirementHelper {
	return NewRequirementHelper(c, req)
}

func (c *ComputeClient) HelperById(ctx context.Context, id string) (*RequirementHelper, error) {
	req, err := c.GetComputeRequirementById(ctx, id)
	if err != nil {
		return nil, err
	}
	return NewRequirementHelper(c, req), nil
}

func (c *ComputeClient) AddComputeRequirementListener(requirementId string, listener func(*model.ComputeRequirement)) func() {
	return getComputeReqSubscriptions(c).AddListener(requirementId, listener)
}
