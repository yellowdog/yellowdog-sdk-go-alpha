package scheduler

import (
	"context"
	"sync"

	"github.com/yellowdog/yellowdog-sdk-go/internal"
	"github.com/yellowdog/yellowdog-sdk-go/model"
)

var workReqSubscriptions sync.Map // *WorkClient -> *internal.SubscriptionManager[model.WorkRequirement]

func getWorkReqSubscriptions(c *WorkClient) *internal.SubscriptionManager[model.WorkRequirement] {
	if v, ok := workReqSubscriptions.Load(c); ok {
		return v.(*internal.SubscriptionManager[model.WorkRequirement])
	}
	mgr := internal.NewSubscriptionManager[model.WorkRequirement](func(id string) internal.SSEClient {
		return c.proxy.newWorkRequirementUpdatesSSEClient(id)
	})
	v, _ := workReqSubscriptions.LoadOrStore(c, mgr)
	return v.(*internal.SubscriptionManager[model.WorkRequirement])
}

func (c *WorkClient) Helper(req *model.WorkRequirement) *WorkRequirementHelper {
	return NewWorkRequirementHelper(c, req)
}

func (c *WorkClient) HelperById(ctx context.Context, id string) (*WorkRequirementHelper, error) {
	req, err := c.GetWorkRequirementById(ctx, id)
	if err != nil {
		return nil, err
	}
	return NewWorkRequirementHelper(c, req), nil
}

func (c *WorkClient) AddWorkRequirementListener(requirementId string, listener func(*model.WorkRequirement)) func() {
	return getWorkReqSubscriptions(c).AddListener(requirementId, listener)
}
