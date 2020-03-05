/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package volumebinding

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller/volume/scheduling"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
)

const (
	// DefaultBindTimeoutSeconds defines the default bind timeout in seconds
	DefaultBindTimeoutSeconds = 600

	allBoundStateKey framework.StateKey = "volumebinding:all-bound"
)

type allBoundStateData bool

func (d allBoundStateData) Clone() framework.StateData {
	return d
}

// VolumeBinding is a plugin that binds pod volumes in scheduling.
type VolumeBinding struct {
	binder scheduling.SchedulerVolumeBinder
}

var _ framework.FilterPlugin = &VolumeBinding{}
var _ framework.ReservePlugin = &VolumeBinding{}
var _ framework.PreBindPlugin = &VolumeBinding{}
var _ framework.UnreservePlugin = &VolumeBinding{}
var _ framework.PostBindPlugin = &VolumeBinding{}

// Name is the name of the plugin used in Registry and configurations.
const Name = "VolumeBinding"

// Name returns name of the plugin. It is used in logs, etc.
func (pl *VolumeBinding) Name() string {
	return Name
}

func podHasPVCs(pod *v1.Pod) bool {
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			return true
		}
	}
	return false
}

// Filter invoked at the filter extension point.
// It evaluates if a pod can fit due to the volumes it requests,
// for both bound and unbound PVCs.
//
// For PVCs that are bound, then it checks that the corresponding PV's node affinity is
// satisfied by the given node.
//
// For PVCs that are unbound, it tries to find available PVs that can satisfy the PVC requirements
// and that the PV node affinity is satisfied by the given node.
//
// The predicate returns true if all bound PVCs have compatible PVs with the node, and if all unbound
// PVCs can be matched with an available and node-compatible PV.
func (pl *VolumeBinding) Filter(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}
	// If pod does not request any PVC, we don't need to do anything.
	if !podHasPVCs(pod) {
		return nil
	}

	reasons, err := pl.binder.FindPodVolumes(pod, node)

	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}

	if len(reasons) > 0 {
		status := framework.NewStatus(framework.UnschedulableAndUnresolvable)
		for _, reason := range reasons {
			status.AppendReason(string(reason))
		}
		return status
	}
	return nil
}

// Reserve is called by the scheduling framework when the scheduler cache is
// updated.
func (pl *VolumeBinding) Reserve(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	allBound, err := pl.binder.AssumePodVolumes(pod, nodeName)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}
	cs.Write(allBoundStateKey, allBoundStateData(allBound))
	return nil
}

// PreBind is called before binding a pod. All prebind plugins must return
// success or the pod will be rejected and won't be sent for binding.
//
// PreBind will make the API update with the assumed bindings and wait until
// the PV controller has completely finished the binding operation.
//
// If binding errors, times out or gets undone, then an error will be returned to
// retry scheduling.
func (pl *VolumeBinding) PreBind(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	state, err := cs.Read(allBoundStateKey)
	if err != nil {
		return framework.NewStatus(framework.Error, err.Error())
	}
	allBound, ok := state.(allBoundStateData)
	if !ok {
		return framework.NewStatus(framework.Error, "unable to convert state into allBoundStateData")
	}
	if allBound {
		// no need to bind volumes
		return nil
	}
	klog.V(5).Infof("Trying to bind volumes for pod \"%v/%v\"", pod.Namespace, pod.Name)
	err = pl.binder.BindPodVolumes(pod)
	if err != nil {
		klog.V(1).Infof("Failed to bind volumes for pod \"%v/%v\": %v", pod.Namespace, pod.Name, err)
		return framework.NewStatus(framework.Error, err.Error())
	}
	klog.V(5).Infof("Success binding volumes for pod \"%v/%v\"", pod.Namespace, pod.Name)
	return nil
}

// Unreserve is called by the scheduling framework when a reserved pod was
// rejected in a later phase.
// TODO(cofyc) Revert assumed PV/PVC cache, see http://issues.k8s.io/82934#issuecomment-538269188
func (pl *VolumeBinding) Unreserve(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) {
	pl.binder.DeletePodBindings(pod)
	return
}

// PostBind is called after a pod is successfully bound.
func (pl *VolumeBinding) PostBind(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) {
	pl.binder.DeletePodBindings(pod)
	return
}

// Args holds the args that are used to configure the plugin.
type Args struct {
	BindTimeoutSeconds *int64 `json:"bindTimeoutSeconds,omitempty"`
}

// New initializes a new plugin and returns it.
func New(plArgs runtime.Object, fh framework.FrameworkHandle) (framework.Plugin, error) {
	args := &Args{}
	if err := framework.DecodeInto(plArgs, args); err != nil {
		return nil, err
	}
	if err := validateArgs(args); err != nil {
		return nil, err
	}
	var bindTimeoutSeconds int64
	if args.BindTimeoutSeconds == nil {
		bindTimeoutSeconds = DefaultBindTimeoutSeconds
	} else {
		bindTimeoutSeconds = *args.BindTimeoutSeconds
	}
	nodeInformer := fh.SharedInformerFactory().Core().V1().Nodes()
	pvcInformer := fh.SharedInformerFactory().Core().V1().PersistentVolumeClaims()
	pvInformer := fh.SharedInformerFactory().Core().V1().PersistentVolumes()
	storageClassInformer := fh.SharedInformerFactory().Storage().V1().StorageClasses()
	csiNodeInformer := fh.SharedInformerFactory().Storage().V1().CSINodes()
	binder := scheduling.NewVolumeBinder(fh.ClientSet(), nodeInformer, csiNodeInformer, pvcInformer, pvInformer, storageClassInformer, time.Duration(bindTimeoutSeconds)*time.Second)
	// In Filter phrase, pod binding cache is created for the pod and used in
	// Reserve and PreBind phrases. Pod binding cache will be cleared at
	// Unreserve and PostBind extension points.  However, if pod fails before
	// Reserve phras and is deleted from the apiserver later, its pod binding
	// cache cannot be cleared at plugin extension points. Here we register an
	// event handler to clear pod binding cache when the pod is deleted to
	// prevent memory leaking.
	// TODO(cofyc) Because pod binding cache is used only in current scheduling
	// cycle, we can share it via framework.CycleState. Then we don't need to
	// clear pod binding cache.
	fh.SharedInformerFactory().Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			var pod *v1.Pod
			switch t := obj.(type) {
			case *v1.Pod:
				pod = obj.(*v1.Pod)
			case cache.DeletedFinalStateUnknown:
				var ok bool
				pod, ok = t.Obj.(*v1.Pod)
				if !ok {
					utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1.Pod", obj))
					return
				}
			default:
				utilruntime.HandleError(fmt.Errorf("unable to handle object %T", obj))
				return
			}
			binder.DeletePodBindings(pod)
		},
	})
	return &VolumeBinding{
		binder: binder,
	}, nil
}

func validateArgs(args *Args) error {
	if args.BindTimeoutSeconds == nil {
		return nil
	}
	bindTimeoutSeconds := *args.BindTimeoutSeconds
	if bindTimeoutSeconds <= 0 {
		return fmt.Errorf("invalid BindTimeoutSeconds: %d, must be positive integer", bindTimeoutSeconds)
	}
	return nil
}

// NewFromVolumeBinder initializes a new plugin from an existing VolumeBinder.
func NewFromVolumeBinder(binder scheduling.SchedulerVolumeBinder) framework.Plugin {
	return &VolumeBinding{
		binder: binder,
	}
}
