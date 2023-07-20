/*
Copyright 2021.

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

package controllers

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	dpv1 "github.com/NVIDIA/k8s-device-plugin/api/config/v1"
	migv1 "github.com/NVIDIA/mig-parted/api/spec/v1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	esv1 "github.com/NVIDIA/gpu-operator/apis/ecns.easystack.com/v1"
)

const (
	DevicePluginDefaultConfigLabel = "nvidia.com/device-plugin.config"
	MigConfigLabel                 = "nvidia.com/mig.config"
	ConfigSkipLabelValue           = "easystack-skip-me"
	ModeDriverVersionLabel         = "nvidia.com/gpu.deploy.driver.version"
	ModeManagedByLabel             = "easystack.com/gpu.deploy.managed-by"

	RELEASEFINALIZERNAME = "release.finalizers.easystack.com"
)

var gpuModeLabels = map[string]map[string]string{
	gpuWorkloadConfigNone: {
		commonOperandsLabelKey: "false",
	},
	gpuWorkloadConfigContainer: {
		commonOperandsLabelKey:         commonOperandsLabelValue,
		gpuWorkloadConfigLabelKey:      gpuWorkloadConfigContainer,
		DevicePluginDefaultConfigLabel: DevicePluginDefaultConfig,
		ModeDriverVersionLabel:         "R535",
	},
	gpuWorkloadConfigVcuda: {
		commonOperandsLabelKey:    commonOperandsLabelValue,
		gpuWorkloadConfigLabelKey: gpuWorkloadConfigVcuda,
		ModeDriverVersionLabel:    "R470",
	},
}

// GpuNodePolicyReconciler reconciles a GpuNodePolicy object
type GpuNodePolicyReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=ecns.easystack.com,resources=gpunodepolicies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ecns.easystack.com,resources=gpunodepolicies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ecns.easystack.com,resources=gpunodepolicies/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the GpuNodePolicy object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.1/pkg/reconcile
func (r *GpuNodePolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var requeue bool
	_ = r.Log.WithValues("Reconciling GpuNodePolicy", req.NamespacedName)

	// Fetch the ClusterPolicy instance
	instance := &esv1.GpuNodePolicy{}
	err := r.Client.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		r.Log.Error(err, "Failed to fetch GpuNodePolicy instance")
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	managedBy := fmt.Sprintf("%s.%s", instance.GetNamespace(), instance.GetName())

	// examine DeletionTimestamp to determine if object is under deletion
	if instance.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted, so if it does not have our finalizer,
		// then lets add the finalizer and update the object. This is equivalent
		// registering our finalizer.
		if !StringInArray(RELEASEFINALIZERNAME, instance.ObjectMeta.Finalizers) {
			instance.ObjectMeta.Finalizers = append(instance.ObjectMeta.Finalizers, RELEASEFINALIZERNAME)
			if err := r.Update(ctx, instance); err != nil {
				return reconcile.Result{}, err
			}
		}
	} else {
		// The object is being deleted
		if StringInArray(RELEASEFINALIZERNAME, instance.ObjectMeta.Finalizers) {
			// our finalizer is present, so lets handle any external dependency
			r.Log.Info("Release nodes managed by GpuNodePolicy", "Namespace", instance.GetNamespace(), "Name", instance.GetName())
			opts := []client.ListOption{
				client.MatchingLabels{ModeManagedByLabel: managedBy},
			}
			managedNodeList := &corev1.NodeList{}
			err = r.Client.List(ctx, managedNodeList, opts...)
			if err != nil {
				return reconcile.Result{}, fmt.Errorf("unable to list managed nodes, err %s", err.Error())
			}

			if err := r.ReleaseNodes(ctx, managedNodeList.Items); err != nil {
				// if fail to delete the external dependency here, return with error
				// so that it can be retried
				return reconcile.Result{}, err
			}
			// remove our finalizer from the list and update it.
			var found bool
			instance.ObjectMeta.Finalizers, found = RemoveString(RELEASEFINALIZERNAME, instance.ObjectMeta.Finalizers)
			if found {
				if err := r.Update(ctx, instance); err != nil {
					return reconcile.Result{}, err
				}
			}
		}
		return reconcile.Result{}, err
	}

	// 1. fetch all nodes
	opts := []client.ListOption{
		client.MatchingLabels(instance.Spec.NodeSelector),
	}
	claimNodeList := &corev1.NodeList{}
	err = r.Client.List(ctx, claimNodeList, opts...)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("unable to list claimed nodes, err %s", err.Error())
	}

	opts = []client.ListOption{
		client.MatchingLabels{ModeManagedByLabel: managedBy},
	}
	matchNodeList := &corev1.NodeList{}
	err = r.Client.List(ctx, matchNodeList, opts...)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("unable to list matched nodes, err %s", err.Error())
	}

	// 2. filter nodes
	adopt, match, release := r.FilterNodes(claimNodeList.Items, matchNodeList.Items)

	// 3. set config
	dpModified, migModified := false, false
	switch instance.Spec.Mode {
	case esv1.VCUDA:
		// If gpu manager has config
	case esv1.TimeSlicing:
		dpModified, err = r.setupTimeSlicingMode(ctx, instance)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("unable to setup to time-slicing mode, err %s", err.Error())
		}
	case esv1.MIG:
		dpModified, err = r.setupTimeSlicingMode(ctx, instance)
		if err != nil {
			r.Log.Error(err, "Unable to setup to time-slicing mode")
			requeue = true
		}
		migModified, err = r.setupMigMode(ctx, instance)
		if err != nil {
			r.Log.Error(err, "Unable to setup to mig mode")
			requeue = true
		}
	}

	// 4. setup nodes
	modeCfg := &gpuModeConfiguration{config: setWorkloadConfig(instance.Spec.Mode), managedBy: managedBy, log: r.Log}

	err = r.AdoptNodes(ctx, adopt, modeCfg)
	if err != nil {
		r.Log.Error(err, "Failed to adopt nodes")
		requeue = true
	}

	err = r.UpdateNodes(ctx, match, modeCfg, dpModified, migModified)
	if err != nil {
		r.Log.Error(err, "Failed to update nodes")
		requeue = true
	}

	err = r.ReleaseNodes(ctx, release)
	if err != nil {
		r.Log.Error(err, "Failed to release nodes")
		requeue = true
	}

	if requeue {
		return ctrl.Result{Requeue: requeue}, fmt.Errorf("failed to setup nodes by %s", managedBy)
	}

	return ctrl.Result{}, nil
}

func (r *GpuNodePolicyReconciler) setupTimeSlicingMode(ctx context.Context, instance *esv1.GpuNodePolicy) (bool, error) {
	modified := false
	origin := &corev1.ConfigMap{}
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: instance.GetNamespace(), Name: DevicePluginDefaultConfigMapName}, origin)
	if err != nil {
		return false, fmt.Errorf("unable to get device plugin ConfigMap, err %s", err.Error())
	}
	patch := client.MergeFrom(origin)
	dpConfigMap := origin.DeepCopy()

	oriRaw, ok := origin.Data[instance.GetName()]
	if !ok && instance.Spec.TimeSlicingConfig == nil && isMigNoneStrategy(instance.Spec.MigConfig) {
		r.Log.Info("No time slicing config found, skip")
		return false, nil
	}
	oriConfig := &dpv1.Config{}
	_ = yaml.Unmarshal([]byte(oriRaw), oriConfig)

	dpConfig := &dpv1.Config{}
	convertSpecToDevicePluginConfig(instance.Spec.TimeSlicingConfig, dpConfig)

	if instance.Spec.Mode == esv1.MIG {
		strategy := setMigStrategy(instance.Spec.MigConfig)
		dpConfig.Flags.MigStrategy = &strategy
	}

	if reflect.DeepEqual(oriConfig, dpConfig) {
		r.Log.Info("No time slicing config need to be updated", "name", instance.GetName())
		return false, nil
	}
	modified = true

	raw, err := yaml.Marshal(dpConfig)
	if err != nil {
		return false, fmt.Errorf("marshal error: %v", err)
	}
	dpConfigMap.Data[instance.GetName()] = string(raw)

	err = r.Client.Patch(ctx, dpConfigMap, patch)
	if err != nil {
		return false, fmt.Errorf("fail to update device plugin ConfigMap, err %s", err.Error())
	}

	return modified, nil
}

func (r *GpuNodePolicyReconciler) setupMigMode(ctx context.Context, instance *esv1.GpuNodePolicy) (bool, error) {
	modified := false
	origin := &corev1.ConfigMap{}
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: instance.GetNamespace(), Name: MigPartedDefaultConfigMapName}, origin)
	if err != nil {
		return false, fmt.Errorf("unable to get mig manager ConfigMap, err %s", err.Error())
	}
	patch := client.MergeFrom(origin)
	migConfigMap := origin.DeepCopy()

	oriConfig := &migv1.Spec{}
	_ = yaml.Unmarshal([]byte(origin.Data["config.yaml"]), oriConfig)

	migConfig := &migv1.Spec{}
	err = yaml.Unmarshal([]byte(migConfigMap.Data["config.yaml"]), migConfig)
	if err != nil {
		return false, fmt.Errorf("unmarshal error: %v", err)
	}

	if _, ok := migConfig.MigConfigs[instance.GetName()]; !ok && isMigNoneStrategy(instance.Spec.MigConfig) {
		r.Log.Info("No mig config found, skip")
		return false, nil
	}

	migSpecSlice := migv1.MigConfigSpecSlice{}
	for _, config := range instance.Spec.MigConfig.ConfigSlice {
		spec := migv1.MigConfigSpec{}
		convertSpecToMigConfigSpec(&config, &spec)
		migSpecSlice = append(migSpecSlice, spec)
	}
	migConfig.MigConfigs[instance.GetName()] = migSpecSlice

	if reflect.DeepEqual(oriConfig, migConfig) {
		r.Log.Info("No mig config need to be updated", "name", instance.GetName())
		return false, nil
	}
	modified = true

	config, err := yaml.Marshal(migConfig)
	if err != nil {
		return false, fmt.Errorf("marshal error: %v", err)
	}
	migConfigMap.Data["config.yaml"] = string(config)

	err = r.Client.Patch(ctx, migConfigMap, patch)
	if err != nil {
		return false, fmt.Errorf("fail to update mig manager ConfigMap, err %s", err.Error())
	}

	return modified, nil
}

func convertSpecToDevicePluginConfig(in *esv1.TimeSlicingConfigSpec, out *dpv1.Config) {
	if in == nil {
		return
	}
	out.Version = dpv1.Version
	out.Sharing.TimeSlicing.RenameByDefault = in.RenameByDefault
	out.Sharing.TimeSlicing.FailRequestsGreaterThanOne = in.FailRequestsGreaterThanOne

	resources := make([]dpv1.ReplicatedResource, len(in.Resources))
	for i, r := range in.Resources {
		resources[i].Name = dpv1.ResourceName(r.Name)
		resources[i].Rename = dpv1.ResourceName(r.Rename)
		resources[i].Replicas = r.Replicas
	}
	out.Sharing.TimeSlicing.Resources = resources
}

func convertSpecToMigConfigSpec(in *esv1.MigConfigSpecSlice, out *migv1.MigConfigSpec) {
	if in == nil {
		return
	}
	out.Devices = in.Devices
	out.DeviceFilter = in.DeviceFilter
	out.MigEnabled = true
	out.MigDevices = in.MigDevices
}

func setMigStrategy(config *esv1.MigConfigSpec) string {
	if isMigNoneStrategy(config) {
		return dpv1.MigStrategyNone
	}
	switch config.Strategy {
	case esv1.MigSingle:
		return dpv1.MigStrategySingle
	case esv1.MigTimeSlicing:
		return dpv1.MigStrategyMixed
	case "":
		return dpv1.MigStrategySingle
	}
	return dpv1.MigStrategyNone
}

func isMigNoneStrategy(config *esv1.MigConfigSpec) bool {
	return config == nil || len(config.ConfigSlice) == 0
}

func (r *GpuNodePolicyReconciler) FilterNodes(claimNodes, matchNodes []corev1.Node) (adopt, match, release []corev1.Node) {
	claimNodeMap := make(map[string]corev1.Node, len(claimNodes))
	for _, claimNode := range claimNodes {
		claimNodeMap[claimNode.GetName()] = claimNode
	}
	for _, matchNode := range matchNodes {
		if _, ok := claimNodeMap[matchNode.GetName()]; !ok {
			release = append(release, matchNode)
			continue
		}
		r.Log.Info("Node is matched", "name", matchNode.GetName())
		match = append(match, matchNode)
		delete(claimNodeMap, matchNode.GetName())
	}
	for _, node := range claimNodeMap {
		adopt = append(adopt, node)
	}
	return
}

func (r *GpuNodePolicyReconciler) AdoptNodes(ctx context.Context, nodes []corev1.Node, mc *gpuModeConfiguration) error {
	var errList []string

	for _, node := range nodes {
		labels := node.GetLabels()
		if v, ok := labels[ModeManagedByLabel]; ok && v != mc.managedBy {
			r.Log.Info(fmt.Sprintf("WARNING: unable to adopt node %s by %s, node has been adopted by %s", node.GetName(), mc.managedBy, v))
			continue
		}
		modified := mc.updateGPUStateLabels(node.GetName(), labels)
		if !modified {
			r.Log.Info("Node had been adopted", "name", node.GetName())
			continue
		}
		// update node labels
		node.SetLabels(labels)
		err := r.Client.Update(ctx, &node)
		if err != nil {
			errList = append(errList, fmt.Sprintf("unable to add the GPU mode labels for node %s, err %s",
				node.GetName(), err.Error()))
		}
		r.Log.Info("Node is adopted", "name", node.GetName())
	}
	if len(errList) > 0 {
		return fmt.Errorf(strings.Join(errList, " "))
	}

	return nil
}

func (r *GpuNodePolicyReconciler) UpdateNodes(ctx context.Context, nodes []corev1.Node, mc *gpuModeConfiguration, dpModified, migModified bool) error {
	var errList []string
	var modified bool

	for _, node := range nodes {
		labels := node.GetLabels()
		if v, ok := labels[gpuWorkloadConfigLabelKey]; ok && v == mc.config {
			patch := client.MergeFrom(&node)
			modiNode := node.DeepCopy()
			if dpModified {
				modiNode.Labels[DevicePluginDefaultConfigLabel] = ConfigSkipLabelValue
				modified = dpModified
			}
			if migModified {
				modiNode.Labels[MigConfigLabel] = ConfigSkipLabelValue
				modified = migModified
			}
			// TODO: If patch failed, node will not be updated
			err := r.Client.Patch(ctx, modiNode, patch)
			if err != nil {
				errList = append(errList, fmt.Sprintf("failed to patch node %s, err %s",
					node.GetName(), err.Error()))
				continue
			}
		}
		modified = mc.updateGPUStateLabels(node.GetName(), labels)
		if !modified {
			r.Log.Info("Node does not need to be updated", "name", node.GetName())
			continue
		}
		// update node labels
		node.SetLabels(labels)
		err := r.Client.Update(ctx, &node)
		if err != nil {
			errList = append(errList, fmt.Sprintf("unable to update the GPU mode labels for node %s, err %s",
				node.GetName(), err.Error()))
		}
	}
	if len(errList) > 0 {
		return fmt.Errorf(strings.Join(errList, " "))
	}

	return nil
}

func (r *GpuNodePolicyReconciler) ReleaseNodes(ctx context.Context, nodes []corev1.Node) error {
	var errList []string

	for _, node := range nodes {
		labels := node.GetLabels()
		modified := removeAllGPUModeLabels(labels)
		if !modified {
			r.Log.Info("Node had been released", "name", node.GetName())
			continue
		}
		// update node labels
		node.SetLabels(labels)
		err := r.Client.Update(ctx, &node)
		if err != nil {
			errList = append(errList, fmt.Sprintf("unable to reset the GPU mode labels for node %s, err %s",
				node.GetName(), err.Error()))
		}
		r.Log.Info("Node is released", "name", node.GetName())
	}
	if len(errList) > 0 {
		return fmt.Errorf(strings.Join(errList, " "))
	}

	return nil
}

type gpuModeConfiguration struct {
	config    string
	managedBy string
	log       logr.Logger
}

func (m *gpuModeConfiguration) updateGPUStateLabels(node string, labels map[string]string) bool {
	removed := m.removeGPUModeLabels(node, labels)
	added := m.addGPUModeLabels(node, labels)
	return removed || added
}

func (m *gpuModeConfiguration) addGPUModeLabels(node string, labels map[string]string) bool {
	modified := false
	for key, value := range gpuModeLabels[m.config] {
		if _, ok := labels[key]; !ok {
			m.log.Info("Setting node label", "NodeName", node, "Label", key, "Value", value)
			labels[key] = value
			modified = true
		}
	}
	if v, ok := labels[ModeManagedByLabel]; !ok || v != m.managedBy {
		m.log.Info("Setting node label", "NodeName", node, "Label", ModeManagedByLabel, "Value", m.managedBy)
		labels[ModeManagedByLabel] = m.managedBy
		modified = true
	}
	return modified
}

func (m *gpuModeConfiguration) removeGPUModeLabels(node string, labels map[string]string) bool {
	modified := false
	for workloadConfig, labelsMap := range gpuModeLabels {
		if workloadConfig == m.config {
			continue
		}
		for key, value := range labelsMap {
			if v, ok := gpuModeLabels[m.config][key]; ok && value == v {
				// skip label if it is in the set of states for modeConfig
				continue
			}
			if _, ok := labels[key]; ok {
				m.log.Info("Deleting node label", "NodeName", node, "Label", key)
				delete(labels, key)
				modified = true
			}
		}
	}
	return modified
}

func removeAllGPUModeLabels(labels map[string]string) bool {
	modified := false
	for _, labelsMap := range gpuStateLabels {
		for key := range labelsMap {
			if v, ok := labels[key]; ok && key != commonOperandsLabelKey && v != "false" {
				delete(labels, key)
				modified = true
			}
		}
	}
	if _, ok := labels[ModeManagedByLabel]; ok {
		delete(labels, ModeManagedByLabel)
		modified = true
	}
	if _, ok := labels[ModeDriverVersionLabel]; ok {
		delete(labels, ModeDriverVersionLabel)
		modified = true
	}
	if _, ok := labels[commonOperandsLabelKey]; ok {
		labels[commonOperandsLabelKey] = "false"
		modified = true
	}
	return modified
}

func setWorkloadConfig(mode esv1.DeviceMode) string {
	switch mode {
	case esv1.VCUDA:
		return gpuWorkloadConfigVcuda
	case esv1.TimeSlicing, esv1.MIG:
		return gpuWorkloadConfigContainer
	case "":
		return gpuWorkloadConfigNone
	}
	return gpuWorkloadConfigNone
}

func StringInArray(val string, array []string) bool {
	for i := range array {
		if array[i] == val {
			return true
		}
	}
	return false
}

func RemoveString(s string, slice []string) (result []string, found bool) {
	if len(slice) != 0 {
		for _, item := range slice {
			if item == s {
				found = true
				continue
			}
			result = append(result, item)
		}
	}
	return
}

// SetupWithManager sets up the controller with the Manager.
func (r *GpuNodePolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&esv1.GpuNodePolicy{}).
		Complete(r)
}
