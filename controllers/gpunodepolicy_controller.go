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
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	dpv1 "github.com/NVIDIA/gpu-operator/apis/config/v1"
	esv1 "github.com/NVIDIA/gpu-operator/apis/ecns.easystack.com/v1"
)

const (
	DevicePluginDefaultConfigLabel      = "nvidia.com/device-plugin.config"
	DevicePluginConfigLabelValueDefault = "default"
	MigConfigLabel                      = "nvidia.com/mig.config"
	MigConfigLabelValueAllDisabled      = "all-disabled"
	ConfigSkipLabelValue                = "easystack-skip-me"
	ModeDriverVersionLabel              = "nvidia.com/gpu.deploy.driver.version"
	ModeManagedByLabel                  = "ecns.easystack.com/gpu.deploy.managed-by"

	DevicePluginConfigAnnotation = "ecns.easystack.com/device-plugin.updated"
	MigConfigUpdateAnnotation    = "ecns.easystack.com/mig.updated"

	RELEASEFINALIZERNAME = "release.finalizers.ecns.easystack.com"
)

var gpuModeLabels = map[string]map[string]string{
	gpuWorkloadConfigNone: {
		commonOperandsLabelKey: "false",
	},
	gpuWorkloadConfigContainer: {
		gpuWorkloadConfigLabelKey: gpuWorkloadConfigContainer,
		ModeDriverVersionLabel:    "R535",
	},
	gpuWorkloadConfigVcuda: {
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
	origin := &esv1.GpuNodePolicy{}
	err := r.Client.Get(ctx, req.NamespacedName, origin)
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

	// examine DeletionTimestamp to determine if object is under deletion
	if origin.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted, so if it does not have our finalizer,
		// then lets add the finalizer and update the object. This is equivalent
		// registering our finalizer.
		if !StringInArray(RELEASEFINALIZERNAME, origin.ObjectMeta.Finalizers) {
			origin.ObjectMeta.Finalizers = append(origin.ObjectMeta.Finalizers, RELEASEFINALIZERNAME)
			if err := r.Update(ctx, origin); err != nil {
				return reconcile.Result{}, err
			}
		}
	} else {
		// The object is being deleted
		if StringInArray(RELEASEFINALIZERNAME, origin.ObjectMeta.Finalizers) {
			// our finalizer is present, so lets handle any external dependency
			r.Log.Info("Revert configmaps managed by GpuNodePolicy", "Namespace", origin.GetNamespace(), "Name", origin.GetName())
			err = r.removeDevicePluginConfigMapData(ctx, origin)
			if err != nil {
				return reconcile.Result{}, fmt.Errorf("unable to revert device plugin configmap, err %s", err.Error())
			}
			err = r.removeMigConfigMapData(ctx, origin)
			if err != nil {
				return reconcile.Result{}, fmt.Errorf("unable to revert mig manager configmap, err %s", err.Error())
			}

			r.Log.Info("Release nodes managed by GpuNodePolicy", "Namespace", origin.GetNamespace(), "Name", origin.GetName())
			opts := []client.ListOption{
				client.MatchingLabels{ModeManagedByLabel: origin.GetFullName()},
			}
			managedNodeList := &corev1.NodeList{}
			err = r.Client.List(ctx, managedNodeList, opts...)
			if err != nil {
				return reconcile.Result{}, fmt.Errorf("unable to list managed nodes, err %s", err.Error())
			}

			if origin.Spec.Mode == esv1.MIG {
				if err := r.RevertMigNodes(ctx, managedNodeList.Items); err != nil {
					return reconcile.Result{}, err
				}
				r.Log.Info("Waiting for mig disabled on nodes", "Namespace", origin.GetNamespace(), "Name", origin.GetName())
				// TODO: waiting time by watching node label `nvidia.com/mig.config.state`
				time.Sleep(60 * time.Second)
			}

			if err := r.ReleaseNodes(ctx, managedNodeList.Items); err != nil {
				// if fail to delete the external dependency here, return with error
				// so that it can be retried
				return reconcile.Result{}, err
			}
			// remove our finalizer from the list and update it.
			var found bool
			origin.ObjectMeta.Finalizers, found = RemoveString(RELEASEFINALIZERNAME, origin.ObjectMeta.Finalizers)
			if found {
				if err := r.Update(ctx, origin); err != nil {
					return reconcile.Result{}, err
				}
			}
		}
		return reconcile.Result{}, err
	}

	instance := origin.DeepCopy()
	if instance.Annotations == nil {
		instance.Annotations = make(map[string]string)
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
		client.MatchingLabels{ModeManagedByLabel: instance.GetFullName()},
	}
	matchNodeList := &corev1.NodeList{}
	err = r.Client.List(ctx, matchNodeList, opts...)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("unable to list matched nodes, err %s", err.Error())
	}

	// 2. filter nodes
	adopt, match, release := r.FilterNodes(claimNodeList.Items, matchNodeList.Items)

	// 3. set configmap
	switch instance.Spec.Mode {
	case esv1.Default:
		// If default need config
	case esv1.VCUDA:
		// If gpu manager has config
	case esv1.TimeSlicing:
		err = r.setupTimeSlicingMode(ctx, instance)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("unable to setup to time-slicing mode, err %s", err.Error())
		}
	case esv1.MIG:
		err = r.setupMigMode(ctx, instance)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("unable to setup to mig mode, err %s", err.Error())
		}
	}

	InitGpuNodePolicyStatus(instance, claimNodeList.Items)

	// 4. setup nodes
	modeCfg := newGpuModeConfiguration(instance, r.Log)

	err = r.AdoptNodes(ctx, adopt, modeCfg)
	if err != nil {
		r.Log.Error(err, "Failed to adopt nodes")
		requeue = true
	}

	err = r.UpdateNodes(ctx, match, modeCfg)
	if err != nil {
		r.Log.Error(err, "Failed to update nodes")
		requeue = true
	}

	err = r.ReleaseNodes(ctx, release)
	if err != nil {
		r.Log.Error(err, "Failed to release nodes")
		requeue = true
	}

	if !reflect.DeepEqual(origin.Status, instance.Status) {
		err = r.Status().Update(ctx, instance)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update status, %s", err.Error())
		}
	}

	if requeue {
		return ctrl.Result{Requeue: requeue}, fmt.Errorf("failed to setup nodes by %s", req.NamespacedName.String())
	}
	r.Log.Info("Sync GpuNodePolicy instance successfully")

	return ctrl.Result{}, nil
}

func (r *GpuNodePolicyReconciler) setupTimeSlicingMode(ctx context.Context, instance *esv1.GpuNodePolicy) error {
	modified, err := r.addDevicePluginConfigMapData(ctx, instance)
	if modified {
		instance.Annotations[DevicePluginConfigAnnotation] = "true"
	}
	return err
}

func (r *GpuNodePolicyReconciler) setupMigMode(ctx context.Context, instance *esv1.GpuNodePolicy) error {
	modified, err := r.addDevicePluginConfigMapData(ctx, instance)
	if err != nil {
		r.Log.Error(err, "Fail to setup to time-slicing config")
		return err
	}
	if modified {
		instance.Annotations[DevicePluginConfigAnnotation] = "true"
	}
	modified, err = r.addMigConfigMapData(ctx, instance)
	if err != nil {
		r.Log.Error(err, "Fail to setup to mig config")
	}
	if modified {
		instance.Annotations[MigConfigUpdateAnnotation] = "true"
	}
	return nil
}

func (r *GpuNodePolicyReconciler) addDevicePluginConfigMapData(ctx context.Context, instance *esv1.GpuNodePolicy) (bool, error) {
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
		if strategy == dpv1.MigStrategySingle {
			dpConfig.Sharing = nil
		}
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

func (r *GpuNodePolicyReconciler) removeDevicePluginConfigMapData(ctx context.Context, instance *esv1.GpuNodePolicy) error {
	dpConfigMap := &corev1.ConfigMap{}
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: instance.GetNamespace(), Name: DevicePluginDefaultConfigMapName}, dpConfigMap)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Error(err, "DevicePlugin ConfigMap is not Found")
			return nil
		}
		return fmt.Errorf("unable to get device plugin ConfigMap, err %s", err.Error())
	}

	if _, ok := dpConfigMap.Data[instance.GetName()]; !ok {
		r.Log.Info("No time slicing config need to be reverted", "name", instance.GetName())
		return nil
	}

	patch := client.MergeFrom(dpConfigMap.DeepCopy())
	delete(dpConfigMap.Data, instance.GetName())
	err = r.Client.Patch(ctx, dpConfigMap, patch)
	if err != nil {
		return fmt.Errorf("fail to revert device plugin ConfigMap, err %s", err.Error())
	}

	return nil
}

func (r *GpuNodePolicyReconciler) addMigConfigMapData(ctx context.Context, instance *esv1.GpuNodePolicy) (bool, error) {
	modified := false
	origin := &corev1.ConfigMap{}
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: instance.GetNamespace(), Name: MigPartedDefaultConfigMapName}, origin)
	if err != nil {
		return false, fmt.Errorf("unable to get mig manager ConfigMap, err %s", err.Error())
	}
	patch := client.MergeFrom(origin)
	migConfigMap := origin.DeepCopy()

	oriConfig := &dpv1.Spec{}
	_ = yaml.Unmarshal([]byte(origin.Data["config.yaml"]), oriConfig)

	migConfig := &dpv1.Spec{}
	err = yaml.Unmarshal([]byte(migConfigMap.Data["config.yaml"]), migConfig)
	if err != nil {
		return false, fmt.Errorf("unmarshal error: %v", err)
	}

	if _, ok := migConfig.MigConfigs[instance.GetName()]; !ok && isMigNoneStrategy(instance.Spec.MigConfig) {
		r.Log.Info("No mig config found, skip")
		return false, nil
	}

	migSpecSlice := dpv1.MigConfigSpecSlice{}
	for _, config := range instance.Spec.MigConfig.ConfigSlice {
		spec := dpv1.MigConfigSpec{}
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

func (r *GpuNodePolicyReconciler) removeMigConfigMapData(ctx context.Context, instance *esv1.GpuNodePolicy) error {
	migConfigMap := &corev1.ConfigMap{}
	err := r.Client.Get(ctx, types.NamespacedName{Namespace: instance.GetNamespace(), Name: MigPartedDefaultConfigMapName}, migConfigMap)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Error(err, "MIG Manager ConfigMap is not Found")
			return nil
		}
		return fmt.Errorf("unable to get mig manager ConfigMap, err %s", err.Error())
	}
	patch := client.MergeFrom(migConfigMap.DeepCopy())

	migConfig := &dpv1.Spec{}
	err = yaml.Unmarshal([]byte(migConfigMap.Data["config.yaml"]), migConfig)
	if err != nil {
		return fmt.Errorf("unmarshal error: %v", err)
	}

	if _, ok := migConfig.MigConfigs[instance.GetName()]; !ok {
		r.Log.Info("No mig config need to be reverted", "name", instance.GetName())
		return nil
	}
	delete(migConfig.MigConfigs, instance.GetName())

	config, err := yaml.Marshal(migConfig)
	if err != nil {
		return fmt.Errorf("marshal error: %v", err)
	}
	migConfigMap.Data["config.yaml"] = string(config)

	err = r.Client.Patch(ctx, migConfigMap, patch)
	if err != nil {
		return fmt.Errorf("fail to revert mig manager ConfigMap, err %s", err.Error())
	}

	return nil
}

func convertSpecToDevicePluginConfig(in *esv1.TimeSlicingConfigSpec, out *dpv1.Config) {
	out.Version = dpv1.Version
	if in == nil {
		return
	}
	if out.Sharing == nil {
		out.Sharing = &dpv1.Sharing{}
	}
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

func convertSpecToMigConfigSpec(in *esv1.MigConfigSpecSlice, out *dpv1.MigConfigSpec) {
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
	return config.Strategy
}

func isMigNoneStrategy(config *esv1.MigConfigSpec) bool {
	return config == nil || len(config.ConfigSlice) == 0 || config.Strategy == ""
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
			continue
		}
		r.Log.Info("Node is adopted", "name", node.GetName())
		sync := false
		UpdateGpuNodePolicyStatus(mc.instance, node.GetName(), &sync, &sync)
	}
	if len(errList) > 0 {
		return fmt.Errorf(strings.Join(errList, " "))
	}
	return nil
}

func (r *GpuNodePolicyReconciler) UpdateNodes(ctx context.Context, nodes []corev1.Node, mc *gpuModeConfiguration) error {
	var errList []string
	var modified bool

	for _, node := range nodes {
		labels := node.GetLabels()
		patch := client.MergeFrom(node.DeepCopy())
		if v, ok := labels[gpuWorkloadConfigLabelKey]; ok && v == mc.config {
			if mc.isDevicePluginConfigUpdated(node.GetName()) && labels[DevicePluginDefaultConfigLabel] != ConfigSkipLabelValue {
				labels[DevicePluginDefaultConfigLabel] = ConfigSkipLabelValue
				r.Log.Info("Setting node label", "NodeName", node.GetName(), "Label", DevicePluginDefaultConfigLabel, "Value", ConfigSkipLabelValue)
				modified = true
			}
			if mc.isMigConfigUpdated(node.GetName()) && labels[MigConfigLabel] != ConfigSkipLabelValue {
				labels[MigConfigLabel] = ConfigSkipLabelValue
				r.Log.Info("Setting node label", "NodeName", node.GetName(), "Label", MigConfigLabel, "Value", ConfigSkipLabelValue)
				modified = true
			}
			if modified {
				node.SetLabels(labels)
				err := r.Client.Patch(ctx, &node, patch)
				if err != nil {
					errList = append(errList, fmt.Sprintf("failed to patch node %s, err %s",
						node.GetName(), err.Error()))
					continue
				}
				r.Log.Info("Skip labels are patched to node", "name", node.GetName())
				patch = client.MergeFrom(node.DeepCopy())
			}
		}
		modified = mc.updateGPUStateLabels(node.GetName(), labels)
		if !modified {
			r.Log.Info("Node does not need to be updated", "name", node.GetName())
			continue
		}
		// update node labels
		node.SetLabels(labels)
		err := r.Client.Patch(ctx, &node, patch)
		if err != nil {
			errList = append(errList, fmt.Sprintf("unable to update the GPU mode labels for node %s, err %s",
				node.GetName(), err.Error()))
			continue
		}
		r.Log.Info("Node is updated", "name", node.GetName())
		sync := false
		UpdateGpuNodePolicyStatus(mc.instance, node.GetName(), &sync, &sync)
	}
	if len(errList) > 0 {
		return fmt.Errorf(strings.Join(errList, " "))
	}
	return nil
}

func (r *GpuNodePolicyReconciler) ReleaseNodes(ctx context.Context, nodes []corev1.Node) error {
	var errList []string

	for _, node := range nodes {
		patch := client.MergeFrom(node.DeepCopy())

		labels := node.GetLabels()
		modified := removeAllGPUModeLabels(labels)
		if !modified {
			r.Log.Info("Node had been released", "name", node.GetName())
			continue
		}
		// update node labels
		node.SetLabels(labels)

		err := r.Client.Patch(ctx, &node, patch)
		if err != nil {
			errList = append(errList, fmt.Sprintf("unable to reset the GPU mode labels for node %s, err %s",
				node.GetName(), err.Error()))
			continue
		}
		r.Log.Info("Node is released", "name", node.GetName())
	}
	if len(errList) > 0 {
		return fmt.Errorf(strings.Join(errList, " "))
	}
	return nil
}

func (r *GpuNodePolicyReconciler) RevertMigNodes(ctx context.Context, nodes []corev1.Node) error {
	var errList []string

	for _, node := range nodes {
		patch := client.MergeFrom(node.DeepCopy())

		labels := node.GetLabels()
		if !hasMIGCapableGPU(labels) || labels[MigConfigLabel] == MigConfigLabelValueAllDisabled {
			continue
		}
		labels[migManagerLabelKey] = "true"
		labels[MigConfigLabel] = MigConfigLabelValueAllDisabled
		node.SetLabels(labels)

		err := r.Client.Patch(ctx, &node, patch)
		if err != nil {
			errList = append(errList, fmt.Sprintf("unable to revert mig mode for node %s, err %s",
				node.GetName(), err.Error()))
			continue
		}
		r.Log.Info("Node is reverted from mig mode", "name", node.GetName())
	}
	if len(errList) > 0 {
		return fmt.Errorf(strings.Join(errList, " "))
	}
	return nil
}

type gpuModeConfiguration struct {
	instance           *esv1.GpuNodePolicy
	mode               esv1.DeviceMode
	config             string
	devicePluginConfig string
	migConfig          string
	managedBy          string
	log                logr.Logger
}

func newGpuModeConfiguration(instance *esv1.GpuNodePolicy, log logr.Logger) *gpuModeConfiguration {
	cfg := setWorkloadConfig(instance.Spec.Mode)
	dpCfg := DevicePluginConfigLabelValueDefault
	migCfg := MigConfigLabelValueAllDisabled

	if cfg == gpuWorkloadConfigContainer && instance.Spec.Mode != esv1.Default {
		dpCfg = instance.GetName()
	}

	if instance.Spec.Mode == esv1.MIG {
		migCfg = instance.GetName()
	}

	return &gpuModeConfiguration{
		instance:           instance,
		mode:               instance.Spec.Mode,
		config:             cfg,
		devicePluginConfig: dpCfg,
		migConfig:          migCfg,
		managedBy:          instance.GetFullName(),
		log:                log,
	}
}

func (m *gpuModeConfiguration) updateGPUStateLabels(node string, labels map[string]string) bool {
	removed := m.removeGPUModeLabels(node, labels)
	added := m.addGPUModeLabels(node, labels)
	return removed || added
}

func (m *gpuModeConfiguration) addGPUModeLabels(node string, labels map[string]string) bool {
	modified := false
	for key, value := range gpuModeLabels[m.config] {
		if v, ok := labels[key]; !ok || v != value {
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
	if v, ok := labels[DevicePluginDefaultConfigLabel]; !ok || v != m.devicePluginConfig {
		m.log.Info("Setting node label", "NodeName", node, "Label", DevicePluginDefaultConfigLabel, "Value", m.devicePluginConfig)
		labels[DevicePluginDefaultConfigLabel] = m.devicePluginConfig
		modified = true
	}
	if v, ok := labels[MigConfigLabel]; !ok || v != m.migConfig {
		m.log.Info("Setting node label", "NodeName", node, "Label", MigConfigLabel, "Value", m.migConfig)
		labels[MigConfigLabel] = m.migConfig
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
		for key := range labelsMap {
			if _, ok := gpuModeLabels[m.config][key]; ok {
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
	/*if m.config != gpuWorkloadConfigContainer {
		m.log.Info("Deleting node label", "NodeName", node, "Label", DevicePluginDefaultConfigLabel)
		delete(labels, DevicePluginDefaultConfigLabel)
		modified = true
	}
	if m.mode != esv1.MIG {
		m.log.Info("Deleting node label", "NodeName", node, "Label", MigConfigLabel)
		delete(labels, MigConfigLabel)
		modified = true
	}*/
	return modified
}

func (m *gpuModeConfiguration) isDevicePluginConfigUpdated(node string) bool {
	status, ok := m.instance.Status.Nodes[node]
	if ok {
		switch m.instance.Spec.Mode {
		case esv1.TimeSlicing:
			return status.TimeSlicingMode.DevicePlugin.Sync
		case esv1.MIG:
			return status.MigMode.DevicePlugin.Sync
		}
	}
	return false
}

func (m *gpuModeConfiguration) isMigConfigUpdated(node string) bool {
	status, ok := m.instance.Status.Nodes[node]
	if ok {
		switch m.instance.Spec.Mode {
		case esv1.MIG:
			return status.MigMode.MigParted.Sync
		}
	}
	return false
}

func removeAllGPUModeLabels(labels map[string]string) bool {
	modified := false
	for k, labelsMap := range gpuModeLabels {
		if k == gpuWorkloadConfigNone {
			continue
		}
		for key := range labelsMap {
			if _, ok := labels[key]; ok {
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
	if _, ok := labels[DevicePluginDefaultConfigLabel]; ok {
		delete(labels, DevicePluginDefaultConfigLabel)
		modified = true
	}
	if _, ok := labels[MigConfigLabel]; ok {
		delete(labels, MigConfigLabel)
		modified = true
	}
	if v, ok := labels[commonOperandsLabelKey]; !ok || v != "false" {
		labels[commonOperandsLabelKey] = "false"
		modified = true
	}
	return modified
}

func setWorkloadConfig(mode esv1.DeviceMode) string {
	switch mode {
	case esv1.VCUDA:
		return gpuWorkloadConfigVcuda
	case esv1.Default, esv1.TimeSlicing, esv1.MIG:
		return gpuWorkloadConfigContainer
	case "":
		return gpuWorkloadConfigNone
	}
	return gpuWorkloadConfigNone
}

func InitGpuNodePolicyStatus(instance *esv1.GpuNodePolicy, nodes []corev1.Node) {
	if instance.Status.Nodes == nil {
		instance.Status.Nodes = make(map[string]esv1.GpuNodeStatus)
	}
	var needDPSync, needMigSync *bool
	if instance.Annotations[DevicePluginConfigAnnotation] == "true" {
		sync := true
		needDPSync = &sync
	}
	if instance.Annotations[MigConfigUpdateAnnotation] == "true" {
		sync := true
		needMigSync = &sync
	}
	for _, node := range nodes {
		UpdateGpuNodePolicyStatus(instance, node.GetName(), needDPSync, needMigSync)
	}
}

func UpdateGpuNodePolicyStatus(instance *esv1.GpuNodePolicy, node string, needDPSync, needMigSync *bool) {
	status, ok := instance.Status.Nodes[node]
	if !ok {
		status = esv1.GpuNodeStatus{}
	}

	switch instance.Spec.Mode {
	case esv1.TimeSlicing:
		if status.TimeSlicingMode == nil {
			status.TimeSlicingMode = &esv1.TimeSlicingModeStatus{
				Enabled: true,
				DevicePlugin: esv1.ConfigSyncStatus{
					Sync: false,
				},
			}
		}
		if needDPSync != nil {
			status.TimeSlicingMode.DevicePlugin.Sync = *needDPSync
		}
		status.MigMode = nil
		status.DefaultMode = nil
		status.VcudaMode = nil
	case esv1.MIG:
		if status.MigMode == nil {
			status.MigMode = &esv1.MigModeStatus{
				Enabled: true,
				DevicePlugin: esv1.ConfigSyncStatus{
					Sync: false,
				},
				MigParted: esv1.ConfigSyncStatus{
					Sync: false,
				},
			}
		}
		if needDPSync != nil {
			status.MigMode.DevicePlugin.Sync = *needDPSync
		}
		if needMigSync != nil {
			status.MigMode.MigParted.Sync = *needMigSync
		}
		status.TimeSlicingMode = nil
		status.DefaultMode = nil
		status.VcudaMode = nil
	case esv1.Default:
		if status.DefaultMode == nil {
			status.DefaultMode = &esv1.DefaultModeStatus{
				Enabled: true,
			}
		}
		status.TimeSlicingMode = nil
		status.VcudaMode = nil
		status.MigMode = nil
	case esv1.VCUDA:
		if status.VcudaMode == nil {
			status.VcudaMode = &esv1.VcudaModeStatus{
				Enabled: true,
			}
		}
		status.TimeSlicingMode = nil
		status.DefaultMode = nil
		status.MigMode = nil
	}
	instance.Status.Nodes[node] = status
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
