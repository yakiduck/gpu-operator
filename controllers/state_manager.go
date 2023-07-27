package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	gpuv1 "github.com/NVIDIA/gpu-operator/api/v1"
	secv1 "github.com/openshift/api/security/v1"
	promv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"

	"github.com/go-logr/logr"
	apiconfigv1 "github.com/openshift/api/config/v1"
	apiimagev1 "github.com/openshift/api/image/v1"
	configv1 "github.com/openshift/client-go/config/clientset/versioned/typed/config/v1"
	"golang.org/x/mod/semver"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

const (
	commonGPULabelKey                   = "nvidia.com/gpu.present"
	commonGPULabelValue                 = "true"
	commonOperandsLabelKey              = "nvidia.com/gpu.deploy.operands"
	commonOperandsLabelValue            = "true"
	migManagerLabelKey                  = "nvidia.com/gpu.deploy.mig-manager"
	migManagerLabelValue                = "true"
	migCapableLabelKey                  = "nvidia.com/mig.capable"
	migCapableLabelValue                = "true"
	vgpuHostDriverLabelKey              = "nvidia.com/vgpu.host-driver-version"
	gpuProductLabelKey                  = "nvidia.com/gpu.product"
	nfdLabelPrefix                      = "feature.node.kubernetes.io/"
	nfdKernelLabelKey                   = "feature.node.kubernetes.io/kernel-version.full"
	nfdOSTreeVersionLabelKey            = "feature.node.kubernetes.io/system-os_release.OSTREE_VERSION"
	ocpDriverToolkitVersionLabel        = "openshift.driver-toolkit.rhcos"
	ocpDriverToolkitIdentificationLabel = "openshift.driver-toolkit"
	ocpDriverToolkitIdentificationValue = "true"
	ocpNamespaceMonitoringLabelKey      = "openshift.io/cluster-monitoring"
	ocpNamespaceMonitoringLabelValue    = "true"
	// see bundle/manifests/gpu-operator.clusterserviceversion.yaml
	//     --> ClusterServiceVersion.metadata.annotations.operatorframework.io/suggested-namespace
	ocpSuggestedNamespace          = "nvidia-gpu-operator"
	gpuWorkloadConfigLabelKey      = "nvidia.com/gpu.workload.config"
	gpuWorkloadConfigNone          = "none"
	gpuWorkloadConfigContainer     = "container"
	gpuWorkloadConfigVcuda         = "vcuda"
	gpuWorkloadConfigVMPassthrough = "vm-passthrough"
	gpuWorkloadConfigVMVgpu        = "vm-vgpu"
	podSecurityLabelPrefix         = "pod-security.kubernetes.io/"
	podSecurityLevelPrivileged     = "privileged"
	driverAutoUpgradeAnnotationKey = "nvidia.com/gpu-driver-upgrade-enabled"
)

var (
	defaultGPUWorkloadConfig = gpuWorkloadConfigNone
	podSecurityModes         = []string{"enforce", "audit", "warn"}
)

var gpuStateLabels = map[string]map[string]string{
	gpuWorkloadConfigNone: {
		commonOperandsLabelKey: "false",
	},
	gpuWorkloadConfigContainer: {
		"nvidia.com/gpu.deploy.driver":                "true",
		"nvidia.com/gpu.deploy.gpu-feature-discovery": "true",
		"nvidia.com/gpu.deploy.container-toolkit":     "true",
		"nvidia.com/gpu.deploy.device-plugin":         "true",
		"nvidia.com/gpu.deploy.dcgm":                  "true",
		"nvidia.com/gpu.deploy.dcgm-exporter":         "true",
		"nvidia.com/gpu.deploy.node-status-exporter":  "true",
		"nvidia.com/gpu.deploy.operator-validator":    "true",
	},
	gpuWorkloadConfigVcuda: {
		"nvidia.com/gpu.deploy.driver":         "true",
		"easystack.com/gpu.deploy.gpu-manager": "true",
	},
	gpuWorkloadConfigVMPassthrough: {
		"nvidia.com/gpu.deploy.sandbox-device-plugin": "true",
		"nvidia.com/gpu.deploy.sandbox-validator":     "true",
		"nvidia.com/gpu.deploy.vfio-manager":          "true",
	},
	gpuWorkloadConfigVMVgpu: {
		"nvidia.com/gpu.deploy.sandbox-device-plugin": "true",
		"nvidia.com/gpu.deploy.vgpu-manager":          "true",
		"nvidia.com/gpu.deploy.vgpu-device-manager":   "true",
		"nvidia.com/gpu.deploy.sandbox-validator":     "true",
	},
}

var gpuNodeLabels = map[string]string{
	"feature.node.kubernetes.io/pci-10de.present":      "true",
	"feature.node.kubernetes.io/pci-0302_10de.present": "true",
	"feature.node.kubernetes.io/pci-0300_10de.present": "true",
}

type state interface {
	init(*ClusterPolicyReconciler, *gpuv1.ClusterPolicy)
	step()
	validate()
	last()
}

type gpuWorkloadConfiguration struct {
	config string
	node   string
	log    logr.Logger
}

// OpenShiftDriverToolkit contains the values required to deploy
// OpenShift DriverToolkit DaemonSet.
type OpenShiftDriverToolkit struct {
	// true if the cluster runs OpenShift and
	// Operator.UseOpenShiftDriverToolkit is turned on in the
	// ClusterPolicy
	requested bool
	// true of the DriverToolkit is requested and the cluster has all
	// the required components (NFD RHCOS OSTree label + OCP
	// DriverToolkit imagestream)
	enabled bool

	currentRhcosVersion      string
	rhcosVersions            map[string]bool
	rhcosDriverToolkitImages map[string]string
}

// ClusterPolicyController represents clusterpolicy controller spec for GPU operator
type ClusterPolicyController struct {
	ctx               context.Context
	singleton         *gpuv1.ClusterPolicy
	operatorNamespace string

	resources       []Resources
	controls        []controlFunc
	stateNames      []string
	operatorMetrics *OperatorMetrics
	rec             *ClusterPolicyReconciler
	idx             int

	k8sVersion       string
	openshift        string
	ocpDriverToolkit OpenShiftDriverToolkit

	runtime        gpuv1.Runtime
	hasGPUNodes    bool
	hasNFDLabels   bool
	sandboxEnabled bool
}

func addState(n *ClusterPolicyController, path string) error {
	// TODO check for path
	res, ctrl := addResourcesControls(n, path)

	n.controls = append(n.controls, ctrl)
	n.resources = append(n.resources, res)
	n.stateNames = append(n.stateNames, filepath.Base(path))

	return nil
}

// OpenshiftVersion fetches OCP version
func OpenshiftVersion(ctx context.Context) (string, error) {
	cfg := config.GetConfigOrDie()
	client, err := configv1.NewForConfig(cfg)
	if err != nil {
		return "", err
	}

	v, err := client.ClusterVersions().Get(ctx, "version", metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	for _, condition := range v.Status.History {
		if condition.State != "Completed" {
			continue
		}

		ocpV := strings.Split(condition.Version, ".")
		if len(ocpV) > 1 {
			return ocpV[0] + "." + ocpV[1], nil
		}
		return ocpV[0], nil
	}

	return "", fmt.Errorf("Failed to find Completed Cluster Version")
}

// KubernetesVersion fetches the Kubernetes API server version
func KubernetesVersion() (string, error) {
	cfg := config.GetConfigOrDie()
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("error building discovery client: %v", err)
	}

	info, err := discoveryClient.ServerVersion()
	if err != nil {
		return "", fmt.Errorf("unable to fetch server version information: %v", err)
	}

	return info.GitVersion, nil
}

// GetClusterWideProxy returns cluster wide proxy object setup in OCP
func GetClusterWideProxy(ctx context.Context) (*apiconfigv1.Proxy, error) {
	cfg := config.GetConfigOrDie()
	client, err := configv1.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	proxy, err := client.Proxies().Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return proxy, nil
}

// hasCommonGPULabel returns true if common Nvidia GPU label exists among provided node labels
func hasCommonGPULabel(labels map[string]string) bool {
	if _, ok := labels[commonGPULabelKey]; ok {
		if labels[commonGPULabelKey] == commonGPULabelValue {
			// node is already labelled with common label
			return true
		}
	}
	return false
}

// hasGPULabels return true if node labels contain Nvidia GPU labels
func hasGPULabels(labels map[string]string) bool {
	for key, val := range labels {
		if _, ok := gpuNodeLabels[key]; ok {
			if gpuNodeLabels[key] == val {
				return true
			}
		}
	}
	return false
}

// hasNFDLabels return true if node labels contain NFD labels
func hasNFDLabels(labels map[string]string) bool {
	for key := range labels {
		if strings.HasPrefix(key, nfdLabelPrefix) {
			return true
		}
	}
	return false
}

// hasMIGCapableGPU returns true if this node has GPU capable of MIG partitioning.
func hasMIGCapableGPU(labels map[string]string) bool {
	if value, exists := labels[vgpuHostDriverLabelKey]; exists && value != "" {
		// vGPU node
		return false
	}

	if value, exists := labels[migCapableLabelKey]; exists {
		if value == migCapableLabelValue {
			return true
		}
		return false
	}

	// check product label if mig.capable label does not exist
	if value, exists := labels[gpuProductLabelKey]; exists {
		if strings.Contains(strings.ToLower(value), "h100") ||
			strings.Contains(strings.ToLower(value), "a100") ||
			strings.Contains(strings.ToLower(value), "a30") {
			return true
		}
	}

	return false
}

func hasMIGManagerLabel(labels map[string]string) bool {
	for key := range labels {
		if key == migManagerLabelKey {
			return true
		}
	}
	return false
}

func hasOperandsDisabled(labels map[string]string) bool {
	if value, ok := labels[commonOperandsLabelKey]; ok {
		if value == "false" {
			return true
		}
	}
	return false
}

func isValidWorkloadConfig(workloadConfig string) bool {
	_, ok := gpuStateLabels[workloadConfig]
	return ok
}

// getWorkloadConfig returns the GPU workload configured for the node.
// If an error occurs when searching for the workload config,
// return defaultGPUWorkloadConfig.
func getWorkloadConfig(labels map[string]string, sandboxEnabled bool) (string, error) {
	if !sandboxEnabled {
		return gpuWorkloadConfigContainer, nil
	}
	if workloadConfig, ok := labels[gpuWorkloadConfigLabelKey]; ok {
		if isValidWorkloadConfig(workloadConfig) {
			return workloadConfig, nil
		}
		return defaultGPUWorkloadConfig, fmt.Errorf("Invalid GPU workload config: %v", workloadConfig)
	}
	return defaultGPUWorkloadConfig, fmt.Errorf("No GPU workload config found")
}

// removeAllGPUStateLabels removes all gpuStateLabels from the provided map of node labels.
// removeAllGPUStateLabels returns true if the labels map has been modified.
func removeAllGPUStateLabels(labels map[string]string) bool {
	modified := false
	for _, labelsMap := range gpuStateLabels {
		for key := range labelsMap {
			if _, ok := labels[key]; ok {
				delete(labels, key)
				modified = true
			}
		}
	}
	if _, ok := labels[migManagerLabelKey]; ok {
		delete(labels, migManagerLabelKey)
		modified = true
	}
	return modified
}

// updateGPUStateLabels applies the correct GPU state labels for the GPU workload configuration.
// updateGPUStateLabels returns true if the input labels map is modified.
func (w *gpuWorkloadConfiguration) updateGPUStateLabels(labels map[string]string) bool {
	if hasOperandsDisabled(labels) {
		// Operands are disabled, delete all GPU state labels
		w.log.Info("Operands are disabled for node", "NodeName", w.node, "Label", commonOperandsLabelKey, "Value", "false")
		w.log.Info("Disabling all operands for node", "NodeName", w.node)
		return removeAllGPUStateLabels(labels)
	}
	removed := w.removeGPUStateLabels(labels)
	added := w.addGPUStateLabels(labels)
	return removed || added
}

// addGPUStateLabels adds GPU state labels needed for the GPU workload configuration.
// If a required state label already exists on the node, honor the current value.
func (w *gpuWorkloadConfiguration) addGPUStateLabels(labels map[string]string) bool {
	modified := false
	for key, value := range gpuStateLabels[w.config] {
		if _, ok := labels[key]; !ok {
			w.log.Info("Setting node label", "NodeName", w.node, "Label", key, "Value", value)
			labels[key] = value
			modified = true
		}
	}
	if w.config == gpuWorkloadConfigContainer && hasMIGCapableGPU(labels) && !hasMIGManagerLabel(labels) {
		w.log.Info("Setting node label", "NodeName", w.node, "Label", migManagerLabelKey, "Value", migManagerLabelValue)
		labels[migManagerLabelKey] = migManagerLabelValue
		modified = true
	}
	return modified
}

// removeGPUStateLabels removes GPU state labels not needed for the GPU workload configuration
func (w *gpuWorkloadConfiguration) removeGPUStateLabels(labels map[string]string) bool {
	modified := false
	for workloadConfig, labelsMap := range gpuStateLabels {
		if workloadConfig == w.config {
			continue
		}
		for key := range labelsMap {
			if _, ok := gpuStateLabels[w.config][key]; ok {
				// skip label if it is in the set of states for workloadConfig
				continue
			}
			if _, ok := labels[key]; ok {
				w.log.Info("Deleting node label", "NodeName", w.node, "Label", key)
				delete(labels, key)
				modified = true
			}
		}
	}
	if w.config != gpuWorkloadConfigContainer {
		if _, ok := labels[migManagerLabelKey]; ok {
			w.log.Info("Deleting node label", "NodeName", w.node, "Label", migManagerLabelKey)
			delete(labels, migManagerLabelKey)
			modified = true
		}
	}
	return modified
}

func (n *ClusterPolicyController) applyDriverAutoUpgradeAnnotation() error {
	// fetch all nodes
	opts := []client.ListOption{}
	list := &corev1.NodeList{}
	err := n.rec.Client.List(n.ctx, list, opts...)
	if err != nil {
		return fmt.Errorf("Unable to list nodes to check annotations, err %s", err.Error())
	}
	for _, node := range list.Items {
		labels := node.GetLabels()
		if !hasCommonGPULabel(labels) {
			// not a gpu node
			continue
		}
		// set node annotation for driver auto-upgrade
		updateRequired := false
		value := "true"
		annotationValue, annotationExists := node.ObjectMeta.Annotations[driverAutoUpgradeAnnotationKey]
		if n.singleton.Spec.Driver.UpgradePolicy != nil &&
			n.singleton.Spec.Driver.UpgradePolicy.AutoUpgrade &&
			!n.sandboxEnabled {
			// check if we need to add the annotation
			if !annotationExists {
				updateRequired = true
			} else if annotationValue != "true" {
				updateRequired = true
			}
		} else {
			// check if we need to remove the annotation
			if annotationExists {
				updateRequired = true
			}
			value = "null"
		}
		if !updateRequired {
			continue
		}
		// update annotation
		node.ObjectMeta.Annotations[driverAutoUpgradeAnnotationKey] = value
		if value == "null" {
			// remove annotation if value is null
			delete(node.ObjectMeta.Annotations, driverAutoUpgradeAnnotationKey)
		}
		err := n.rec.Client.Update(n.ctx, &node)
		if err != nil {
			n.rec.Log.Info("Failed to update node state annotation on a node",
				"node", node.Name,
				"annotationKey", driverAutoUpgradeAnnotationKey,
				"annotationValue", value, "error", err)
			return err
		}
	}
	return nil
}

// labelGPUNodes labels nodes with GPU's with NVIDIA common label
// it return clusterHasNFDLabels (bool), gpuNodesTotal (int), error
func (n *ClusterPolicyController) labelGPUNodes() (bool, int, error) {
	ctx := n.ctx
	// fetch all nodes
	opts := []client.ListOption{}
	list := &corev1.NodeList{}
	err := n.rec.Client.List(ctx, list, opts...)
	if err != nil {
		return false, 0, fmt.Errorf("Unable to list nodes to check labels, err %s", err.Error())
	}

	clusterHasNFDLabels := false
	gpuNodesTotal := 0
	for _, node := range list.Items {
		// get node labels
		labels := node.GetLabels()
		if !clusterHasNFDLabels {
			clusterHasNFDLabels = hasNFDLabels(labels)
		}
		config, err := getWorkloadConfig(labels, n.sandboxEnabled)
		if err != nil {
			n.rec.Log.Info("WARNING: failed to get GPU workload config for node; using default",
				"NodeName", node.ObjectMeta.Name, "SandboxEnabled", n.sandboxEnabled,
				"Error", err, "defaultGPUWorkloadConfig", defaultGPUWorkloadConfig)
		}
		n.rec.Log.Info("GPU workload configuration", "NodeName", node.ObjectMeta.Name, "GpuWorkloadConfig", config)
		gpuWorkloadConfig := &gpuWorkloadConfiguration{config, node.ObjectMeta.Name, n.rec.Log}
		if !hasCommonGPULabel(labels) && hasGPULabels(labels) {
			n.rec.Log.Info("Node has GPU(s)", "NodeName", node.ObjectMeta.Name)
			// label the node with common Nvidia GPU label
			n.rec.Log.Info("Setting node label", "NodeName", node.ObjectMeta.Name, "Label", commonGPULabelKey, "Value", commonGPULabelValue)
			labels[commonGPULabelKey] = commonGPULabelValue
			// label the node with the state GPU labels
			n.rec.Log.Info("Applying correct GPU state labels to the node", "NodeName", node.ObjectMeta.Name)
			gpuWorkloadConfig.updateGPUStateLabels(labels)
			// update node labels
			node.SetLabels(labels)
			err = n.rec.Client.Update(ctx, &node)
			if err != nil {
				return false, 0, fmt.Errorf("Unable to label node %s for the GPU Operator deployment, err %s",
					node.ObjectMeta.Name, err.Error())
			}
		} else if hasCommonGPULabel(labels) && !hasGPULabels(labels) {
			// previously labelled node and no longer has GPU's
			// label node to reset common Nvidia GPU label
			n.rec.Log.Info("Node no longer has GPUs", "NodeName", node.ObjectMeta.Name)
			n.rec.Log.Info("Setting node label", "Label", commonGPULabelKey, "Value", "false")
			labels[commonGPULabelKey] = "false"
			n.rec.Log.Info("Disabling all operands for node", "NodeName", node.ObjectMeta.Name)
			removeAllGPUStateLabels(labels)
			// update node labels
			node.SetLabels(labels)
			err = n.rec.Client.Update(ctx, &node)
			if err != nil {
				return false, 0, fmt.Errorf("Unable to reset the GPU Operator labels for node %s, err %s",
					node.ObjectMeta.Name, err.Error())
			}
		}
		if hasCommonGPULabel(labels) {
			n.rec.Log.Info("Checking GPU state labels on the node", "NodeName", node.ObjectMeta.Name)
			if gpuWorkloadConfig.updateGPUStateLabels(labels) {
				n.rec.Log.Info("Applying correct GPU state labels to the node", "NodeName", node.ObjectMeta.Name)
				node.SetLabels(labels)
				err = n.rec.Client.Update(ctx, &node)
				if err != nil {
					return false, 0, fmt.Errorf("Unable to update the GPU Operator labels for node %s, err %s",
						node.ObjectMeta.Name, err.Error())
				}
			}
			gpuNodesTotal++

			if n.ocpDriverToolkit.requested {
				rhcosVersion, ok := labels[nfdOSTreeVersionLabelKey]
				if ok {
					n.ocpDriverToolkit.rhcosVersions[rhcosVersion] = true
					n.rec.Log.V(1).Info("GPU node running RHCOS",
						"nodeName", node.ObjectMeta.Name,
						"RHCOS version", rhcosVersion,
					)
				} else {
					n.rec.Log.Info("node doesn't have the proper NFD RHCOS version label.",
						"nodeName", node.ObjectMeta.Name,
						"nfdLabel", nfdOSTreeVersionLabelKey,
					)
				}

			}
		}
	}
	n.rec.Log.Info("Number of nodes with GPU label", "NodeCount", gpuNodesTotal)
	n.operatorMetrics.gpuNodesTotal.Set(float64(gpuNodesTotal))

	return clusterHasNFDLabels, gpuNodesTotal, nil
}

func getRuntimeString(node corev1.Node) (gpuv1.Runtime, error) {
	// ContainerRuntimeVersion string will look like <runtime>://<x.y.z>
	runtimeVer := node.Status.NodeInfo.ContainerRuntimeVersion
	var runtime gpuv1.Runtime
	if strings.HasPrefix(runtimeVer, "docker") {
		runtime = gpuv1.Docker
	} else if strings.HasPrefix(runtimeVer, "containerd") {
		runtime = gpuv1.Containerd
	} else if strings.HasPrefix(runtimeVer, "cri-o") {
		runtime = gpuv1.CRIO
	} else {
		return "", fmt.Errorf("runtime not recognized: %s", runtimeVer)
	}
	return runtime, nil
}

func (n *ClusterPolicyController) setPodSecurityLabelsForNamespace() error {
	ctx := n.ctx
	namespaceName := clusterPolicyCtrl.operatorNamespace

	if n.openshift != "" && namespaceName != ocpSuggestedNamespace {
		// The GPU Operator is not installed in the suggested
		// namespace, so the namespace may be shared with other
		// untrusted operators.  Do not set Pod Security Admission labels.
		n.rec.Log.Info("GPU Operator is not installed in the suggested namespace. Not setting Pod Security Admission labels for namespace",
			"namespace", namespaceName,
			"suggested namespace", ocpSuggestedNamespace)
		return nil
	}

	ns := &corev1.Namespace{}
	opts := client.ObjectKey{Name: namespaceName}
	err := n.rec.Client.Get(ctx, opts, ns)
	if err != nil {
		return fmt.Errorf("ERROR: could not get Namespace %s from client: %v", namespaceName, err)
	}

	patch := client.MergeFrom(ns.DeepCopy())
	modified := false
	// On K8s<1.21, namespaces are not automatically labeled with an immutable label. Initialize
	// a labels map if needed before adding PSA labels.
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/#automatic-labelling
	if ns.ObjectMeta.Labels == nil {
		ns.ObjectMeta.Labels = make(map[string]string)
		modified = true
	}
	for _, mode := range podSecurityModes {
		key := podSecurityLabelPrefix + mode
		if val, ok := ns.ObjectMeta.Labels[key]; !ok || (val != podSecurityLevelPrivileged) {
			ns.ObjectMeta.Labels[key] = podSecurityLevelPrivileged
			modified = true
		}
	}

	if !modified {
		return nil
	}

	err = n.rec.Client.Patch(ctx, ns, patch)
	if err != nil {
		return fmt.Errorf("unable to label namespace %s with pod security levels: %v", namespaceName, err)
	}

	return nil
}

func (n *ClusterPolicyController) ocpEnsureNamespaceMonitoring() error {
	ctx := n.ctx
	namespaceName := clusterPolicyCtrl.operatorNamespace

	if namespaceName != ocpSuggestedNamespace {
		// The GPU Operator is not installed in the suggested
		// namespace, so the namespace may be shared with other
		// untrusted operators.  Do not enable namespace monitoring in
		// this case, as per OpenShift/Prometheus best practices.
		n.rec.Log.Info("GPU Operator not installed in the suggested namespace, skipping namespace monitoring verification",
			"namespace", namespaceName,
			"suggested namespace", ocpSuggestedNamespace)
		return nil
	}

	ns := &corev1.Namespace{}
	opts := client.ObjectKey{Name: namespaceName}
	err := n.rec.Client.Get(ctx, opts, ns)
	if err != nil {
		return fmt.Errorf("ERROR: could not get Namespace %s from client: %v", namespaceName, err)
	}

	val, ok := ns.ObjectMeta.Labels[ocpNamespaceMonitoringLabelKey]
	if ok {
		// label already defined, do not change it
		var msg string
		if val == ocpNamespaceMonitoringLabelValue {
			msg = "OpenShift monitoring is enabled on the GPU Operator namespace"
		} else {
			msg = "WARNING: OpenShift monitoring currently disabled on user request"
		}
		n.rec.Log.Info(msg,
			"namespace", namespaceName,
			"label", ocpNamespaceMonitoringLabelKey,
			"value", val,
			"excepted value", ocpNamespaceMonitoringLabelValue)

		return nil
	}

	// label not defined, enable monitoring
	n.rec.Log.Info("Enabling OpenShift monitoring")
	n.rec.Log.V(1).Info("Adding monitoring label to the operator namespace",
		"namespace", namespaceName,
		"label", ocpNamespaceMonitoringLabelKey,
		"value", ocpNamespaceMonitoringLabelValue)
	n.rec.Log.Info("Monitoring can be disabled by setting the namespace label " +
		ocpNamespaceMonitoringLabelKey + "=false")
	patch := client.MergeFrom(ns.DeepCopy())
	ns.ObjectMeta.Labels[ocpNamespaceMonitoringLabelKey] = ocpNamespaceMonitoringLabelValue
	err = n.rec.Client.Patch(ctx, ns, patch)
	if err != nil {
		return fmt.Errorf("Unable to label namespace %s for the GPU Operator monitoring, err %s",
			namespaceName, err.Error())
	}

	return nil
}

// getRuntime will detect the container runtime used by nodes in the
// cluster and correctly set the value for clusterPolicyController.runtime
// For openshift, set runtime to crio. Otherwise, the default runtime is
// containerd -- if >=1 node is configured with containerd, set
// clusterPolicyController.runtime = containerd
func (n *ClusterPolicyController) getRuntime() error {
	ctx := n.ctx
	// assume crio for openshift clusters
	if n.openshift != "" {
		n.runtime = gpuv1.CRIO
		return nil
	}

	opts := []client.ListOption{
		client.MatchingLabels{commonGPULabelKey: "true"},
	}
	list := &corev1.NodeList{}
	err := n.rec.Client.List(ctx, list, opts...)
	if err != nil {
		return fmt.Errorf("Unable to list nodes prior to checking container runtime: %v", err)
	}

	var runtime gpuv1.Runtime
	for _, node := range list.Items {
		rt, err := getRuntimeString(node)
		if err != nil {
			n.rec.Log.Info(fmt.Sprintf("Unable to get runtime info for node %s: %v", node.Name, err))
			continue
		}
		runtime = rt
		if runtime == gpuv1.Containerd {
			// default to containerd if >=1 node running containerd
			break
		}
	}

	if runtime.String() == "" {
		n.rec.Log.Info("Unable to get runtime info from the cluster, defaulting to containerd")
		runtime = gpuv1.Containerd
	}
	n.runtime = runtime
	return nil
}

func (n *ClusterPolicyController) init(ctx context.Context, reconciler *ClusterPolicyReconciler, clusterPolicy *gpuv1.ClusterPolicy) error {
	n.singleton = clusterPolicy
	n.ctx = ctx
	n.rec = reconciler
	n.idx = 0

	if len(n.controls) == 0 {
		clusterPolicyCtrl.operatorNamespace = os.Getenv("OPERATOR_NAMESPACE")

		if clusterPolicyCtrl.operatorNamespace == "" {
			n.rec.Log.Error(nil, "OPERATOR_NAMESPACE environment variable not set, cannot proceed")
			// we cannot do anything without the operator namespace,
			// let the operator Pod run into `CrashloopBackOff`

			os.Exit(1)
		}

		version, err := OpenshiftVersion(ctx)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		n.openshift = version

		k8sVersion, err := KubernetesVersion()
		if err != nil {
			return err
		}
		if !semver.IsValid(k8sVersion) {
			return fmt.Errorf("k8s version detected '%s' is not a valid semantic version", k8sVersion)
		}
		n.k8sVersion = k8sVersion
		n.rec.Log.Info("Kubernetes version detected", "version", k8sVersion)

		promv1.AddToScheme(reconciler.Scheme)
		secv1.Install(reconciler.Scheme)
		apiconfigv1.Install(reconciler.Scheme)
		apiimagev1.Install(reconciler.Scheme)

		n.operatorMetrics = initOperatorMetrics(n)
		n.rec.Log.Info("Operator metrics initialized.")

		addState(n, "/opt/gpu-operator/pre-requisites")
		addState(n, "/opt/gpu-operator/state-operator-metrics")
		addState(n, "/opt/gpu-operator/state-driver")
		addState(n, "/opt/gpu-operator/state-container-toolkit")
		addState(n, "/opt/gpu-operator/state-operator-validation")
		addState(n, "/opt/gpu-operator/state-device-plugin")
		addState(n, "/opt/gpu-operator/state-dcgm")
		addState(n, "/opt/gpu-operator/state-dcgm-exporter")
		addState(n, "/opt/gpu-operator/gpu-feature-discovery")
		addState(n, "/opt/gpu-operator/state-mig-manager")
		addState(n, "/opt/gpu-operator/state-node-status-exporter")
		// add sandbox workload states
		addState(n, "/opt/gpu-operator/state-vgpu-manager")
		addState(n, "/opt/gpu-operator/state-vgpu-device-manager")
		addState(n, "/opt/gpu-operator/state-sandbox-validation")
		addState(n, "/opt/gpu-operator/state-vfio-manager")
		addState(n, "/opt/gpu-operator/state-sandbox-device-plugin")
	}

	if clusterPolicy.Spec.SandboxWorkloads.IsEnabled() {
		n.sandboxEnabled = true
		// defaultGPUWorkloadConfig is container, unless
		// user overrides in ClusterPolicy with a valid GPU
		// workload configuration
		defaultWorkload := clusterPolicy.Spec.SandboxWorkloads.DefaultWorkload
		if isValidWorkloadConfig(defaultWorkload) {
			n.rec.Log.Info("Default GPU workload is overridden in ClusterPolicy", "DefaultWorkload", defaultWorkload)
			defaultGPUWorkloadConfig = defaultWorkload
		}
	} else {
		n.sandboxEnabled = false
	}
	n.rec.Log.Info("Sandbox workloads", "Enabled", n.sandboxEnabled, "DefaultWorkload", defaultGPUWorkloadConfig)

	if n.openshift != "" && (n.singleton.Spec.Operator.UseOpenShiftDriverToolkit == nil ||
		*n.singleton.Spec.Operator.UseOpenShiftDriverToolkit) {
		// DTK is enabled by default on OpenShift
		n.ocpDriverToolkit.requested = true

		// mind that this is executed at every reconciliation loop,
		// do not assume "permanent" data storage.
		n.ocpDriverToolkit.rhcosVersions = make(map[string]bool)
		n.ocpDriverToolkit.rhcosDriverToolkitImages = make(map[string]string)
	} else {
		// DTK is disabled on non-OpenShift deployments or when operator.use_ocp_driver_toolkit
		// is explicitly set to 'false' in ClusterPolicy
		n.ocpDriverToolkit.requested = false
		n.ocpDriverToolkit.enabled = false

		n.operatorMetrics.openshiftDriverToolkitEnabled.Set(openshiftDriverToolkitDisabled)
	}

	if clusterPolicy.Spec.PSP.IsEnabled() {
		// label namespace with Pod Security Admission levels
		n.rec.Log.Info("Pod Security is enabled. Adding labels to GPU Operator namespace", "namespace", n.operatorNamespace)
		err := n.setPodSecurityLabelsForNamespace()
		if err != nil {
			return err
		}
		n.rec.Log.Info("Pod Security Admission labels added to GPU Operator namespace", "namespace", n.operatorNamespace)
	}

	// fetch all nodes and label gpu nodes
	hasNFDLabels, gpuNodeCount, err := n.labelGPUNodes()
	if err != nil {
		return err
	}
	n.hasGPUNodes = gpuNodeCount != 0
	n.hasNFDLabels = hasNFDLabels

	// fetch all nodes and annotate gpu nodes
	err = n.applyDriverAutoUpgradeAnnotation()
	if err != nil {
		return err
	}

	// detect the container runtime on worker nodes
	err = n.getRuntime()
	if err != nil {
		return err
	}
	n.rec.Log.Info(fmt.Sprintf("Using container runtime: %s", n.runtime.String()))

	if n.ocpDriverToolkit.requested {
		hasImageStream, err := ocpHasDriverToolkitImageStream(n)
		if err != nil {
			n.rec.Log.Info("ocpHasDriverToolkitImageStream", "err", err)
			return err
		}

		hasCompatibleNFD := len(n.ocpDriverToolkit.rhcosVersions) != 0
		n.ocpDriverToolkit.enabled = hasImageStream && hasCompatibleNFD

		if n.ocpDriverToolkit.enabled {
			n.operatorMetrics.openshiftDriverToolkitEnabled.Set(openshiftDriverToolkitEnabled)
		} else {
			n.operatorMetrics.openshiftDriverToolkitEnabled.Set(openshiftDriverToolkitNotPossible)
		}
		n.rec.Log.Info("OpenShift Driver Toolkit requested",
			"hasCompatibleNFD", hasCompatibleNFD,
			"hasDriverToolkitImageStream", hasImageStream)

		n.rec.Log.Info("OpenShift Driver Toolkit",
			"enabled", n.ocpDriverToolkit.enabled)

		if hasImageStream {
			n.operatorMetrics.openshiftDriverToolkitIsMissing.Set(0)
		} else {
			n.operatorMetrics.openshiftDriverToolkitIsMissing.Set(1)
		}
		if n.hasGPUNodes && !hasCompatibleNFD {
			n.operatorMetrics.openshiftDriverToolkitNfdTooOld.Set(1)
		} else {
			n.operatorMetrics.openshiftDriverToolkitNfdTooOld.Set(0)
		}
	}

	if n.openshift != "" {
		if err := n.ocpEnsureNamespaceMonitoring(); err != nil {
			return err
		}
	}

	return nil
}

func (n *ClusterPolicyController) step() (gpuv1.State, error) {
	result := gpuv1.Ready
	for _, fs := range n.controls[n.idx] {
		stat, err := fs(*n)
		if err != nil {
			return stat, err
		}
		// successfully deployed resource, now check if its ready
		if stat != gpuv1.Ready {
			// mark overall status of this component as not-ready and continue with other resources, while this becomes ready
			result = stat
		}
	}

	// move to next state
	n.idx = n.idx + 1

	return result, nil
}

func (n ClusterPolicyController) validate() {
	// TODO add custom validation functions
}

func (n ClusterPolicyController) last() bool {
	if n.idx == len(n.controls) {
		return true
	}
	return false
}

func (n ClusterPolicyController) isStateEnabled(stateName string) bool {
	clusterPolicySpec := &n.singleton.Spec

	switch stateName {
	case "state-driver":
		return clusterPolicySpec.Driver.IsEnabled()
	case "state-container-toolkit":
		return clusterPolicySpec.Toolkit.IsEnabled()
	case "state-device-plugin":
		return clusterPolicySpec.DevicePlugin.IsEnabled()
	case "state-dcgm":
		return clusterPolicySpec.DCGM.IsEnabled()
	case "state-dcgm-exporter":
		return clusterPolicySpec.DCGMExporter.IsEnabled()
	case "state-mig-manager":
		return clusterPolicySpec.MIGManager.IsEnabled()
	case "gpu-feature-discovery":
		return clusterPolicySpec.GPUFeatureDiscovery.IsEnabled()
	case "state-node-status-exporter":
		return clusterPolicySpec.NodeStatusExporter.IsEnabled()
	case "state-sandbox-device-plugin":
		return n.sandboxEnabled && clusterPolicySpec.SandboxDevicePlugin.IsEnabled()
	case "state-vfio-manager":
		return n.sandboxEnabled && clusterPolicySpec.VFIOManager.IsEnabled()
	case "state-vgpu-device-manager":
		return n.sandboxEnabled && clusterPolicySpec.VGPUDeviceManager.IsEnabled()
	case "state-vgpu-manager":
		return n.sandboxEnabled && clusterPolicySpec.VGPUManager.IsEnabled()
	case "state-sandbox-validation":
		return n.sandboxEnabled
	case "state-operator-validation":
		return true
	case "state-operator-metrics":
		return true
	default:
		n.rec.Log.Error(nil, "invalid state passed", "stateName", stateName)
		return false
	}
}
