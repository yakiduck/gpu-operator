/**
# Copyright (c) NVIDIA CORPORATION. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
**/

package controllers

import (
	"context"
	"testing"

	gpuv1 "github.com/NVIDIA/gpu-operator/api/nvidia/v1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestUnassignedGPUWorkloadTransitions(t *testing.T) {
	originalDefault := defaultGPUWorkloadConfig
	t.Cleanup(func() { defaultGPUWorkloadConfig = originalDefault })
	defaultGPUWorkloadConfig = gpuWorkloadConfigNone
	for _, policyDefault := range []string{"", gpuWorkloadConfigNone} {
		config, err := getWorkloadConfig(nil, policyDefault)
		require.Error(t, err)
		require.Equal(t, gpuWorkloadConfigNone, config)
		config, err = getWorkloadConfig(map[string]string{gpuWorkloadConfigLabelKey: "invalid"}, policyDefault)
		require.Error(t, err)
		require.Equal(t, gpuWorkloadConfigNone, config)
		for _, workload := range []string{gpuWorkloadConfigNone, gpuWorkloadConfigContainer, gpuWorkloadConfigVMPassthrough, gpuWorkloadConfigVMVgpu} {
			config, err = getWorkloadConfig(map[string]string{gpuWorkloadConfigLabelKey: workload}, policyDefault)
			require.NoError(t, err)
			require.Equal(t, workload, config)
		}
	}
	labels := map[string]string{driverDeployLabelKey: "true", migManagerLabelKey: "true"}
	w := &gpuWorkloadConfiguration{config: gpuWorkloadConfigNone}
	require.True(t, w.updateGPUStateLabels(labels))
	require.Empty(t, labels)
	require.False(t, w.updateGPUStateLabels(labels))
	w.config = gpuWorkloadConfigContainer
	require.True(t, w.updateGPUStateLabels(labels))
	require.Equal(t, "true", labels[driverDeployLabelKey])
	labels[commonOperandsLabelKey] = "false"
	w.config = gpuWorkloadConfigNone
	require.True(t, w.updateGPUStateLabels(labels))
	require.Equal(t, "false", labels[commonOperandsLabelKey])
}

func TestDevicePluginDefaultConfigFallback(t *testing.T) {
	for _, pluginConfig := range []*gpuv1.DevicePluginConfig{nil, {}, {Default: "other"}} {
		config := &gpuv1.ClusterPolicySpec{}
		config.DevicePlugin.Config = pluginConfig
		setDevicePluginDefaultConfig(config)
		require.Equal(t, DevicePluginDefaultConfigMapName, config.DevicePlugin.Config.Name)
		require.Equal(t, DevicePluginDefaultConfig, config.DevicePlugin.Config.Default)
	}
	custom := &gpuv1.DevicePluginConfig{Name: "custom", Default: "shared"}
	config := &gpuv1.ClusterPolicySpec{}
	config.DevicePlugin.Config = custom
	setDevicePluginDefaultConfig(config)
	require.Same(t, custom, config.DevicePlugin.Config)
}

func TestEditableDefaultConfigMaps(t *testing.T) {
	for _, name := range []string{DevicePluginDefaultConfigMapName, MigPartedDefaultConfigMapName, "other"} {
		t.Run(name, func(t *testing.T) {
			s := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(s))
			require.NoError(t, gpuv1.AddToScheme(s))
			cp := &gpuv1.ClusterPolicy{ObjectMeta: metav1.ObjectMeta{Name: "test", UID: "policy"}}
			desired := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: map[string]string{"config": "default"}}
			c := fake.NewClientBuilder().WithScheme(s).Build()
			n := ClusterPolicyController{
				ctx: context.Background(), client: c, scheme: s, singleton: cp,
				operatorNamespace: "test", stateNames: []string{"state-device-plugin"},
				resources: []Resources{{ConfigMaps: []corev1.ConfigMap{desired}}},
			}
			state, err := createConfigMap(n, 0)
			require.NoError(t, err)
			require.Equal(t, gpuv1.Ready, state)
			found := &corev1.ConfigMap{}
			key := types.NamespacedName{Namespace: "test", Name: name}
			require.NoError(t, c.Get(n.ctx, key, found))
			require.Equal(t, desired.Data, found.Data)
			found.Data["config"] = "administrator edit"
			require.NoError(t, c.Update(n.ctx, found))
			state, err = createConfigMap(n, 0)
			require.NoError(t, err)
			require.Equal(t, gpuv1.Ready, state)
			require.NoError(t, c.Get(n.ctx, key, found))
			if name == "other" {
				require.Equal(t, "default", found.Data["config"])
			} else {
				require.Equal(t, "administrator edit", found.Data["config"])
			}
		})
	}
}

func TestNodeLabelingUnassignedGPU(t *testing.T) {
	labels := map[string]string{commonGPULabelKey: "true", driverDeployLabelKey: "true"}
	n := &nodeLabelingController{clusterPolicy: &gpuv1.ClusterPolicy{}, logger: logr.Discard()}
	require.True(t, n.updateGPUStateLabels(context.Background(), labels, "test-node"))
	require.Equal(t, map[string]string{commonGPULabelKey: "true"}, labels)
	labels[gpuWorkloadConfigLabelKey] = gpuWorkloadConfigContainer
	require.True(t, n.updateGPUStateLabels(context.Background(), labels, "test-node"))
	require.Equal(t, "true", labels[driverDeployLabelKey])
	labels[gpuWorkloadConfigLabelKey] = gpuWorkloadConfigNone
	require.True(t, n.updateGPUStateLabels(context.Background(), labels, "test-node"))
	require.NotContains(t, labels, driverDeployLabelKey)
	require.NotContains(t, labels, commonOperandsLabelKey)
	// A policy default is resolved directly from this reconciler's policy,
	// without waiting for the ClusterPolicy controller to update global state.
	delete(labels, gpuWorkloadConfigLabelKey)
	n.clusterPolicy.Spec.SandboxWorkloads.DefaultWorkload = gpuWorkloadConfigContainer
	require.True(t, n.updateGPUStateLabels(context.Background(), labels, "test-node"))
	require.Equal(t, "true", labels[driverDeployLabelKey])
}

func TestCustomPluginConfigDoesNotCreateDefault(t *testing.T) {
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, gpuv1.AddToScheme(s))
	cp := &gpuv1.ClusterPolicy{}
	cp.Spec.DevicePlugin.Config = &gpuv1.DevicePluginConfig{Name: "custom"}
	c := fake.NewClientBuilder().WithScheme(s).Build()
	n := ClusterPolicyController{
		ctx: context.Background(), client: c, scheme: s, singleton: cp,
		operatorNamespace: "test", stateNames: []string{"state-device-plugin"},
		resources: []Resources{{ConfigMaps: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: DevicePluginDefaultConfigMapName}}}}},
	}
	state, err := createConfigMap(n, 0)
	require.NoError(t, err)
	require.Equal(t, gpuv1.Ready, state)
	err = c.Get(n.ctx, types.NamespacedName{Namespace: "test", Name: DevicePluginDefaultConfigMapName}, &corev1.ConfigMap{})
	require.True(t, apierrors.IsNotFound(err))
}
