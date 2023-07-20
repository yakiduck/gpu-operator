package controllers

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"testing"

	dpv1 "github.com/NVIDIA/gpu-operator/apis/config/v1"
	esv1 "github.com/NVIDIA/gpu-operator/apis/ecns.easystack.com/v1"
)

func TestConvert(t *testing.T) {
	in := &esv1.TimeSlicingConfigSpec{
		RenameByDefault:            true,
		FailRequestsGreaterThanOne: true,
		Resources: []esv1.ReplicatedResource{
			{
				Name:     "nvidia.com/gpu",
				Replicas: 4,
			},
		},
	}
	out := &dpv1.Config{}

	convertSpecToDevicePluginConfig(in, out)
	fmt.Printf("%+v", out)
}

func TestTimeSlicingNotUpdateLabels(t *testing.T) {
	instance := &esv1.GpuNodePolicy{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-ns",
		},
		Spec: esv1.GpuNodePolicySpec{
			Mode: esv1.TimeSlicing,
		},
	}

	labels := map[string]string{
		gpuWorkloadConfigLabelKey:      gpuWorkloadConfigContainer,
		ModeDriverVersionLabel:         "R535",
		ModeManagedByLabel:             instance.GetFullName(),
		DevicePluginDefaultConfigLabel: instance.GetName(),
		MigConfigLabel:                 MigConfigLabelValueAllDisabled,
	}

	modeCfg := newGpuModeConfiguration(instance, ctrl.Log)

	modified := modeCfg.updateGPUStateLabels("node-1", labels)

	fmt.Println(modified)

}

func TestMigNotUpdateLabels(t *testing.T) {
	instance := &esv1.GpuNodePolicy{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-ns",
		},
		Spec: esv1.GpuNodePolicySpec{
			Mode: esv1.MIG,
		},
	}

	labels := map[string]string{
		gpuWorkloadConfigLabelKey:      gpuWorkloadConfigContainer,
		ModeDriverVersionLabel:         "R535",
		ModeManagedByLabel:             instance.GetFullName(),
		DevicePluginDefaultConfigLabel: instance.GetName(),
		MigConfigLabel:                 instance.GetName(),
	}

	modeCfg := newGpuModeConfiguration(instance, ctrl.Log)

	modified := modeCfg.updateGPUStateLabels("node-1", labels)

	fmt.Println(modified)

}

func TestVcudaNotUpdateLabels(t *testing.T) {
	instance := &esv1.GpuNodePolicy{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-ns",
		},
		Spec: esv1.GpuNodePolicySpec{
			Mode: esv1.VCUDA,
		},
	}

	labels := map[string]string{
		gpuWorkloadConfigLabelKey:      gpuWorkloadConfigVcuda,
		ModeDriverVersionLabel:         "R470",
		ModeManagedByLabel:             instance.GetFullName(),
		DevicePluginDefaultConfigLabel: DevicePluginConfigLabelValueDefault,
		MigConfigLabel:                 MigConfigLabelValueAllDisabled,
	}

	modeCfg := newGpuModeConfiguration(instance, ctrl.Log)

	modified := modeCfg.updateGPUStateLabels("node-1", labels)

	fmt.Println(modified)

}

func TestUpdateStatus(t *testing.T) {
	instance := &esv1.GpuNodePolicy{
		ObjectMeta: v1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-ns",
			Annotations: map[string]string{
				DevicePluginConfigAnnotation: "true",
			},
		},

		Spec: esv1.GpuNodePolicySpec{
			Mode: esv1.TimeSlicing,
		},
	}

	nodes := []corev1.Node{
		{
			ObjectMeta: v1.ObjectMeta{
				Name: "node-1",
			},
		},
	}

	InitGpuNodePolicyStatus(instance, nodes)

	fmt.Println(instance)

}
