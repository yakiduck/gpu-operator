package v1

import (
	"time"
)

// Copy from github.com/NVIDIA/k8s-device-plugin@v0.13.0:api/config/v1/config.go

// Version indicates the version of the 'Config' struct used to hold configuration information.
const Version = "v1"

// Constants representing the various MIG strategies
const (
	MigStrategyNone   = "none"
	MigStrategySingle = "single"
	MigStrategyMixed  = "mixed"
)

// Config is a versioned struct used to hold configuration information.
type Config struct {
	Version string   `json:"version"             yaml:"version"`
	Flags   Flags    `json:"flags,omitempty"     yaml:"flags,omitempty"`
	Sharing *Sharing `json:"sharing,omitempty"   yaml:"sharing,omitempty"`
}

// Flags holds the full list of flags used to configure the device plugin and GFD.
type Flags struct {
	CommandLineFlags
}

// CommandLineFlags holds the list of command line flags used to configure the device plugin and GFD.
type CommandLineFlags struct {
	MigStrategy      *string                 `json:"migStrategy,omitempty"      yaml:"migStrategy,omitempty"`
	FailOnInitError  *bool                   `json:"failOnInitError,omitempty"  yaml:"failOnInitError,omitempty"`
	NvidiaDriverRoot *string                 `json:"nvidiaDriverRoot,omitempty" yaml:"nvidiaDriverRoot,omitempty"`
	GDSEnabled       *bool                   `json:"gdsEnabled,omitempty"       yaml:"gdsEnabled,omitempty"`
	MOFEDEnabled     *bool                   `json:"mofedEnabled,omitempty"     yaml:"mofedEnabled,omitempty"`
	Plugin           *PluginCommandLineFlags `json:"plugin,omitempty"           yaml:"plugin,omitempty"`
	GFD              *GFDCommandLineFlags    `json:"gfd,omitempty"              yaml:"gfd,omitempty"`
}

// PluginCommandLineFlags holds the list of command line flags specific to the device plugin.
type PluginCommandLineFlags struct {
	PassDeviceSpecs    *bool   `json:"passDeviceSpecs"    yaml:"passDeviceSpecs"`
	DeviceListStrategy *string `json:"deviceListStrategy" yaml:"deviceListStrategy"`
	DeviceIDStrategy   *string `json:"deviceIDStrategy"   yaml:"deviceIDStrategy"`
}

// GFDCommandLineFlags holds the list of command line flags specific to GFD.
type GFDCommandLineFlags struct {
	Oneshot         *bool     `json:"oneshot"         yaml:"oneshot"`
	NoTimestamp     *bool     `json:"noTimestamp"     yaml:"noTimestamp"`
	SleepInterval   *Duration `json:"sleepInterval"   yaml:"sleepInterval"`
	OutputFile      *string   `json:"outputFile"      yaml:"outputFile"`
	MachineTypeFile *string   `json:"machineTypeFile" yaml:"machineTypeFile"`
}

type Duration time.Duration

// ResourceName represents a valid resource name in Kubernetes
type ResourceName string

// Sharing encapsulates the set of sharing strategies that are supported.
type Sharing struct {
	TimeSlicing TimeSlicing `json:"timeSlicing,omitempty" yaml:"timeSlicing,omitempty"`
}

// TimeSlicing defines the set of replicas to be made for timeSlicing available resources.
type TimeSlicing struct {
	RenameByDefault            bool                 `json:"renameByDefault,omitempty"            yaml:"renameByDefault,omitempty"`
	FailRequestsGreaterThanOne bool                 `json:"failRequestsGreaterThanOne,omitempty" yaml:"failRequestsGreaterThanOne,omitempty"`
	Resources                  []ReplicatedResource `json:"resources,omitempty"                  yaml:"resources,omitempty"`
}

// ReplicatedResource represents a resource to be replicated.
type ReplicatedResource struct {
	Name     ResourceName `json:"name,omitempty"     yaml:"name,omitempty"`
	Rename   ResourceName `json:"rename,omitempty"   yaml:"rename,omitempty"`
	Replicas int          `json:"replicas,omitempty" yaml:"replicas,omitempty"`
}

// ReplicatedDeviceRef can either be a full GPU index, a MIG index, or a UUID (full GPU or MIG)
type ReplicatedDeviceRef string

//Copy from github.com/NVIDIA/mig-parted@v0.5.0:api/spec/v1/spec.go

// Spec is a versioned struct used to hold information on 'MigConfigs'.
type Spec struct {
	Version    string                        `json:"version"               yaml:"version"`
	MigConfigs map[string]MigConfigSpecSlice `json:"mig-configs,omitempty" yaml:"mig-configs,omitempty"`
}

// MigConfigSpec defines the spec to declare the desired MIG configuration for a set of GPUs.
type MigConfigSpec struct {
	DeviceFilter interface{} `json:"device-filter,omitempty" yaml:"device-filter,flow,omitempty"`
	Devices      interface{} `json:"devices"                 yaml:"devices,flow"`
	MigEnabled   bool        `json:"mig-enabled"             yaml:"mig-enabled"`
	MigDevices   MigConfig   `json:"mig-devices,omitempty"   yaml:"mig-devices,omitempty"`
}

// MigConfigSpecSlice represents a slice of 'MigConfigSpec'.
type MigConfigSpecSlice []MigConfigSpec

// MigConfig holds a map of strings representing a MigProfile to a count of that profile type.
// It is meant to represent the set of MIG profiles (and how many of a
// particular type) should be instantiated on a GPU.
type MigConfig map[string]int
