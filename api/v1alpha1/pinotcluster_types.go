package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ----- Zookeeper -----

type PinotZookeeperExternal struct {
	// e.g. "zookeeper:2181/pinot-cluster"
	ConnectionString string `json:"connectionString"`
}

type PinotZookeeperSpec struct {
	External *PinotZookeeperExternal `json:"external,omitempty"`
}

// ----- Common resources / pod config -----

type PinotComponentResources struct {
	corev1.ResourceRequirements `json:",inline"`
}

type PinotServerStorageSpec struct {
	Size             string `json:"size"`
	StorageClassName string `json:"storageClassName,omitempty"`
}

type PinotControllerSpec struct {
	// +kubebuilder:validation:Minimum=1
	Replicas  int32                   `json:"replicas"`
	Resources PinotComponentResources `json:"resources,omitempty"`
}

type PinotBrokerSpec struct {
	// +kubebuilder:validation:Minimum=1
	Replicas  int32                   `json:"replicas"`
	Resources PinotComponentResources `json:"resources,omitempty"`
}

type PinotServerSpec struct {
	// +kubebuilder:validation:Minimum=1
	Replicas  int32                   `json:"replicas"`
	Resources PinotComponentResources `json:"resources,omitempty"`
	Storage   PinotServerStorageSpec  `json:"storage"`
}

type PinotMinionSpec struct {
	Enabled bool `json:"enabled"`
	// +kubebuilder:validation:Minimum=0
	Replicas  int32                   `json:"replicas,omitempty"`
	Resources PinotComponentResources `json:"resources,omitempty"`
}

type PinotPodSettings struct {
	NodeSelector map[string]string   `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
	Affinity     *corev1.Affinity    `json:"affinity,omitempty"`
}

// PinotClusterSpec defines the desired state of PinotCluster
type PinotClusterSpec struct {
	ClusterName string `json:"clusterName"`

	Image           string            `json:"image"`
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	Zookeeper PinotZookeeperSpec `json:"zookeeper"`

	Controller PinotControllerSpec `json:"controller"`
	Broker     PinotBrokerSpec     `json:"broker"`
	Server     PinotServerSpec     `json:"server"`
	Minion     PinotMinionSpec     `json:"minion,omitempty"`

	Pod PinotPodSettings `json:"pod,omitempty"`
}

// PinotClusterStatus defines the observed state of PinotCluster
type PinotClusterStatus struct {
	Phase                   string             `json:"phase,omitempty"`
	ControllerReadyReplicas int32              `json:"controllerReadyReplicas,omitempty"`
	BrokerReadyReplicas     int32              `json:"brokerReadyReplicas,omitempty"`
	ServerReadyReplicas     int32              `json:"serverReadyReplicas,omitempty"`
	MinionReadyReplicas     int32              `json:"minionReadyReplicas,omitempty"`
	Conditions              []metav1.Condition `json:"conditions,omitempty"`
	ObservedGeneration      int64              `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// PinotCluster is the Schema for the pinotclusters API
type PinotCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PinotClusterSpec   `json:"spec,omitempty"`
	Status PinotClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PinotClusterList contains a list of PinotCluster
type PinotClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PinotCluster `json:"items"`
}
