package controllers

import (
	"context"
	"reflect"
	"time"

	pinotov1alpha1 "github.com/f-the-architect/pinot-operator/api/v1alpha1"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// +kubebuilder:rbac:groups=pinot.apache.org,resources=pinotclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pinot.apache.org,resources=pinotclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods;services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets;deployments,verbs=get;list;watch;create;update;patch;delete

type PinotClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type pinotRole string

const (
	roleController pinotRole = "controller"
	roleBroker     pinotRole = "broker"
	roleServer     pinotRole = "server"
	roleMinion     pinotRole = "minion"
)

// Reconcile is the main reconciliation loop.
func (r *PinotClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var cluster pinotov1alpha1.PinotCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			// deleted
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	cluster.Status.ObservedGeneration = cluster.GetGeneration()

	// Controller
	if err := r.reconcileRole(ctx, &cluster, roleController); err != nil {
		logger.Error(err, "failed to reconcile controller")
		r.setCondition(&cluster, "ReconcileError", metav1.ConditionTrue, "ControllerError", err.Error())
		_ = r.Status().Update(ctx, &cluster)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Broker
	if err := r.reconcileRole(ctx, &cluster, roleBroker); err != nil {
		logger.Error(err, "failed to reconcile broker")
		r.setCondition(&cluster, "ReconcileError", metav1.ConditionTrue, "BrokerError", err.Error())
		_ = r.Status().Update(ctx, &cluster)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Server
	if err := r.reconcileRole(ctx, &cluster, roleServer); err != nil {
		logger.Error(err, "failed to reconcile server")
		r.setCondition(&cluster, "ReconcileError", metav1.ConditionTrue, "ServerError", err.Error())
		_ = r.Status().Update(ctx, &cluster)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Minion (optional)
	if cluster.Spec.Minion.Enabled {
		if err := r.reconcileRole(ctx, &cluster, roleMinion); err != nil {
			logger.Error(err, "failed to reconcile minion")
			r.setCondition(&cluster, "ReconcileError", metav1.ConditionTrue, "MinionError", err.Error())
			_ = r.Status().Update(ctx, &cluster)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
	} else {
		cluster.Status.MinionReadyReplicas = 0
	}

	r.updateStatusPhase(&cluster)

	if err := r.Status().Update(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	// periodic reconcile
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// ----- Role reconciliation -----

func (r *PinotClusterReconciler) reconcileRole(ctx context.Context, cluster *pinotov1alpha1.PinotCluster, role pinotRole) error {
	svc := r.desiredService(cluster, role)
	if err := r.createOrUpdateService(ctx, svc); err != nil {
		return err
	}

	sts := r.desiredStatefulSet(cluster, role)
	var existing appsv1.StatefulSet
	key := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}
	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		if err := r.Create(ctx, sts); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		if !reflect.DeepEqual(existing.Spec, sts.Spec) {
			existing.Spec = sts.Spec
			if err := r.Update(ctx, &existing); err != nil {
				return err
			}
		}
	}

	// Refresh status from existing (or newly created) object
	if err := r.Get(ctx, key, &existing); err == nil {
		switch role {
		case roleController:
			cluster.Status.ControllerReadyReplicas = existing.Status.ReadyReplicas
		case roleBroker:
			cluster.Status.BrokerReadyReplicas = existing.Status.ReadyReplicas
		case roleServer:
			cluster.Status.ServerReadyReplicas = existing.Status.ReadyReplicas
		case roleMinion:
			cluster.Status.MinionReadyReplicas = existing.Status.ReadyReplicas
		}
	}

	return nil
}

// ----- Desired objects -----

func (r *PinotClusterReconciler) desiredService(cluster *pinotov1alpha1.PinotCluster, role pinotRole) *corev1.Service {
	name := cluster.Name + "-" + string(role)
	labels := map[string]string{
		"app":           "pinot",
		"pinot-role":    string(role),
		"pinot-cluster": cluster.Spec.ClusterName,
	}

	ports := []corev1.ServicePort{}
	switch role {
	case roleController:
		ports = []corev1.ServicePort{
			{Name: "http", Port: 9000},
			{Name: "admin", Port: 9001},
		}
	case roleBroker:
		ports = []corev1.ServicePort{
			{Name: "broker", Port: 8099},
		}
	case roleServer:
		ports = []corev1.ServicePort{
			{Name: "server", Port: 8098},
		}
	case roleMinion:
		ports = []corev1.ServicePort{
			{Name: "minion", Port: 9514},
		}
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports:    ports,
		},
	}

	ctrl.SetControllerReference(cluster, svc, r.Scheme)
	return svc
}

func (r *PinotClusterReconciler) desiredStatefulSet(cluster *pinotov1alpha1.PinotCluster, role pinotRole) *appsv1.StatefulSet {
	name := cluster.Name + "-" + string(role)
	labels := map[string]string{
		"app":           "pinot",
		"pinot-role":    string(role),
		"pinot-cluster": cluster.Spec.ClusterName,
	}

	var replicas int32
	var resources corev1.ResourceRequirements
	needsStorage := false
	var storageSize, storageClass string

	switch role {
	case roleController:
		replicas = cluster.Spec.Controller.Replicas
		resources = cluster.Spec.Controller.Resources.ResourceRequirements
	case roleBroker:
		replicas = cluster.Spec.Broker.Replicas
		resources = cluster.Spec.Broker.Resources.ResourceRequirements
	case roleServer:
		replicas = cluster.Spec.Server.Replicas
		resources = cluster.Spec.Server.Resources.ResourceRequirements
		needsStorage = true
		storageSize = cluster.Spec.Server.Storage.Size
		storageClass = cluster.Spec.Server.Storage.StorageClassName
	case roleMinion:
		replicas = cluster.Spec.Minion.Replicas
		resources = cluster.Spec.Minion.Resources.ResourceRequirements
	}

	// Command & ports
	cmd := []string{"bin/pinot-admin.sh"}
	switch role {
	case roleController:
		cmd = append(cmd,
			"StartController",
			"-clusterName", cluster.Spec.ClusterName,
			"-zkAddress", cluster.Spec.Zookeeper.External.ConnectionString,
			"-configFileName", "conf/controller.conf",
		)
	case roleBroker:
		cmd = append(cmd,
			"StartBroker",
			"-clusterName", cluster.Spec.ClusterName,
			"-zkAddress", cluster.Spec.Zookeeper.External.ConnectionString,
			"-configFileName", "conf/broker.conf",
		)
	case roleServer:
		cmd = append(cmd,
			"StartServer",
			"-clusterName", cluster.Spec.ClusterName,
			"-zkAddress", cluster.Spec.Zookeeper.External.ConnectionString,
			"-configFileName", "conf/server.conf",
		)
	case roleMinion:
		cmd = append(cmd,
			"StartMinion",
			"-clusterName", cluster.Spec.ClusterName,
			"-zkAddress", cluster.Spec.Zookeeper.External.ConnectionString,
			"-configFileName", "conf/minion.conf",
		)
	}

	container := corev1.Container{
		Name:            "pinot-" + string(role),
		Image:           cluster.Spec.Image,
		ImagePullPolicy: cluster.Spec.ImagePullPolicy,
		Command:         cmd,
		Resources:       resources,
	}

	var volumeClaims []corev1.PersistentVolumeClaim
	if needsStorage {
		container.VolumeMounts = []corev1.VolumeMount{
			{
				Name:      "data",
				MountPath: "/var/pinot/server/data",
			},
		}
		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "data",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(storageSize),
					},
				},
			},
		}
		if storageClass != "" {
			pvc.Spec.StorageClassName = &storageClass
		}
		volumeClaims = []corev1.PersistentVolumeClaim{pvc}
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers:   []corev1.Container{container},
					NodeSelector: cluster.Spec.Pod.NodeSelector,
					Tolerations:  cluster.Spec.Pod.Tolerations,
					Affinity:     cluster.Spec.Pod.Affinity,
				},
			},
			VolumeClaimTemplates: volumeClaims,
		},
	}

	ctrl.SetControllerReference(cluster, sts, r.Scheme)
	return sts
}

// ----- Helpers: Service upsert, conditions, phase -----

func (r *PinotClusterReconciler) createOrUpdateService(ctx context.Context, svc *corev1.Service) error {
	var existing corev1.Service
	key := types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}

	err := r.Get(ctx, key, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, svc)
	} else if err != nil {
		return err
	}

	// preserve ClusterIP
	svc.Spec.ClusterIP = existing.Spec.ClusterIP
	existing.Spec = svc.Spec
	existing.Labels = svc.Labels
	return r.Update(ctx, &existing)
}

func (r *PinotClusterReconciler) setCondition(cluster *pinotov1alpha1.PinotCluster, condType string, status metav1.ConditionStatus, reason, message string) {
	newCond := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}

	conditions := cluster.Status.Conditions
	idx := -1
	for i, c := range conditions {
		if c.Type == condType {
			idx = i
			break
		}
	}
	if idx == -1 {
		conditions = append(conditions, newCond)
	} else {
		conditions[idx] = newCond
	}
	cluster.Status.Conditions = conditions
}

func (r *PinotClusterReconciler) updateStatusPhase(cluster *pinotov1alpha1.PinotCluster) {
	ready := cluster.Status.ControllerReadyReplicas >= cluster.Spec.Controller.Replicas &&
		cluster.Status.BrokerReadyReplicas >= cluster.Spec.Broker.Replicas &&
		cluster.Status.ServerReadyReplicas >= cluster.Spec.Server.Replicas

	if cluster.Spec.Minion.Enabled {
		ready = ready && (cluster.Status.MinionReadyReplicas >= cluster.Spec.Minion.Replicas)
	}

	if ready {
		cluster.Status.Phase = "Ready"
		r.setCondition(cluster, "Ready", metav1.ConditionTrue, "ComponentsReady", "All Pinot components are ready")
	} else {
		cluster.Status.Phase = "Progressing"
		r.setCondition(cluster, "Ready", metav1.ConditionFalse, "Reconciling", "Reconciling Pinot components")
	}
}

// SetupWithManager wires the controller into the manager.
func (r *PinotClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pinotov1alpha1.PinotCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
