package controllers

import (
	"context"
	"time"

	pinotov1alpha1 "github.com/f-the-architect/pinot-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
}

// Reconcile is the main reconciliation loop.
func (r *PinotClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch PinotCluster
	var cluster pinotov1alpha1.PinotCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if errors.IsNotFound(err) {
			// resource deleted, nothing to do (child resources have ownerRefs)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// ensure status is always updated against current generation
	cluster.Status.ObservedGeneration = cluster.GetGeneration()

	// 2. Reconcile each component
	if err := r.reconcileController(ctx, &cluster); err != nil {
		logger.Error(err, "failed to reconcile controller")
		r.setCondition(&cluster, "ReconcileError", metav1.ConditionTrue, "ControllerError", err.Error())
		_ = r.Status().Update(ctx, &cluster)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	if err := r.reconcileBroker(ctx, &cluster); err != nil {
		logger.Error(err, "failed to reconcile broker")
		r.setCondition(&cluster, "ReconcileError", metav1.ConditionTrue, "BrokerError", err.Error())
		_ = r.Status().Update(ctx, &cluster)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	if err := r.reconcileServer(ctx, &cluster); err != nil {
		logger.Error(err, "failed to reconcile server")
		r.setCondition(&cluster, "ReconcileError", metav1.ConditionTrue, "ServerError", err.Error())
		_ = r.Status().Update(ctx, &cluster)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	if cluster.Spec.Minion.Enabled {
		if err := r.reconcileMinion(ctx, &cluster); err != nil {
			logger.Error(err, "failed to reconcile minion")
			r.setCondition(&cluster, "ReconcileError", metav1.ConditionTrue, "MinionError", err.Error())
			_ = r.Status().Update(ctx, &cluster)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
	}

	// update phase & conditions
	r.updateStatusPhase(&cluster)

	if err := r.Status().Update(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	// Reconcile again periodically (or rely purely on events)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}
