package main

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appinformers "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedv1core "k8s.io/client-go/kubernetes/typed/core/v1"
	applisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type Controller struct {
	kubeclientset    kubernetes.Interface
	deploymentLister applisters.DeploymentLister
	deploymentSynced cache.InformerSynced
	recorder         record.EventRecorder
	workqueue        workqueue.RateLimitingInterface
}

func NewController(ctx context.Context, kubeclientset kubernetes.Interface, deploymentInformer appinformers.DeploymentInformer) *Controller {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedv1core.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "tutorial-controller"})

	controller := &Controller{
		kubeclientset:    kubeclientset,
		deploymentLister: deploymentInformer.Lister(),
		deploymentSynced: deploymentInformer.Informer().HasSynced,
		recorder:         recorder,
		workqueue:        workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "tutorial-controller"),
	}

	deploymentInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(new interface{}) {
			controller.enqueue(new)
		},
		UpdateFunc: func(old, new interface{}) {
			newDeployment := new.(*appsv1.Deployment)
			oldDeployment := old.(*appsv1.Deployment)
			if newDeployment.ResourceVersion == oldDeployment.ResourceVersion {
				return
			}
			controller.enqueue(new)
		},
	})

	return controller
}

func (c *Controller) enqueue(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func (c *Controller) Run(ctx context.Context, numWorkers int) error {
	defer c.workqueue.ShutDown()
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting tutorial-controller")
	defer logger.Info("Shutting down tutorial-controller")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.deploymentSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "numWorkers", numWorkers)
	for i := 0; i < numWorkers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	<-ctx.Done()
	return ctx.Err()
}

func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}

	logger := klog.FromContext(ctx)
	defer c.workqueue.Done(obj)

	key, ok := obj.(string)
	if !ok || key == "" {
		c.workqueue.Forget(obj)
		utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
		return true
	}

	logger = klog.LoggerWithValues(logger, "deployment", key)
	if err := c.syncHandler(klog.NewContext(ctx, logger), key); err != nil {
		logger.Error(err, "Error syncing")
		c.workqueue.AddRateLimited(key)
		utilruntime.HandleError(err)
		return true
	}

	c.workqueue.Forget(obj)
	logger.Info("Successfully synced")
	return true
}

func (c *Controller) syncHandler(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	deploy, err := c.deploymentLister.Deployments(ns).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.V(4).Info("Deployment not found", "deployment", key)
			return nil
		}
		return fmt.Errorf("failed to list deployment %s", err)
	}

	if deploy.Annotations == nil {
		val, found := deploy.Annotations["tutorial-controller"]
		if found && val == "True" {
			logger.V(4).Info("tutorial-controller annotation already set", "deployment", key)
			return nil
		}
	}

	deploy2 := deploy.DeepCopy()
	if deploy2.Annotations == nil {
		deploy2.Annotations = make(map[string]string)
	}
	deploy2.Annotations["tutorial-controller"] = "True"
	_, err = c.kubeclientset.AppsV1().Deployments(deploy2.Namespace).Update(ctx, deploy2, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	logger.V(4).Info("tutorial-controller annotation added", "deployment", key)
	c.recorder.Event(deploy2, corev1.EventTypeNormal, "Synced", "tutorial-controller annotation added")
	return nil
}
