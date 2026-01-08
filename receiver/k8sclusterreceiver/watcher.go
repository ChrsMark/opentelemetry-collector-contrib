// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package k8sclusterreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver"

import (
	"context"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	quotaclientset "github.com/openshift/client-go/quota/clientset/versioned"
	quotainformersv1 "github.com/openshift/client-go/quota/informers/externalversions"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/k8sconfig"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/experimentalmetricmetadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/cronjob"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/daemonset"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/deployment"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/gvk"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/hpa"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/jobs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/namespace"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/node"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/pod"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/replicaset"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/replicationcontroller"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/statefulset"
)

type sharedInformer interface {
	Start(<-chan struct{})
	WaitForCacheSync(<-chan struct{}) map[reflect.Type]bool
}

// informerGroup aggregates multiple informers and exposes a factory-like interface used by this receiver.
type informerGroup struct {
	informers []cache.SharedIndexInformer
}

func (g *informerGroup) Start(stop <-chan struct{}) {
	for _, inf := range g.informers {
		go inf.Run(stop)
	}
}

func (g *informerGroup) WaitForCacheSync(stop <-chan struct{}) map[reflect.Type]bool {
	syncFns := make([]cache.InformerSynced, 0, len(g.informers))
	for _, inf := range g.informers {
		syncFns = append(syncFns, inf.HasSynced)
	}
	_ = cache.WaitForCacheSync(stop, syncFns...)
	// We do not rely on the returned map anywhere; provide an empty one.
	return map[reflect.Type]bool{}
}

type resourceWatcher struct {
	client              kubernetes.Interface
	osQuotaClient       quotaclientset.Interface
	informerFactories   []sharedInformer
	metadataStore       *metadata.Store
	logger              *zap.Logger
	metadataConsumers   []metadataConsumer
	initialTimeout      time.Duration
	initialSyncDone     *atomic.Bool
	initialSyncTimedOut *atomic.Bool
	config              *Config
	entityLogConsumer   consumer.Logs

	// For mocking.
	makeClient               func(apiConf k8sconfig.APIConfig) (kubernetes.Interface, error)
	makeOpenShiftQuotaClient func(apiConf k8sconfig.APIConfig) (quotaclientset.Interface, error)
}

type metadataConsumer func(metadata []*experimentalmetricmetadata.MetadataUpdate) error

// newResourceWatcher creates a Kubernetes resource watcher.
func newResourceWatcher(set receiver.Settings, cfg *Config, metadataStore *metadata.Store) *resourceWatcher {
	return &resourceWatcher{
		logger:                   set.Logger,
		metadataStore:            metadataStore,
		initialSyncDone:          &atomic.Bool{},
		initialSyncTimedOut:      &atomic.Bool{},
		initialTimeout:           defaultInitialSyncTimeout,
		config:                   cfg,
		makeClient:               k8sconfig.MakeClient,
		makeOpenShiftQuotaClient: k8sconfig.MakeOpenShiftQuotaClient,
	}
}

func (rw *resourceWatcher) initialize() error {
	client, err := rw.makeClient(rw.config.APIConfig)
	if err != nil {
		return fmt.Errorf("Failed to create Kubernetes client: %w", err)
	}
	rw.client = client

	if rw.config.Distribution == distributionOpenShift && rw.config.Namespace == "" && len(rw.config.Namespaces) == 0 {
		rw.osQuotaClient, err = rw.makeOpenShiftQuotaClient(rw.config.APIConfig)
		if err != nil {
			return fmt.Errorf("Failed to create OpenShift quota API client: %w", err)
		}
	}

	err = rw.prepareSharedInformerFactory()
	if err != nil {
		return err
	}

	return nil
}

func (rw *resourceWatcher) prepareSharedInformerFactory() error {
	namespaces := rw.getInformerNamespaces()
	group := &informerGroup{}

	// Map of supported group version kinds by name of a kind.
	// If none of the group versions are supported by k8s server for a specific kind,
	// informer for that kind won't be set and a warning message is thrown.
	// This map should be kept in sync with what can be provided by the supported k8s server versions.
	supportedKinds := map[string][]schema.GroupVersionKind{
		"Pod":                     {gvk.Pod},
		"Node":                    {gvk.Node},
		"Namespace":               {gvk.Namespace},
		"ReplicationController":   {gvk.ReplicationController},
		"ResourceQuota":           {gvk.ResourceQuota},
		"Service":                 {gvk.Service},
		"DaemonSet":               {gvk.DaemonSet},
		"Deployment":              {gvk.Deployment},
		"ReplicaSet":              {gvk.ReplicaSet},
		"StatefulSet":             {gvk.StatefulSet},
		"Job":                     {gvk.Job},
		"CronJob":                 {gvk.CronJob},
		"HorizontalPodAutoscaler": {gvk.HorizontalPodAutoscaler},
	}

	for kind, gvks := range supportedKinds {
		anySupported := false
		for _, gvk := range gvks {
			supported, err := rw.isKindSupported(gvk)
			if err != nil {
				return err
			}
			if supported {
				anySupported = true
				rw.setupInformerForKind(gvk, namespaces, group)
			}
		}
		if !anySupported {
			rw.logger.Warn("Server doesn't support any of the group versions defined for the kind",
				zap.String("kind", kind))
		}
	}

	if rw.osQuotaClient != nil {
		quotaFactory := quotainformersv1.NewSharedInformerFactory(rw.osQuotaClient, 0)
		rw.setupInformer(gvk.ClusterResourceQuota, metadata.ClusterWideInformerKey, quotaFactory.Quota().V1().ClusterResourceQuotas().Informer())
		rw.informerFactories = append(rw.informerFactories, quotaFactory)
	}
	// Register the constructed group of informers.
	rw.informerFactories = append(rw.informerFactories, group)

	return nil
}

// getInformerNamespaces determines which namespaces should be observed.
// It returns a slice of namespaces; an empty string represents cluster-wide.
func (rw *resourceWatcher) getInformerNamespaces() []string {
	switch {
	case len(rw.config.Namespaces) > 0:
		rw.logger.Info("Namespaces filter has been enabled. Nodes and namespaces will not be observed.", zap.String("namespaces", strings.Join(rw.config.Namespaces, ",")))
		return append([]string(nil), rw.config.Namespaces...)
	case rw.config.Namespace != "":
		rw.logger.Info("Namespace filter has been enabled. Nodes and namespaces will not be observed.", zap.String("namespace", rw.config.Namespace))
		return []string{rw.config.Namespace}
	default:
		// if no namespace is provided, observe the whole cluster
		return []string{metadata.ClusterWideInformerKey}
	}
}

func (rw *resourceWatcher) isKindSupported(gvk schema.GroupVersionKind) (bool, error) {
	resources, err := rw.client.Discovery().ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		if apierrors.IsNotFound(err) { // if the discovery endpoint isn't present, assume group version is not supported
			rw.logger.Debug("Group version is not supported", zap.String("group", gvk.GroupVersion().String()))
			return false, nil
		}
		return false, fmt.Errorf("failed to fetch group version details: %w", err)
	}

	for i := range resources.APIResources {
		r := &resources.APIResources[i]
		if r.Kind == gvk.Kind {
			return true, nil
		}
	}
	return false, nil
}

type sharding struct {
	shard       uint64
	totalShards uint64
}

func (s *sharding) keep(o metav1.Object) bool {
	h := xxhash.New()
	h.Write([]byte(o.GetUID()))
	ret := (h.Sum64() % s.totalShards) == s.shard
	if ret {
		fmt.Println("The result of FILTER is KEEP for object with UID ", o.GetUID())
	} else {
		fmt.Println("The result of FILTER is SKIP for obejct with UID ", o.GetUID())
	}
	return ret
}

// newShardedInformer builds a SharedIndexInformer with a ListWatch that filters objects by the sharding rule.
func newShardedInformer(
	s sharding,
	objType runtime.Object,
	listWithCtx func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error),
	watchWithCtx func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error),
	resyncPeriod time.Duration,
	indexers cache.Indexers,
) cache.SharedIndexInformer {
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return listWithCtx(context.Background(), options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			w, err := watchWithCtx(context.Background(), options)
			if err != nil {
				return nil, err
			}
			return watch.Filter(w, func(in watch.Event) (out watch.Event, keep bool) {
				a, err := meta.Accessor(in.Object)
				if err != nil {
					return in, true
				}
				return in, s.keep(a)
			}), nil
		},
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			obj, err := listWithCtx(ctx, options)
			if err != nil {
				return nil, err
			}
			items, err := meta.ExtractList(obj)
			if err != nil {
				// If extraction fails, do not drop the list.
				return obj, nil
			}
			kept := make([]runtime.Object, 0, len(items))
			for _, it := range items {
				a, err := meta.Accessor(it)
				if err != nil {
					kept = append(kept, it)
					continue
				}
				if s.keep(a) {
					kept = append(kept, it)
				}
			}
			_ = meta.SetList(obj, kept)
			return obj, nil
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			w, err := watchWithCtx(ctx, options)
			if err != nil {
				return nil, err
			}
			return watch.Filter(w, func(in watch.Event) (out watch.Event, keep bool) {
				a, err := meta.Accessor(in.Object)
				if err != nil {
					return in, true
				}
				return in, s.keep(a)
			}), nil
		},
	}
	return cache.NewSharedIndexInformer(
		lw,
		objType,
		resyncPeriod,
		indexers,
	)
}

// setupInformerForKind creates the informers for the given GVKs, based on the provided informer factories.
// Namespaces represents the namespaces to observe; for cluster-wide, metadata.ClusterWideInformerKey is used.
func (rw *resourceWatcher) setupInformerForKind(kind schema.GroupVersionKind, namespaces []string, group *informerGroup) {
	switch kind {
	case gvk.Pod:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&corev1.Pod{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.CoreV1().Pods(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.CoreV1().Pods(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.Node:
		// Nodes are cluster-scoped; only build when cluster-wide is selected.
		if len(rw.config.Namespaces) == 0 && rw.config.Namespace == "" {
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&corev1.Node{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.CoreV1().Nodes().List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.CoreV1().Nodes().Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, metadata.ClusterWideInformerKey, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.Namespace:
		if len(rw.config.Namespaces) == 0 && rw.config.Namespace == "" {
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&corev1.Namespace{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.CoreV1().Namespaces().List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.CoreV1().Namespaces().Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, metadata.ClusterWideInformerKey, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.ReplicationController:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&corev1.ReplicationController{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.CoreV1().ReplicationControllers(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.CoreV1().ReplicationControllers(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.ResourceQuota:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&corev1.ResourceQuota{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.CoreV1().ResourceQuotas(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.CoreV1().ResourceQuotas(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.Service:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&corev1.Service{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.CoreV1().Services(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.CoreV1().Services(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.DaemonSet:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&appsv1.DaemonSet{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.AppsV1().DaemonSets(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.AppsV1().DaemonSets(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.Deployment:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&appsv1.Deployment{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.AppsV1().Deployments(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.AppsV1().Deployments(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.ReplicaSet:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&appsv1.ReplicaSet{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.AppsV1().ReplicaSets(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.AppsV1().ReplicaSets(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.StatefulSet:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&appsv1.StatefulSet{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.AppsV1().StatefulSets(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.AppsV1().StatefulSets(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.Job:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&batchv1.Job{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.BatchV1().Jobs(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.BatchV1().Jobs(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.CronJob:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&batchv1.CronJob{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.BatchV1().CronJobs(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.BatchV1().CronJobs(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	case gvk.HorizontalPodAutoscaler:
		for _, ns := range namespaces {
			namespace := ns
			if namespace == metadata.ClusterWideInformerKey {
				namespace = ""
			}
			inf := newShardedInformer(
				sharding{shard: rw.config.Sharding.ShardInstanceID, totalShards: rw.config.Sharding.TotalShards},
				&autoscalingv2.HorizontalPodAutoscaler{},
				func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					return rw.client.AutoscalingV2().HorizontalPodAutoscalers(namespace).List(ctx, options)
				},
				func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					return rw.client.AutoscalingV2().HorizontalPodAutoscalers(namespace).Watch(ctx, options)
				},
				rw.config.MetadataCollectionInterval,
				cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			)
			rw.setupInformer(kind, ns, inf)
			group.informers = append(group.informers, inf)
		}
	default:
		rw.logger.Error("Could not setup an informer for provided group version kind",
			zap.String("group version kind", kind.String()))
	}
}

// startWatchingResources starts up all informers.
func (rw *resourceWatcher) startWatchingResources(ctx context.Context, inf sharedInformer) context.Context {
	var cancel context.CancelFunc
	timedContextForInitialSync, cancel := context.WithTimeout(ctx, rw.initialTimeout)

	// Start off individual informers in the factory.
	inf.Start(ctx.Done())

	// Ensure cache is synced with initial state, once informers are started up.
	// Note that the event handler can start receiving events as soon as the informers
	// are started. So it's required to ensure that the receiver does not start
	// collecting data before the cache sync since all data may not be available.
	// This method will block either till the timeout set on the context, until
	// the initial sync is complete or the parent context is cancelled.
	inf.WaitForCacheSync(timedContextForInitialSync.Done())
	defer cancel()
	return timedContextForInitialSync
}

// setupInformer adds event handlers to informers and setups a metadataStore.
func (rw *resourceWatcher) setupInformer(gvk schema.GroupVersionKind, namespace string, informer cache.SharedIndexInformer) {
	err := informer.SetTransform(transformObject)
	if err != nil {
		rw.logger.Error("error setting informer transform function", zap.Error(err))
	}
	_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    rw.onAdd,
		UpdateFunc: rw.onUpdate,
		DeleteFunc: rw.onDelete,
	})
	if err != nil {
		rw.logger.Error("error adding event handler to informer", zap.Error(err))
	}
	rw.metadataStore.Setup(gvk, namespace, informer.GetStore())
}

func (rw *resourceWatcher) onAdd(obj any) {
	rw.waitForInitialInformerSync()

	// Sync metadata only if there's at least one destination for it to sent.
	if !rw.hasDestination() {
		return
	}

	rw.syncMetadataUpdate(map[experimentalmetricmetadata.ResourceID]*metadata.KubernetesMetadata{}, rw.objMetadata(obj))
}

func (rw *resourceWatcher) hasDestination() bool {
	return len(rw.metadataConsumers) != 0 || rw.entityLogConsumer != nil
}

func (rw *resourceWatcher) onUpdate(oldObj, newObj any) {
	rw.waitForInitialInformerSync()

	// Sync metadata only if there's at least one destination for it to sent.
	if !rw.hasDestination() {
		return
	}

	rw.syncMetadataUpdate(rw.objMetadata(oldObj), rw.objMetadata(newObj))
}

func (rw *resourceWatcher) onDelete(oldObj any) {
	rw.waitForInitialInformerSync()

	// Sync metadata only if there's at least one destination for it to sent.
	if !rw.hasDestination() {
		return
	}

	rw.syncMetadataUpdate(rw.objMetadata(oldObj), map[experimentalmetricmetadata.ResourceID]*metadata.KubernetesMetadata{})
}

// objMetadata returns the metadata for the given object.
func (rw *resourceWatcher) objMetadata(obj any) map[experimentalmetricmetadata.ResourceID]*metadata.KubernetesMetadata {
	switch o := obj.(type) {
	case *corev1.Pod:
		return pod.GetMetadata(o, rw.metadataStore, rw.logger)
	case *corev1.Node:
		return node.GetMetadata(o)
	case *corev1.ReplicationController:
		return replicationcontroller.GetMetadata(o)
	case *appsv1.Deployment:
		return deployment.GetMetadata(o)
	case *appsv1.ReplicaSet:
		return replicaset.GetMetadata(o)
	case *appsv1.DaemonSet:
		return daemonset.GetMetadata(o)
	case *appsv1.StatefulSet:
		return statefulset.GetMetadata(o)
	case *batchv1.Job:
		return jobs.GetMetadata(o)
	case *batchv1.CronJob:
		return cronjob.GetMetadata(o)
	case *autoscalingv2.HorizontalPodAutoscaler:
		return hpa.GetMetadata(o)
	case *corev1.Namespace:
		return namespace.GetMetadata(o)
	}
	return nil
}

func (rw *resourceWatcher) waitForInitialInformerSync() {
	if rw.initialSyncDone.Load() || rw.initialSyncTimedOut.Load() {
		return
	}

	// Wait till initial sync is complete or timeout.
	for !rw.initialSyncDone.Load() {
		if rw.initialSyncTimedOut.Load() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rw *resourceWatcher) setupMetadataExporters(
	exporters map[component.ID]component.Component,
	metadataExportersFromConfig []string,
) error {
	var out []metadataConsumer

	metadataExportersSet := stringSliceToMap(metadataExportersFromConfig)
	if err := validateMetadataExporters(metadataExportersSet, exporters); err != nil {
		return fmt.Errorf("failed to configure metadata_exporters: %w", err)
	}

	for cfg, exp := range exporters {
		if !metadataExportersSet[cfg.String()] {
			continue
		}
		kme, ok := exp.(experimentalmetricmetadata.MetadataExporter)
		if !ok {
			return fmt.Errorf("%s exporter does not implement MetadataExporter", cfg.Name())
		}
		out = append(out, kme.ConsumeMetadata)
		rw.logger.Info("Configured Kubernetes MetadataExporter",
			zap.String("exporter_name", cfg.String()),
		)
	}

	rw.metadataConsumers = out
	return nil
}

func validateMetadataExporters(metadataExporters map[string]bool, exporters map[component.ID]component.Component) error {
	configuredExporters := map[string]bool{}
	for cfg := range exporters {
		configuredExporters[cfg.String()] = true
	}

	for e := range metadataExporters {
		if !configuredExporters[e] {
			return fmt.Errorf("%s exporter is not in collector config", e)
		}
	}

	return nil
}

func (rw *resourceWatcher) syncMetadataUpdate(oldMetadata, newMetadata map[experimentalmetricmetadata.ResourceID]*metadata.KubernetesMetadata) {
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	metadataUpdate := metadata.GetMetadataUpdate(oldMetadata, newMetadata)
	if len(metadataUpdate) != 0 {
		for _, consume := range rw.metadataConsumers {
			_ = consume(metadataUpdate)
		}
	}

	if rw.entityLogConsumer != nil {
		// Represent metadata update as entity events.
		entityEvents := metadata.GetEntityEvents(oldMetadata, newMetadata, timestamp, rw.config.MetadataCollectionInterval)

		// Convert entity events to log representation.
		logs := entityEvents.ConvertAndMoveToLogs()

		if logs.LogRecordCount() != 0 {
			err := rw.entityLogConsumer.ConsumeLogs(context.Background(), logs)
			if err != nil {
				rw.logger.Error("Error sending entity events to the consumer", zap.Error(err))

				// Note: receiver contract says that we need to retry sending if the
				// returned error is not Permanent. However, we are not doing it here.
				// Instead, we rely on the fact the metadata is collected periodically
				// and the entity events will be delivered on the next cycle. This is
				// fine because we deliver cumulative entity state.
				// This allows us to avoid stressing the Collector or its destination
				// unnecessarily (typically non-Permanent errors happen in stressed conditions).
				// The periodic collection will be implemented later, see
				// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/24413
			}
		}
	}
}

// stringSliceToMap converts a slice of strings into a map with keys from the slice
func stringSliceToMap(strings []string) map[string]bool {
	ret := map[string]bool{}
	for _, s := range strings {
		ret[s] = true
	}
	return ret
}
