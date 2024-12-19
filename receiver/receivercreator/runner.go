// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package receivercreator // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/receivercreator"

import (
	"context"
	"fmt"
	"sync"

	"github.com/spf13/cast"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pipeline"
	rcvr "go.opentelemetry.io/collector/receiver"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// runner starts and stops receiver instances.
type runner interface {
	// start a metrics receiver instance from its static config and discovered config.
	start(receiver receiverConfig, discoveredConfig userConfigMap) (*component.ID, error)
	// shutdown a receiver.
	shutdown(rcvrID component.ID) error
}

// receiverRunner handles starting/stopping of a concrete subreceiver instance.
type receiverRunner struct {
	logger      *zap.Logger
	params      rcvr.Settings
	idNamespace component.ID
	host        host
	receivers   map[string]*wrappedReceiver
	lock        *sync.Mutex
}

func newReceiverRunner(params rcvr.Settings, host host) *receiverRunner {
	return &receiverRunner{
		logger:      params.Logger,
		params:      params,
		idNamespace: params.ID,
		host:        host,
		receivers:   map[string]*wrappedReceiver{},
		lock:        &sync.Mutex{},
	}
}

var _ runner = (*receiverRunner)(nil)

func (run *receiverRunner) start(
	receiver receiverConfig,
	discoveredConfig userConfigMap,
) (*component.ID, error) {
	factory := run.host.GetFactory(component.KindReceiver, receiver.id.Type())

	if factory == nil {
		return nil, fmt.Errorf("unable to lookup factory for receiver %q", receiver.id.String())
	}

	receiverFactory := factory.(rcvr.Factory)

	cfg, targetEndpoint, err := run.loadRuntimeReceiverConfig(receiverFactory, receiver, discoveredConfig)
	if err != nil {
		return nil, err
	}

	// Sets dynamically created receiver to something like receiver_creator/1/redis{endpoint="localhost:6380"}/<EndpointID>.
	componentID := component.NewIDWithName(factory.Type(), fmt.Sprintf("%s/%s{endpoint=%q}/%s", receiver.id.Name(), run.idNamespace, targetEndpoint, receiver.endpointID))

	err = run.host.AddComponent(pipeline.NewID(pipeline.SignalMetrics), component.KindReceiver, componentID, cfg)
	if err != nil {
		run.logger.Error(fmt.Sprintf("Xcreator error on adding receiver: %s", err.Error()))
		return nil, fmt.Errorf(fmt.Sprintf("Xcreator error on adding receiver: %s", err.Error()))
	}

	return &componentID, nil
}

// shutdown the given receiver.
func (run *receiverRunner) shutdown(rcvrID component.ID) error {
	return run.host.RemoveComponent(component.KindReceiver, rcvrID)
}

// loadRuntimeReceiverConfig loads the given receiverTemplate merged with config values
// that may have been discovered at runtime.
func (run *receiverRunner) loadRuntimeReceiverConfig(
	factory rcvr.Factory,
	receiver receiverConfig,
	discoveredConfig userConfigMap,
) (component.Config, string, error) {
	// remove dynamically added "endpoint" field if not supported by receiver
	mergedConfig, targetEndpoint, err := mergeTemplatedAndDiscoveredConfigs(factory, receiver.config, discoveredConfig)
	if err != nil {
		return nil, targetEndpoint, fmt.Errorf("failed to merge constituent template configs: %w", err)
	}

	receiverCfg := factory.CreateDefaultConfig()
	if err := mergedConfig.Unmarshal(receiverCfg); err != nil {
		return nil, "", fmt.Errorf("failed to load %q template config: %w", receiver.id.String(), err)
	}
	if err := component.ValidateConfig(receiverCfg); err != nil {
		return nil, "", fmt.Errorf("invalid runtime receiver config: receivers::%s: %w", receiver.id, err)
	}
	return receiverCfg, targetEndpoint, nil
}

// mergeTemplateAndDiscoveredConfigs will unify the templated and discovered configs,
// setting the `endpoint` field from the discovered one if 1. not specified by the user
// and 2. determined to be supported (by trial and error of unmarshalling a temp intermediary).
func mergeTemplatedAndDiscoveredConfigs(factory rcvr.Factory, templated, discovered userConfigMap) (*confmap.Conf, string, error) {
	targetEndpoint := cast.ToString(templated[endpointConfigKey])
	if _, endpointSet := discovered[tmpSetEndpointConfigKey]; endpointSet {
		delete(discovered, tmpSetEndpointConfigKey)
		targetEndpoint = cast.ToString(discovered[endpointConfigKey])

		// confirm the endpoint we've added is supported, removing if not
		endpointConfig := confmap.NewFromStringMap(map[string]any{
			endpointConfigKey: targetEndpoint,
		})
		if err := endpointConfig.Unmarshal(factory.CreateDefaultConfig()); err != nil {
			// rather than attach to error content that can change over time,
			// confirm the error only arises w/ ErrorUnused mapstructure setting ("invalid keys")
			if err = endpointConfig.Unmarshal(factory.CreateDefaultConfig(), confmap.WithIgnoreUnused()); err == nil {
				delete(discovered, endpointConfigKey)
			}
		}
	}
	discoveredConfig := confmap.NewFromStringMap(discovered)
	templatedConfig := confmap.NewFromStringMap(templated)

	// Merge in discoveredConfig containing values discovered at runtime.
	if err := templatedConfig.Merge(discoveredConfig); err != nil {
		return nil, targetEndpoint, fmt.Errorf("failed to merge template config from discovered runtime values: %w", err)
	}
	return templatedConfig, targetEndpoint, nil
}

// createLogsRuntimeReceiver creates a receiver that is discovered at runtime.
func (run *receiverRunner) createLogsRuntimeReceiver(
	factory rcvr.Factory,
	id component.ID,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (rcvr.Logs, error) {
	runParams := run.params
	runParams.Logger = runParams.Logger.With(zap.String("name", id.String()))
	runParams.ID = id
	return factory.CreateLogs(context.Background(), runParams, cfg, nextConsumer)
}

// createMetricsRuntimeReceiver creates a receiver that is discovered at runtime.
func (run *receiverRunner) createMetricsRuntimeReceiver(
	factory rcvr.Factory,
	id component.ID,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (rcvr.Metrics, error) {
	runParams := run.params
	runParams.Logger = runParams.Logger.With(zap.String("name", id.String()))
	runParams.ID = id
	return factory.CreateMetrics(context.Background(), runParams, cfg, nextConsumer)
}

// createTracesRuntimeReceiver creates a receiver that is discovered at runtime.
func (run *receiverRunner) createTracesRuntimeReceiver(
	factory rcvr.Factory,
	id component.ID,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (rcvr.Traces, error) {
	runParams := run.params
	runParams.Logger = runParams.Logger.With(zap.String("name", id.String()))
	runParams.ID = id
	return factory.CreateTraces(context.Background(), runParams, cfg, nextConsumer)
}

var _ component.Component = (*wrappedReceiver)(nil)

type wrappedReceiver struct {
	logs    rcvr.Logs
	metrics rcvr.Metrics
	traces  rcvr.Traces
}

func (w *wrappedReceiver) Start(ctx context.Context, host component.Host) error {
	var err error
	for _, r := range []component.Component{w.logs, w.metrics, w.traces} {
		if r != nil {
			if e := r.Start(ctx, host); e != nil {
				err = multierr.Combine(err, e)
			}
		}
	}
	return err
}

func (w *wrappedReceiver) Shutdown(ctx context.Context) error {
	var err error
	for _, r := range []component.Component{w.logs, w.metrics, w.traces} {
		if r != nil {
			if e := r.Shutdown(ctx); e != nil {
				err = multierr.Combine(err, e)
			}
		}
	}
	return err
}
