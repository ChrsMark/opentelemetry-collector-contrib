// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adapter // import "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/adapter"

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/experimental/storage"
	rcvr "go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/pipeline"
)

type receiver struct {
	id     component.ID
	wg     sync.WaitGroup
	cancel context.CancelFunc

	pipe      pipeline.Pipeline
	emitter   *LogEmitter
	consumer  consumer.Logs
	converter *Converter
	logger    *zap.Logger
	obsrecv   *receiverhelper.ObsReport

	storageID     *component.ID
	storageClient storage.Client
}

// Ensure this receiver adheres to required interface
var _ rcvr.Logs = (*receiver)(nil)

// Start tells the receiver to start
func (r *receiver) Start(ctx context.Context, host component.Host) error {
	rctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.logger.Info("Starting stanza receiver")

	if err := r.setStorageClient(ctx, host); err != nil {
		return fmt.Errorf("storage client: %w", err)
	}

	if err := r.pipe.Start(r.storageClient); err != nil {
		return fmt.Errorf("start stanza: %w", err)
	}

	r.converter.Start()

	// Below we're starting 2 loops:
	// * one which reads all the logs produced by the emitter and then forwards
	//   them to converter
	// ...
	r.wg.Add(1)
	rctr, cancel := context.WithCancel(ctx)
	go r.emitterLoop(rctx, cancel)

	// ...
	// * second one which reads all the logs produced by the converter
	//   (aggregated by Resource) and then calls consumer to consumer them.
	r.wg.Add(1)
	go r.consumerLoop(rctr)

	// Those 2 loops are started in separate goroutines because batching in
	// the emitter loop can cause a flush, caused by either reaching the max
	// flush size or by the configurable ticker which would in turn cause
	// a set of log entries to be available for reading in converter's out
	// channel. In order to prevent backpressure, reading from the converter
	// channel and batching are done in those 2 goroutines.

	return nil
}

// emitterLoop reads the log entries produced by the emitter and batches them
// in converter.
func (r *receiver) emitterLoop(ctx context.Context, cancel context.CancelFunc) {
	defer r.wg.Done()

	for e := range r.emitter.logChan {
		if err := r.converter.Batch(e); err != nil {
			r.logger.Error("Could not add entry to batch", zap.Error(err))
		}
	}
	// signal the Converter to immidiately flush and stop. The Converter will stop the consumerLoop
	r.converter.Stop()
	return
}

// consumerLoop reads converter log entries and calls the consumer to consumer them.
func (r *receiver) consumerLoop(ctx context.Context) {
	defer r.wg.Done()

	pLogsChan := r.converter.OutChannel()
	for pLogs := range pLogsChan {
		obsrecvCtx := r.obsrecv.StartLogsOp(ctx)
		logRecordCount := pLogs.LogRecordCount()
		cErr := r.consumer.ConsumeLogs(ctx, pLogs)
		if cErr != nil {
			r.logger.Error("ConsumeLogs() failed", zap.Error(cErr))
		}
		r.obsrecv.EndLogsOp(obsrecvCtx, "stanza", logRecordCount, cErr)
	}
	return
}

// Shutdown is invoked during service shutdown
func (r *receiver) Shutdown(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}

	r.logger.Warn("Stopping stanza receiver")
	// HEHE: step1: this will need to first stop all the readers in step2
	// the readers stop happens syncronously
	// this will also stop the emmiter and hence the r.emitter.logChan. It can be the first trigger point
	pipelineErr := r.pipe.Stop()
	// here the Reader should be stopped
	// here in order to properly stop the converter
	r.wg.Wait()

	if r.storageClient != nil {
		clientErr := r.storageClient.Close(ctx)
		return multierr.Combine(pipelineErr, clientErr)
	}
	return pipelineErr
}
