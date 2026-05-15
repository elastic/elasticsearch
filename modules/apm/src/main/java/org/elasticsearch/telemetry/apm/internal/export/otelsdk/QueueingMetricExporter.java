/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Decouples the metric reader from the delegate via a bounded single-thread FIFO worker.
 * {@link #export(Collection)} returns success as soon as the batch is queued; the eventual outcome
 * surfaces through the {@code es.apm.metrics.export_queue.*} counters.
 * <p>
 * On overflow we drop the incoming batch (drop-newest), keeping the older queued entries that are
 * closest to the onset of the slowdown.
 */
public class QueueingMetricExporter extends DelegatingMetricExporter {

    private static final Logger logger = LogManager.getLogger(QueueingMetricExporter.class);

    private final LongCounter drainedCounter;
    private final LongCounter droppedCounter;
    private final EsThreadPoolExecutor worker;
    private final TimeValue exportOperationTimeout;

    public QueueingMetricExporter(MetricExporter delegate, Settings settings, Meter meter) {
        super(delegate, meter, "export_queue");
        int capacity = OtelSdkSettings.TELEMETRY_OTEL_METRICS_EXPORT_QUEUE_SIZE.get(settings);
        if (capacity <= 0) {
            throw new IllegalArgumentException("queue capacity must be positive, got [" + capacity + "]");
        }
        this.exportOperationTimeout = OtelSdkSettings.computeExportOperationTimeout(settings);

        this.drainedCounter = counter("drained", "Metric batches the background worker successfully exported through the delegate");
        this.droppedCounter = counter("dropped", "Newest metric batches dropped because the queue was full");

        this.worker = EsExecutors.newFixed(
            "metrics_export_queue",
            1,
            capacity,
            EsExecutors.daemonThreadFactory(settings, "metrics_export_queue"),
            new ThreadContext(settings),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );

        longGauge("depth", "Metric batches currently waiting in the in-memory export queue", "1", () -> worker.getQueue().size());
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        try {
            worker.execute(() -> exportBatch(metrics));
            return CompletableResultCode.ofSuccess();
        } catch (EsRejectedExecutionException e) {
            if (e.isExecutorShutdown()) {
                logger.error("metrics export queue rejected a batch after shutdown", e);
                return CompletableResultCode.ofFailure();
            }
            // Queue full: drop the newest (this incoming batch), keep older queued entries closest to the onset of the slowdown.
            droppedCounter.add(1);
            return CompletableResultCode.ofSuccess();
        }
    }

    @Override
    public CompletableResultCode flush() {
        // A no-op queued now runs only after every previously enqueued batch is drained.
        try {
            worker.submit(() -> {}).get(exportOperationTimeout.millis(), TimeUnit.MILLISECONDS);
        } catch (EsRejectedExecutionException e) {
            // already shut down
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            logger.warn("flush did not complete within the configured timeout");
        } catch (ExecutionException e) {
            logger.error("metrics queue flush failed", e.getCause());
        }
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        ThreadPool.terminate(worker, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        return delegate.shutdown();
    }

    private void exportBatch(Collection<MetricData> batch) {
        try {
            CompletableResultCode result = delegate.export(batch);
            result.whenComplete(() -> {
                if (result.isSuccess()) {
                    drainedCounter.add(1);
                } else {
                    logger.warn("delegate failed to export queued metric batch");
                }
            });
            result.join(exportOperationTimeout.millis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.warn("failed to export queued metric batch", e);
        }
    }
}
