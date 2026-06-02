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
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.contrib.disk.buffering.exporters.MetricToDiskExporter;
import io.opentelemetry.contrib.disk.buffering.storage.SignalStorage;
import io.opentelemetry.contrib.disk.buffering.storage.impl.FileMetricStorage;
import io.opentelemetry.contrib.disk.buffering.storage.impl.FileStorageConfiguration;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Persists failed export batches to disk and replays them once the delegate recovers.
 */
public class BufferingMetricExporter implements MetricExporter {

    private static final Logger logger = LogManager.getLogger(BufferingMetricExporter.class);

    private static final String METRIC_PREFIX = "es.apm.metrics.disk_buffer.";

    private final MetricExporter delegate;
    private final Meter meter;
    private final List<ObservableLongGauge> selfMeters = new ArrayList<>();

    private final TimeValue sendTimeout;
    private final Path diskDir;

    private final SignalStorage.Metric storage;
    private final MetricToDiskExporter diskWriter;

    private final Scheduler.SafeScheduledThreadPoolExecutor diskExecutor;

    private final LongCounter writesCounter;
    private final LongCounter replaysCounter;
    private final LongCounter dropsFullCounter;

    private volatile long cachedFileCount;
    private volatile long cachedByteCount;

    public BufferingMetricExporter(MetricExporter delegate, Settings settings, Path bufferPath, Meter meter) {
        this.delegate = delegate;
        this.meter = meter;
        long maxDiskBytes = OtelSdkSettings.TELEMETRY_OTEL_METRICS_DISK_BUFFER_SIZE.get(settings).getBytes();
        long ttlMillis = OtelSdkSettings.TELEMETRY_OTEL_METRICS_BUFFER_TTL.get(settings).millis();
        long writeWindowMillis = OtelSdkSettings.TELEMETRY_OTEL_METRICS_DISK_BUFFER_WRITE_WINDOW.get(settings).millis();
        long readMinAgeMillis = OtelSdkSettings.TELEMETRY_OTEL_METRICS_DISK_BUFFER_READ_MIN_AGE.get(settings).millis();
        this.sendTimeout = OtelSdkSettings.TELEMETRY_OTEL_OTLP_SEND_TIMEOUT.get(settings);
        this.diskDir = bufferPath;

        FileStorageConfiguration config = FileStorageConfiguration.builder()
            .setMaxFolderSize((int) maxDiskBytes)
            .setMaxFileAgeForReadMillis(ttlMillis)
            .setMaxFileAgeForWriteMillis(writeWindowMillis)
            .setMinFileAgeForReadMillis(readMinAgeMillis)
            // drainFiles() removes items only after a successful replay.
            .setDeleteItemsOnIteration(false)
            .build();
        this.storage = createStorage(diskDir, config);
        this.diskWriter = MetricToDiskExporter.builder(storage).build();

        this.writesCounter = counter("writes", "Metric batches written to disk after delegate failure");
        this.replaysCounter = counter("replays", "Disk-buffered batches successfully replayed to the delegate");
        this.dropsFullCounter = counter(
            "drops_full",
            "Failed batches dropped because the disk buffer is at its size cap or disk write failed"
        );
        longGauge("files", "Metric batches currently pending replay on disk", "1", () -> cachedFileCount);
        longGauge("bytes", "Total bytes currently used by the on-disk metric buffer", "By", () -> cachedByteCount);

        this.diskExecutor = new Scheduler.SafeScheduledThreadPoolExecutor(
            1,
            EsExecutors.daemonThreadFactory(settings, "metrics_buffer_disk"),
            new EsAbortPolicy()
        );
        refreshDiskStats();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return delegate.getAggregationTemporality(instrumentType);
    }

    @Override
    public Aggregation getDefaultAggregation(InstrumentType instrumentType) {
        return delegate.getDefaultAggregation(instrumentType);
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        CompletableResultCode bufferedResult = new CompletableResultCode();
        CompletableResultCode otlpResult = delegate.export(metrics);
        otlpResult.whenComplete(() -> {
            if (otlpResult.isSuccess()) {
                if (cachedFileCount > 0) {
                    scheduleAsyncDrain();
                }
                bufferedResult.succeed();
            } else {
                submitWrite(metrics, bufferedResult);
            }
        });
        return bufferedResult;
    }

    @Override
    public CompletableResultCode flush() {
        CompletableResultCode delegateFlush = delegate.flush();
        if (cachedFileCount == 0) {
            return delegateFlush;
        }
        CompletableResultCode drainResult = new CompletableResultCode();
        try {
            diskExecutor.execute(() -> {
                try {
                    drainFiles();
                } finally {
                    drainResult.succeed();
                }
            });
        } catch (RejectedExecutionException e) {
            drainResult.succeed();
        }
        return CompletableResultCode.ofAll(List.of(delegateFlush, drainResult));
    }

    @Override
    public CompletableResultCode shutdown() {
        diskExecutor.shutdown();
        IOUtils.closeWhileHandlingException(diskWriter, storage);
        selfMeters.forEach(ObservableLongGauge::close);
        return delegate.shutdown();
    }

    private LongCounter counter(String suffix, String description) {
        return meter.counterBuilder(METRIC_PREFIX + suffix).setDescription(description).setUnit("1").build();
    }

    private void longGauge(String suffix, String description, String unit, LongSupplier value) {
        selfMeters.add(
            meter.gaugeBuilder(METRIC_PREFIX + suffix)
                .ofLongs()
                .setDescription(description)
                .setUnit(unit)
                .buildWithCallback(r -> r.record(value.getAsLong()))
        );
    }

    private void scheduleAsyncDrain() {
        try {
            diskExecutor.execute(this::drainFiles);
        } catch (RejectedExecutionException e) {
            logger.debug("skipping scheduled drain: disk executor terminated", e);
        }
    }

    private void submitWrite(Collection<MetricData> metrics, CompletableResultCode result) {
        try {
            diskExecutor.execute(() -> doWrite(metrics, result));
        } catch (RejectedExecutionException e) {
            // Post-shutdown: write inline rather than drop the batch.
            doWrite(metrics, result);
        }
    }

    private void doWrite(Collection<MetricData> metrics, CompletableResultCode result) {
        try {
            CompletableResultCode r = diskWriter.export(metrics);
            r.whenComplete(() -> {
                if (r.isSuccess()) {
                    writesCounter.add(1);
                    result.succeed();
                } else {
                    dropsFullCounter.add(1);
                    result.fail();
                }
                refreshDiskStats();
            });
        } catch (Exception e) {
            logger.warn("unexpected failure while writing metrics to disk", e);
            dropsFullCounter.add(1);
            result.fail();
            refreshDiskStats();
        }
    }

    private void drainFiles() {
        try {
            Iterator<Collection<MetricData>> it = storage.iterator();
            while (it.hasNext()) {
                Collection<MetricData> batch = it.next();
                CompletableResultCode result = delegate.export(batch).join(sendTimeout.millis(), TimeUnit.MILLISECONDS);
                if (result.isSuccess()) {
                    it.remove();
                    replaysCounter.add(1);
                } else {
                    logger.warn("delegate failed replay of disk-buffered metrics; deferring drain");
                    return;
                }
            }
        } finally {
            refreshDiskStats();
        }
    }

    private void refreshDiskStats() {
        long files = 0;
        long bytes = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(diskDir)) {
            for (Path f : stream) {
                try {
                    bytes += Files.size(f);
                    files++;
                } catch (IOException ignored) {
                    // file may have been deleted between listing and size
                }
            }
        } catch (IOException e) {
            logger.warn("failed to scan disk buffer directory", e);
        }
        cachedFileCount = files;
        cachedByteCount = bytes;
    }

    @SuppressForbidden(reason = "disk-buffering library exposes a java.io.File based API")
    private static SignalStorage.Metric createStorage(Path dir, FileStorageConfiguration config) {
        return FileMetricStorage.create(dir.toFile(), config);
    }
}
