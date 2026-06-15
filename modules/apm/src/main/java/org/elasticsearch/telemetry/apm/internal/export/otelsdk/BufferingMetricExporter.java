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
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.MeterProvider;
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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Persists failed export batches to disk and replays them once the delegate recovers.
 */
public class BufferingMetricExporter implements MetricExporter {

    private static final Logger logger = LogManager.getLogger(BufferingMetricExporter.class);

    private final MetricExporter delegate;
    private final BufferingMetrics bufferingMetrics;

    private final TimeValue sendTimeout;
    private final Path diskDir;

    private final SignalStorage.Metric storage;
    private final MetricToDiskExporter diskWriter;

    private final Scheduler.SafeScheduledThreadPoolExecutor diskExecutor;

    private volatile long cachedFileCount;

    public BufferingMetricExporter(
        MetricExporter delegate,
        Settings settings,
        Path bufferPath,
        Supplier<MeterProvider> meterProviderSupplier
    ) {
        this.delegate = delegate;
        this.bufferingMetrics = new BufferingMetrics(meterProviderSupplier);
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
        return delegate.shutdown();
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
                    bufferingMetrics.writes().add(1);
                    result.succeed();
                } else {
                    bufferingMetrics.fullDrops().add(1);
                    result.fail();
                }
                refreshDiskStats();
            });
        } catch (Exception e) {
            logger.warn("unexpected failure while writing metrics to disk", e);
            bufferingMetrics.fullDrops().add(1);
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
                    bufferingMetrics.replays().add(1);
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
        bufferingMetrics.cachedFiles().set(files);
        bufferingMetrics.cachedBytes().set(bytes);
    }

    @SuppressForbidden(reason = "disk-buffering library exposes a java.io.File based API")
    private static SignalStorage.Metric createStorage(Path dir, FileStorageConfiguration config) {
        return FileMetricStorage.create(dir.toFile(), config);
    }

    private static final class BufferingMetrics {

        private static final String METRIC_PREFIX = "es.apm.metrics.disk_buffer.";

        private final Supplier<MeterProvider> meterProviderSupplier;

        @Nullable
        private volatile LongCounter writes = null;
        @Nullable
        private volatile LongCounter replays = null;
        @Nullable
        private volatile LongCounter fullDrops = null;
        @Nullable
        private volatile LongGauge cachedFiles = null;
        @Nullable
        private volatile LongGauge cachedBytes = null;

        private BufferingMetrics(Supplier<MeterProvider> meterProviderSupplier) {
            this.meterProviderSupplier = meterProviderSupplier;
        }

        private LongCounter writes() {
            var counter = this.writes;
            if (counter == null) {
                counter = longCounter("writes", "Metric batches written to disk after delegate failure");
                this.writes = counter;
            }
            return counter;
        }

        private LongCounter replays() {
            var counter = this.replays;
            if (counter == null) {
                counter = longCounter("replays", "Disk-buffered batches successfully replayed to the delegate");
                this.replays = counter;
            }
            return counter;
        }

        private LongCounter fullDrops() {
            var counter = this.fullDrops;
            if (counter == null) {
                counter = longCounter(
                    "drops_full",
                    "Failed batches dropped because the disk buffer is at its size cap or disk write failed"
                );
                this.fullDrops = counter;
            }
            return counter;
        }

        private LongGauge cachedFiles() {
            var gauge = this.cachedFiles;
            if (gauge == null) {
                gauge = longGauge("files", "Metric batches currently pending replay on disk", "1");
                this.cachedFiles = gauge;
            }
            return gauge;
        }

        private LongGauge cachedBytes() {
            var gauge = this.cachedBytes;
            if (gauge == null) {
                gauge = longGauge("bytes", "Total bytes currently used by the on-disk metric buffer", "By");
                this.cachedBytes = gauge;
            }
            return gauge;
        }

        private LongCounter longCounter(String suffix, String description) {
            return meterProviderSupplier.get()
                .get("elasticsearch")
                .counterBuilder(METRIC_PREFIX + suffix)
                .setDescription(description)
                .setUnit("1")
                .build();
        }

        private LongGauge longGauge(String suffix, String description, String unit) {
            return meterProviderSupplier.get()
                .get("elasticsearch")
                .gaugeBuilder(METRIC_PREFIX + suffix)
                .ofLongs()
                .setDescription(description)
                .setUnit(unit)
                .build();
        }
    }
}
