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
import io.opentelemetry.contrib.disk.buffering.exporters.MetricToDiskExporter;
import io.opentelemetry.contrib.disk.buffering.storage.SignalStorage;
import io.opentelemetry.contrib.disk.buffering.storage.impl.FileMetricStorage;
import io.opentelemetry.contrib.disk.buffering.storage.impl.FileStorageConfiguration;
import io.opentelemetry.sdk.common.CompletableResultCode;
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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Persists failed export batches to disk and replays them once the delegate recovers.
 */
public class BufferingMetricExporter extends DelegatingMetricExporter {

    private static final Logger logger = LogManager.getLogger(BufferingMetricExporter.class);

    // Library requires MIN_FILE_AGE_BEFORE_READ > MAX_WRITE_TIME_BEFORE_NEW_FILE; single-thread executor serializes I/O.
    private static final long MAX_WRITE_TIME_BEFORE_NEW_FILE_MILLIS = 100L;
    private static final long MIN_FILE_AGE_BEFORE_READ_MILLIS = 200L;

    private final TimeValue exportOperationTimeout;
    private final Path diskDir;

    private final SignalStorage.Metric storage;
    private final MetricToDiskExporter diskWriter;

    private final Scheduler.SafeScheduledThreadPoolExecutor diskExecutor;

    /**
     * Most recent in-flight export result. {@link #flush()} joins on this so disk-buffer writes
     * complete before returning. Single-writer field: only assigned by {@link #export(Collection)},
     * which the OTel {@code PeriodicMetricReader} drives from a single thread.
     */
    private volatile CompletableResultCode lastExport;

    private final LongCounter writesCounter;
    private final LongCounter replaysCounter;
    private final LongCounter dropsFullCounter;

    // Updated by the disk executor after each write/drain so gauges can read without I/O.
    private volatile long cachedFileCount;
    private volatile long cachedByteCount;

    public BufferingMetricExporter(MetricExporter delegate, Settings settings, Path bufferPath, Meter meter) {
        super(delegate, meter, "disk_buffer");
        long maxDiskBytes = OtelSdkSettings.TELEMETRY_OTEL_METRICS_DISK_BUFFER_SIZE.get(settings).getBytes();
        long ttlMillis = OtelSdkSettings.TELEMETRY_OTEL_METRICS_BUFFER_TTL.get(settings).millis();
        this.exportOperationTimeout = OtelSdkSettings.computeExportOperationTimeout(settings);
        this.diskDir = bufferPath;

        FileStorageConfiguration storageConfig = FileStorageConfiguration.builder()
            .setMaxFolderSize((int) maxDiskBytes)
            .setMaxFileAgeForReadMillis(ttlMillis)
            .setMaxFileAgeForWriteMillis(MAX_WRITE_TIME_BEFORE_NEW_FILE_MILLIS)
            .setMinFileAgeForReadMillis(MIN_FILE_AGE_BEFORE_READ_MILLIS)
            // drainFiles() removes items only after a successful replay.
            .setDeleteItemsOnIteration(false)
            .build();
        this.storage = createStorage(diskDir, storageConfig);
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
    public CompletableResultCode export(Collection<MetricData> metrics) {
        CompletableResultCode bufferedResult = new CompletableResultCode();
        lastExport = bufferedResult;
        try {
            CompletableResultCode otlpResult = delegate.export(metrics);
            otlpResult.whenComplete(() -> {
                try {
                    if (otlpResult.isSuccess()) {
                        scheduleAsyncDrain();
                        bufferedResult.succeed();
                    } else {
                        // submitWrite completes bufferedResult only after the batch lands on disk.
                        submitWrite(metrics, bufferedResult);
                    }
                } catch (Exception e) {
                    logger.warn("unexpected failure handling export completion", e);
                    bufferedResult.fail();
                }
            });
        } catch (Exception e) {
            logger.warn("unexpected error while exporting metrics; buffering batch to disk", e);
            submitWrite(metrics, bufferedResult);
        }
        return bufferedResult;
    }

    @Override
    public CompletableResultCode flush() {
        awaitLastExport(exportOperationTimeout.millis());
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        awaitLastExport(Long.MAX_VALUE);
        ThreadPool.terminate(diskExecutor, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        drainFiles();
        IOUtils.closeWhileHandlingException(diskWriter, storage);
        return delegate.shutdown();
    }

    private void awaitLastExport(long timeoutMillis) {
        CompletableResultCode pending = lastExport;
        if (pending != null) {
            pending.join(timeoutMillis, TimeUnit.MILLISECONDS);
        }
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
            // Post-shutdown: write inline rather than drop. Briefly blocks the OTLP thread; acceptable on shutdown.
            doWrite(metrics, result);
        }
    }

    private void doWrite(Collection<MetricData> metrics, CompletableResultCode result) {
        try {
            CompletableResultCode r = diskWriter.export(metrics).join(exportOperationTimeout.millis(), TimeUnit.MILLISECONDS);
            if (r.isSuccess()) {
                writesCounter.add(1);
                result.succeed();
            } else {
                dropsFullCounter.add(1);
                result.fail();
            }
        } catch (Exception e) {
            logger.warn("unexpected failure while writing metrics to disk", e);
            dropsFullCounter.add(1);
            result.fail();
        } finally {
            refreshDiskStats();
        }
    }

    private void drainFiles() {
        try {
            Iterator<Collection<MetricData>> it = storage.iterator();
            while (it.hasNext()) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                Collection<MetricData> batch = it.next();
                try {
                    CompletableResultCode result = delegate.export(batch).join(exportOperationTimeout.millis(), TimeUnit.MILLISECONDS);
                    if (result.isSuccess()) {
                        it.remove();
                        replaysCounter.add(1);
                    } else {
                        logger.warn("delegate failed replay of disk-buffered metrics; deferring drain");
                        return;
                    }
                } catch (Exception e) {
                    logger.warn("delegate threw while replaying disk-buffered metrics; deferring drain", e);
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
