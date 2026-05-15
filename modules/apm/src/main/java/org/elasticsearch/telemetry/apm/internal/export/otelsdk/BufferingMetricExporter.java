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

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.apm.internal.export.otelsdk.serializer.MetricDataSerializer;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persists failed export batches to disk and replays them once the delegate recovers.
 */
public class BufferingMetricExporter extends DelegatingMetricExporter {

    private static final Logger logger = LogManager.getLogger(BufferingMetricExporter.class);

    private static final TimeValue RECONCILE_INTERVAL = TimeValue.timeValueMinutes(15);

    private final long maxDiskBytes;
    private final long ttlMillis;
    private final Path diskDir;
    private final AtomicLong totalBytes = new AtomicLong();
    private final TimeValue exportOperationTimeout;

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
    private final LongCounter evictionsTtlCounter;

    public BufferingMetricExporter(MetricExporter delegate, Settings settings, Path bufferPath, Meter meter) {
        super(delegate, meter, "disk_buffer");
        this.maxDiskBytes = OtelSdkSettings.TELEMETRY_OTEL_METRICS_DISK_BUFFER_SIZE.get(settings).getBytes();
        this.ttlMillis = OtelSdkSettings.TELEMETRY_OTEL_METRICS_BUFFER_TTL.get(settings).millis();
        this.exportOperationTimeout = OtelSdkSettings.computeExportOperationTimeout(settings);
        this.diskDir = bufferPath;
        initDiskDir();

        this.writesCounter = counter("writes", "Metric batches written to disk after delegate failure");
        this.replaysCounter = counter("replays", "Disk-buffered batches successfully replayed to the delegate");
        this.dropsFullCounter = counter("drops_full", "Failed batches dropped because the disk buffer is at its size cap");
        this.evictionsTtlCounter = counter(
            "evictions_ttl",
            "Disk-buffered batches dropped because they exceeded the buffer TTL before being replayed"
        );
        longGauge("files", "Metric batches currently pending replay on disk", "1", () -> BufferFiles.list(diskDir).size());
        longGauge("bytes", "Total bytes currently used by the on-disk metric buffer", "By", totalBytes::get);

        this.diskExecutor = new Scheduler.SafeScheduledThreadPoolExecutor(
            1,
            EsExecutors.daemonThreadFactory(settings, "metrics_buffer_disk"),
            new EsAbortPolicy()
        );
        diskExecutor.scheduleWithFixedDelay(
            this::reconcileTotalBytes,
            RECONCILE_INTERVAL.millis(),
            RECONCILE_INTERVAL.millis(),
            TimeUnit.MILLISECONDS
        );
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
        return delegate.shutdown();
    }

    private void initDiskDir() {
        try {
            Files.createDirectories(diskDir);
            BufferFiles.cleanupOrphanTmp(diskDir);
            long sum = 0L;
            for (Path f : BufferFiles.list(diskDir)) {
                sum += safeFileSize(f);
            }
            totalBytes.set(sum);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to initialize APM telemetry disk buffer at [" + diskDir + "]", e);
        }
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
            if (writeToDisk(metrics)) {
                result.succeed();
            } else {
                result.fail();
            }
        } catch (Exception e) {
            logger.warn("unexpected failure while writing metrics to disk", e);
            result.fail();
        }
    }

    private boolean writeToDisk(Collection<MetricData> metrics) {
        if (totalBytes.get() >= maxDiskBytes) {
            logger.warn("APM telemetry disk buffer at size cap [{} bytes]; dropping failed batch", maxDiskBytes);
            dropsFullCounter.add(1);
            return false;
        }
        return serializeBatch(diskDir, metrics);
    }

    // totalBytes can drift over time due to IOExceptions in reading file size. This reconciles it periodically.
    private void reconcileTotalBytes() {
        if (totalBytes.get() == 0L) {
            return;
        }
        long sum = 0L;
        for (Path f : BufferFiles.list(diskDir)) {
            sum += safeFileSize(f);
        }
        totalBytes.set(sum);
    }

    private boolean serializeBatch(Path dir, Collection<MetricData> metrics) {
        String name = BufferFiles.nameFor();
        Path tmp = dir.resolve(name + BufferFiles.TMP_SUFFIX);
        Path dst = dir.resolve(name);
        try {
            try (OutputStream out = Files.newOutputStream(tmp)) {
                MetricDataSerializer.serialize(metrics, out);
            }
            long size = Files.size(tmp);
            Files.move(tmp, dst, StandardCopyOption.ATOMIC_MOVE);
            totalBytes.addAndGet(size);
            writesCounter.add(1);
            return true;
        } catch (IOException e) {
            logger.warn("failed to write metrics to disk", e);
            IOUtils.deleteFilesIgnoringExceptions(tmp);
            return false;
        }
    }

    private void drainFiles() {
        for (Path file : BufferFiles.list(diskDir)) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            if (evictIfExpired(file)) {
                continue;
            }
            long size = safeFileSize(file);
            List<MetricData> metrics;
            try (var is = Files.newInputStream(file)) {
                metrics = MetricDataSerializer.deserialize(is);
            } catch (NoSuchFileException e) {
                continue;
            } catch (ClosedByInterruptException | InterruptedIOException e) {
                // Interrupt mid-read: leave the file for next startup.
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                logger.warn(() -> "dropping unrecoverable disk-buffered metrics file [" + file + "]", e);
                deleteAndAccount(file, size);
                continue;
            }
            if (metrics.isEmpty()) {
                // E.g. all points had unsupported types.
                deleteAndAccount(file, size);
                continue;
            }
            try {
                CompletableResultCode result = delegate.export(metrics).join(exportOperationTimeout.millis(), TimeUnit.MILLISECONDS);
                if (result.isSuccess()) {
                    replaysCounter.add(1);
                    deleteAndAccount(file, size);
                } else {
                    logger.warn("delegate failed replay of disk-buffered metrics [{}]; deferring drain", file);
                    return;
                }
            } catch (Exception e) {
                logger.warn(() -> "delegate threw while replaying [" + file + "]; deferring drain", e);
                return;
            }
        }
    }

    private static long safeFileSize(Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            return 0L;
        }
    }

    private void deleteAndAccount(Path file, long size) {
        try {
            if (Files.deleteIfExists(file)) {
                totalBytes.addAndGet(-size);
            }
        } catch (IOException e) {
            logger.warn(() -> "replayed/evicted disk-buffered metrics but failed to delete [" + file + "]", e);
        }
    }

    private boolean evictIfExpired(Path file) {
        try {
            long fileTime = Files.getLastModifiedTime(file).toMillis();
            if (System.currentTimeMillis() - fileTime > ttlMillis) {
                long size = safeFileSize(file);
                if (Files.deleteIfExists(file)) {
                    totalBytes.addAndGet(-size);
                    evictionsTtlCounter.add(1);
                    return true;
                }
            }
        } catch (NoSuchFileException e) {
            // already gone
        } catch (IOException e) {
            logger.warn(() -> "failed to check/evict expired disk file [" + file + "]", e);
        }
        return false;
    }

    private static final class BufferFiles {
        private static final Logger logger = LogManager.getLogger(BufferFiles.class);

        static final String PREFIX = "metrics-";
        static final String SUFFIX = ".bin";
        static final String TMP_SUFFIX = ".tmp";

        private BufferFiles() {}

        static String nameFor() {
            return PREFIX + UUIDs.base64UUID() + SUFFIX;
        }

        static List<Path> list(Path dir) {
            List<Path> files = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, PREFIX + "*" + SUFFIX)) {
                for (Path p : stream) {
                    files.add(p);
                }
            } catch (IOException e) {
                logger.warn(() -> "failed to list disk buffer directory [" + dir + "]", e);
            }
            return files;
        }

        static void cleanupOrphanTmp(Path dir) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + TMP_SUFFIX)) {
                for (Path tmp : stream) {
                    Files.deleteIfExists(tmp);
                }
            } catch (IOException e) {
                logger.warn(() -> "failed to delete orphaned tmp files from [" + dir + "]", e);
            }
        }
    }

}
