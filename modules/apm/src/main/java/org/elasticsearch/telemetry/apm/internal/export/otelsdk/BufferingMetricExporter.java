/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * A {@link MetricExporter} that writes failed export batches directly to disk as JSON
 * files and replays them through the delegate exporter when the endpoint becomes available again.
 */
public class BufferingMetricExporter implements MetricExporter {

    private static final Logger logger = LogManager.getLogger(BufferingMetricExporter.class);

    private static final String FILE_PREFIX = "metrics-";
    private static final String FILE_SUFFIX = ".bin";
    private static final String TMP_SUFFIX = ".tmp";

    private final MetricExporter delegate;
    private final long maxDiskBytes;
    private final long ttlMillis;
    private final Supplier<Path> diskBufferPathSupplier;

    private final AtomicLong diskSeqNo = new AtomicLong();
    private final AtomicBoolean drainInProgress = new AtomicBoolean();
    private final ExecutorService drainExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "metrics-buffer-drain");
        t.setDaemon(true);
        return t;
    });
    private volatile Path resolvedDiskPath;

    public BufferingMetricExporter(MetricExporter delegate, Settings settings, Supplier<Path> diskBufferPathSupplier) {
        this.delegate = delegate;
        this.maxDiskBytes = OtelSdkSettings.TELEMETRY_OTEL_METRICS_DISK_BUFFER_SIZE.get(settings).getBytes();
        this.ttlMillis = OtelSdkSettings.TELEMETRY_OTEL_METRICS_BUFFER_TTL.get(settings).millis();
        this.diskBufferPathSupplier = diskBufferPathSupplier;
    }

    /**
     * Lazily resolves the disk buffer directory from the supplier.
     */
    private Path diskPath() {
        if (resolvedDiskPath != null) {
            return resolvedDiskPath;
        }
        Path supplied = diskBufferPathSupplier.get();
        if (supplied == null) {
            return null;
        }
        try {
            Files.createDirectories(supplied);
            cleanupTmpFiles(supplied);
            resolvedDiskPath = supplied;
            return supplied;
        } catch (IOException e) {
            logger.warn("failed to initialize disk buffer at [{}], disk buffering disabled", supplied, e);
            return null;
        }
    }

    /**
     * Ensures the sequence counter is advanced past any pre-existing {@code .bin} files
     * so new writes never collide with files from a previous run.
     */
    private void initSeqNoIfNeeded(Path dir) {
        if (diskSeqNo.get() == 0) {
            long maxExistingSeq = listDiskFiles(dir).stream().map(BufferingMetricExporter::seqNoFromFileName).reduce(0L, Math::max);
            diskSeqNo.set(maxExistingSeq + 1);
        }
    }

    /**
     * Non-blocking export that delegates to the underlying exporter and registers an async callback.
     * On success the callback schedules a background drain of any disk-buffered files.
     * On failure the current batch is serialized to disk so it can be replayed later.
     */
    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        evictExpiredDiskFiles();
        CompletableResultCode result = delegate.export(metrics);
        result.whenComplete(() -> {
            if (result.isSuccess()) {
                scheduleDrain();
            } else {
                writeToDisk(metrics);
            }
        });
        return result;
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return delegate.getAggregationTemporality(instrumentType);
    }

    @Override
    public Aggregation getDefaultAggregation(InstrumentType instrumentType) {
        return delegate.getDefaultAggregation(instrumentType);
    }

    /**
     * Best-effort drain of disk-buffered files before flushing the delegate.
     * <p>
     * Called by the OTel SDK after an {@code export()} cycle has already completed, so by the
     * time we reach here the export callback has already fired (writing to disk on failure or
     * scheduling a drain on success).
     */
    @Override
    public CompletableResultCode flush() {
        try {
            drainExecutor.submit(this::drainDisk).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.info("Exception while waiting for drain during flush: ", e);
        }
        return delegate.flush();
    }

    /**
     * Waits for any in-progress drain to finish, then performs a final
     * synchronous drain on the calling thread. This last drain catches files written between the
     * last scheduled drain and shutdown.
     */
    @Override
    public CompletableResultCode shutdown() {
        drainExecutor.shutdown();
        try {
            if (drainExecutor.awaitTermination(10, TimeUnit.SECONDS) == false) {
                logger.info("Timeout reached trying to flush metrics from disk before shutdown.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        drainDisk();
        return delegate.shutdown();
    }

    /**
     * Schedules a drain on the background thread if one is not already running.
     */
    private void scheduleDrain() {
        if (drainInProgress.compareAndSet(false, true)) {
            drainExecutor.submit(() -> {
                try {
                    drainDisk();
                } finally {
                    drainInProgress.set(false);
                }
            });
        }
    }

    /**
     * Serializes the metrics batch to a JSON file on disk using atomic rename
     * (write to {@code .tmp}, then {@code ATOMIC_MOVE} to {@code .bin}) so readers never
     * see a partially-written file.
     */
    private void writeToDisk(Collection<MetricData> metrics) {
        Path dir = diskPath();
        if (dir == null) {
            logger.debug("dropping failed batch (disk buffer path unavailable)");
            return;
        }
        initSeqNoIfNeeded(dir);
        if (isDiskOverLimit(dir)) {
            logger.debug("disk buffer full, dropping batch");
            return;
        }
        try {
            long seq = diskSeqNo.getAndIncrement();
            String fileName = FILE_PREFIX + seq + FILE_SUFFIX;
            Path tmpFile = dir.resolve(fileName + TMP_SUFFIX);
            Path finalFile = dir.resolve(fileName);
            try (OutputStream out = Files.newOutputStream(tmpFile)) {
                MetricDataSerializer.serialize(metrics, out);
            }
            Files.move(tmpFile, finalFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            logger.warn("failed to write metrics to disk", e);
        }
    }

    /**
     * Reads disk-buffered files in sequence-number order, deserializes each back to
     * {@link MetricData} objects, and re-exports through the delegate.
     * Successfully replayed files are deleted.
     */
    private void drainDisk() {
        Path dir = diskPath();
        if (dir == null) {
            return;
        }
        List<Path> files = listDiskFiles(dir);
        for (Path file : files) {
            try (var is = Files.newInputStream(file)) {
                List<MetricData> metrics = MetricDataSerializer.deserialize(is);
                CompletableResultCode result = delegate.export(metrics);
                if (result.join(10, TimeUnit.SECONDS).isSuccess()) {
                    Files.deleteIfExists(file);
                } else {
                    return;
                }
            } catch (Exception e) {
                logger.warn("failed to replay disk-buffered metrics from [{}], skipping", file, e);
            }
        }
    }

    private boolean isDiskOverLimit(Path dir) {
        long totalBytes = 0;
        for (Path f : listDiskFiles(dir)) {
            try {
                totalBytes += Files.size(f);
            } catch (IOException e) {
                // file may have been deleted since listing
            }
        }
        return totalBytes >= maxDiskBytes;
    }

    private void evictExpiredDiskFiles() {
        Path dir = diskPath();
        if (dir == null) {
            return;
        }
        long now = System.currentTimeMillis();
        for (Path file : listDiskFiles(dir)) {
            try {
                long fileTime = Files.getLastModifiedTime(file).toMillis();
                if (now - fileTime > ttlMillis) {
                    Files.deleteIfExists(file);
                }
            } catch (IOException e) {
                logger.warn("failed to evict expired disk file [{}]", file, e);
            }
        }
    }

    private static List<Path> listDiskFiles(Path dir) {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, FILE_PREFIX + "*" + FILE_SUFFIX)) {
            for (Path p : stream) {
                files.add(p);
            }
        } catch (IOException e) {
            logger.debug("failed to list disk buffer directory [{}]", dir, e);
        }
        files.sort(Comparator.comparingLong(BufferingMetricExporter::seqNoFromFileName));
        return files;
    }

    private static long seqNoFromFileName(Path file) {
        String name = file.getFileName().toString();
        String numStr = name.substring(FILE_PREFIX.length(), name.length() - FILE_SUFFIX.length());
        try {
            return Long.parseLong(numStr);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static void cleanupTmpFiles(Path dir) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + TMP_SUFFIX)) {
            for (Path tmp : stream) {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            logger.warn("failed to delete orphaned tmp files from [{}]", dir, e);
        }
    }
}
