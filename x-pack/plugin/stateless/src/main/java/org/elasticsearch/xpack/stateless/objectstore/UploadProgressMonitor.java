/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.Strings.format;

/**
 * Tracks how quickly a {@link VirtualBatchedCompoundCommit} (VBCC) is read while it is uploaded to the object store,
 * and logs periodic INFO progress lines for long-running uploads.
 * <p>
 * Used from {@link ObjectStoreService} during batched commit file uploads. Callers wrap upload {@link InputStream}s
 * with {@link #monitor(InputStream)} so byte counts reflect what the blob store has consumed from the VBCC source.
 * Progress logging is controlled by {@code stateless.object_store.upload_progress_log_interval}; a zero interval
 * disables both periodic logs and stream instrumentation overhead.
 * <p>
 * Logged {@code bytesUploaded} is not object-store acknowledgment progress. It is a conservative estimate of how
 * much data has been handed to the blob layer: during reads it intentionally lags {@code bytesRead} by one read
 * operation so progress lines never show {@code bytesUploaded &gt; bytesRead}. Each monitored stream catches up on
 * {@link MonitoredInputStream#close()}.
 */
final class UploadProgressMonitor implements Scheduler.Cancellable {

    private final Logger logger;

    private final ThreadPool threadPool;

    private final ShardId shardId;

    private final PrimaryTermAndGeneration primaryTermAndGeneration;

    private final String blobPath;

    private final long totalSizeInBytes;

    private final UploadProgressTracker progressTracker;

    private Scheduler.Cancellable loggingTask;

    private UploadProgressMonitor(
        Logger logger,
        ThreadPool threadPool,
        String blobPath,
        VirtualBatchedCompoundCommit virtualBatchedCompoundCommit,
        UploadProgressTracker progressTracker
    ) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.shardId = virtualBatchedCompoundCommit.getShardId();
        this.primaryTermAndGeneration = virtualBatchedCompoundCommit.getPrimaryTermAndGeneration();
        this.blobPath = blobPath;
        this.totalSizeInBytes = virtualBatchedCompoundCommit.getTotalSizeInBytes();
        this.progressTracker = progressTracker;
    }

    /**
     * Creates a monitor for one VBCC upload.
     *
     * @param logInterval how often to log progress; {@link TimeValue#ZERO} disables logging and byte tracking
     * @param multipart {@code true} when the upload may open multiple input streams concurrently (for example
     *                  concurrent multipart uploads with one stream per range); {@code false} for a single stream
     */
    public static UploadProgressMonitor newInstance(
        Logger logger,
        ThreadPool threadPool,
        BlobContainer blobContainer,
        VirtualBatchedCompoundCommit virtualBatchedCompoundCommit,
        TimeValue logInterval,
        boolean multipart
    ) {
        boolean enabled = logInterval.millis() > 0;
        var progressTracker = UploadProgressTracker.newInstance(enabled, multipart);
        var monitor = new UploadProgressMonitor(
            logger,
            threadPool,
            blobContainer.path().buildAsString() + virtualBatchedCompoundCommit.getBlobName(),
            virtualBatchedCompoundCommit,
            progressTracker
        );
        if (enabled) {
            monitor.startMonitoring(logInterval);
        }
        return monitor;
    }

    /**
     * Returns an {@link InputStream} that updates byte counters as the blob store reads from {@code in}.
     * When progress logging is disabled, returns {@code in} unchanged.
     */
    public InputStream monitor(InputStream in) {
        return progressTracker.track(in);
    }

    private void startMonitoring(TimeValue logInterval) {
        long uploadRunStartNanos = threadPool.relativeTimeInNanos();
        loggingTask = threadPool.scheduleWithFixedDelay(() -> logProgressLine(uploadRunStartNanos), logInterval, threadPool.generic());
    }

    @Override
    public boolean cancel() {
        return loggingTask == null || loggingTask.cancel();
    }

    @Override
    public boolean isCancelled() {
        return loggingTask == null || loggingTask.isCancelled();
    }

    private void logProgressLine(long uploadRunStartNanos) {
        // We read the uploaded bytes before the read ones to avoid races that could make the log message confusing (uploaded > read)
        long uploaded = progressTracker.bytesUploaded();
        long read = progressTracker.bytesRead();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(threadPool.relativeTimeInNanos() - uploadRunStartNanos);
        logger.info(
            () -> format(
                "%s batched compound commit upload progress [%s]: blob [%s] bytesRead=%d/%d bytesUploaded=%d/%d elapsed=%dms",
                shardId,
                primaryTermAndGeneration,
                blobPath,
                read,
                totalSizeInBytes,
                uploaded,
                totalSizeInBytes,
                elapsedMs
            )
        );
    }

    /**
     * Counts bytes consumed from VBCC upload streams.
     */
    interface UploadProgressTracker {

        /** Total bytes read from all monitored streams so far. */
        long bytesRead();

        /**
         * Bytes considered uploaded for logging; see class Javadoc for the relationship to {@link #bytesRead()}.
         */
        long bytesUploaded();

        InputStream track(InputStream in);

        static UploadProgressTracker newInstance(boolean enabled, boolean multipart) {

            if (enabled == false) return NoopUploadTracker.INSTANCE;

            return multipart ? new MultiPartUploadTracker() : new SinglePartUploadTracker();
        }
    }

    /** Used when progress logging is disabled. */
    static final class NoopUploadTracker implements UploadProgressTracker {

        static final UploadProgressTracker INSTANCE = new NoopUploadTracker();

        @Override
        public long bytesRead() {
            return 0;
        }

        @Override
        public long bytesUploaded() {
            return 0;
        }

        @Override
        public InputStream track(InputStream in) {
            return in;
        }
    }

    /**
     * Tracks one upload input stream. Safe when a single stream is read sequentially.
     */
    static final class SinglePartUploadTracker implements UploadProgressTracker {

        private final AtomicLong bytesRead = new AtomicLong();
        private final AtomicLong bytesUploaded = new AtomicLong();

        public InputStream track(InputStream in) {
            return new MonitoredInputStream(in, this);
        }

        @Override
        public long bytesRead() {
            return bytesRead.get();
        }

        @Override
        public long bytesUploaded() {
            return bytesUploaded.get();
        }

        /**
         * Records {@code bytes} newly read. Sets {@link #bytesUploaded} to the pre-read total so it never exceeds
         * {@link #bytesRead} while the stream is open.
         */
        void onByteRead(long bytes) {
            bytesUploaded.set(bytesRead.getAndAdd(bytes));
        }

        /** Aligns {@link #bytesUploaded} with {@link #bytesRead} when the stream is fully consumed. */
        void onClose() {
            bytesUploaded.set(bytesRead.get());
        }
    }

    /**
     * Tracks multiple concurrent upload streams by summing per-stream {@link SinglePartUploadTracker} instances.
     */
    static final class MultiPartUploadTracker implements UploadProgressTracker {

        private final List<SinglePartUploadTracker> trackers = new CopyOnWriteArrayList<>();

        public InputStream track(InputStream in) {
            SinglePartUploadTracker tracker = new SinglePartUploadTracker();
            this.trackers.add(tracker);
            return tracker.track(in);
        }

        @Override
        public long bytesRead() {
            long bytesRead = 0;
            for (var tracker : trackers) {
                bytesRead += tracker.bytesRead();
            }
            return bytesRead;
        }

        @Override
        public long bytesUploaded() {
            long byteUploaded = 0;
            for (var tracker : trackers) {
                byteUploaded += tracker.bytesUploaded();
            }
            return byteUploaded;
        }
    }

    /** {@link FilterInputStream} that forwards read/skip/close events to a {@link SinglePartUploadTracker}. */
    static final class MonitoredInputStream extends FilterInputStream {

        private final SinglePartUploadTracker uploadTracker;

        MonitoredInputStream(InputStream in, SinglePartUploadTracker uploadMonitor) {
            super(Objects.requireNonNull(in, "in"));
            this.uploadTracker = Objects.requireNonNull(uploadMonitor, "uploadTracker");
        }

        @Override
        public int read() throws IOException {
            int n = super.read();
            if (n != -1) {
                uploadTracker.onByteRead(1);
            }
            return n;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n > 0) {
                this.uploadTracker.onByteRead(n);
            }
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = super.skip(n);
            if (skipped > 0) {
                this.uploadTracker.onByteRead(skipped);
            }
            return skipped;
        }

        @Override
        public void close() throws IOException {
            this.uploadTracker.onClose();
            super.close();
        }
    }
}
