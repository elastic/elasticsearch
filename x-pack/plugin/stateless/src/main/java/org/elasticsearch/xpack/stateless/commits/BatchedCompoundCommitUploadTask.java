/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.core.Strings.format;

/**
 * Runs a task to upload a new commit to the blob store. Handles errors and retrying by implementing the {@link RetryableAction} interface.
 */
public class BatchedCompoundCommitUploadTask extends RetryableAction<BccUploadResult> {

    private static final Logger logger = LogManager.getLogger(BatchedCompoundCommitUploadTask.class);

    private final ThreadPool threadPool;
    private final SharedBlobCacheWarmingService cacheWarmingService;
    private final ObjectStoreService objectStoreService;
    private final Predicate<Exception> shouldRetryPredicate;
    private final Function<Long, Boolean> pauseUploadSupplier;
    private final BiConsumer<ActionListener<Void>, Long> uploadWhenReady;
    private final VirtualBatchedCompoundCommit virtualBcc;
    private final ShardId shardId;
    private final long generation;
    private final long taskStartMillis;
    private final TimeValue hotThreadsLogInterval;
    private final AtomicBoolean cacheWarmedAttempted = new AtomicBoolean();
    private final List<BccUploadResult.BccUploadAttemptTiming> attemptTimings = new CopyOnWriteArrayList<>();
    private int uploadTryNumber = 0;
    private volatile Scheduler.Cancellable hotThreadsLoggingTask;
    private volatile long attemptStartMillis;

    public BatchedCompoundCommitUploadTask(
        ThreadPool threadPool,
        SharedBlobCacheWarmingService cacheWarmingService,
        ObjectStoreService objectStoreService,
        Predicate<Exception> shouldRetryPredicate,
        Function<Long, Boolean> pauseUploadSupplier,
        BiConsumer<ActionListener<Void>, Long> uploadWhenReady,
        VirtualBatchedCompoundCommit virtualBcc,
        TimeValue initialDelay,
        TimeValue hotThreadsLogInterval,
        ActionListener<BccUploadResult> listener
    ) {
        super(
            logger,
            threadPool,
            initialDelay,
            TimeValue.timeValueSeconds(5),
            TimeValue.timeValueMillis(Long.MAX_VALUE),
            listener,
            threadPool.executor(StatelessPlugin.SHARD_WRITE_THREAD_POOL)
        );
        assert virtualBcc.isFrozen();

        this.threadPool = threadPool;
        this.cacheWarmingService = cacheWarmingService;
        this.objectStoreService = objectStoreService;
        this.shouldRetryPredicate = shouldRetryPredicate;
        this.pauseUploadSupplier = pauseUploadSupplier;
        this.uploadWhenReady = uploadWhenReady;
        this.virtualBcc = virtualBcc;
        this.shardId = virtualBcc.getShardId();
        this.generation = virtualBcc.getPrimaryTermAndGeneration().generation();
        this.taskStartMillis = threadPool.relativeTimeInMillis();
        this.hotThreadsLogInterval = hotThreadsLogInterval;
    }

    // TODO: shouldRetrySupplier, pauseUploadSupplier and uploadWhenReady (in executeUpload)
    // could possibly be refactored into a single lambda, rather than three separate checks/delays on whether to upload, to simplify logic.
    @Override
    public void tryAction(ActionListener<BccUploadResult> listener) {
        ++uploadTryNumber;
        attemptStartMillis = threadPool.relativeTimeInMillis();
        maybeStartHotThreadsLogging();

        // Check whether a shard relocation is in progress. If the relocation hand-off fails, then the state will be set back to RUNNING and
        // the next upload attempt will be allowed through. If the state is transition to CLOSED then we still don't upload and the upload
        // task will not be retried again. We rely on and accept the max retry delay of 5s, i.e., in case of hand-off abort, this upload
        // could be delayed for up to 5 additional seconds.
        if (pauseUploadSupplier.apply(generation)) {
            logger.trace(() -> format("%s skipped upload [%s] to object because of active relocation handoff", shardId, generation));
            listener.onFailure(new IllegalStateException("Upload paused because of relocation handoff"));
        } else {
            executeUpload(listener.delegateResponse((l, e) -> {
                logUploadAttemptFailure(e);
                l.onFailure(e);
            }));
        }
    }

    private void maybeStartHotThreadsLogging() {
        if (hotThreadsLoggingTask != null || hotThreadsLogInterval.millis() <= 0) {
            return;
        }
        hotThreadsLoggingTask = threadPool.scheduleWithFixedDelay(
            () -> HotThreads.logLocalHotThreads(logger, Level.INFO, shardId + " bcc upload ", ReferenceDocs.LOGGING),
            hotThreadsLogInterval,
            threadPool.generic()
        );
    }

    private void cancelHotThreadsLogging() {
        final Scheduler.Cancellable task = hotThreadsLoggingTask;
        if (task != null) {
            task.cancel();
            hotThreadsLoggingTask = null;
        }
    }

    private void logUploadAttemptFailure(Exception e) {
        if (e instanceof AlreadyClosedException) {
            logger.trace(
                () -> format("%s failed attempt to upload commit [%s] to object store because shard closed", shardId, generation),
                e
            );
        } else {
            org.apache.logging.log4j.util.Supplier<Object> messageSupplier = () -> format(
                "%s failed attempt [%s] to upload commit [%s] to object store, will retry",
                shardId,
                uploadTryNumber,
                generation
            );
            if (uploadTryNumber == 5) {
                logger.warn(messageSupplier, e);
            } else {
                logger.info(messageSupplier, e);
            }
        }
    }

    private void executeUpload(ActionListener<BccUploadResult> listener) {
        try {
            ActionListener<Void> uploadListener = listener.delegateFailure((l, v) -> uploadBatchedCompoundCommitFile(l));
            uploadWhenReady.accept(uploadListener, generation);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void uploadBatchedCompoundCommitFile(ActionListener<BccUploadResult> listener) {
        final long generationQueueWaitMs = threadPool.relativeTimeInMillis() - attemptStartMillis;
        final int attempt = uploadTryNumber;
        final long enqueuedAtNanos = threadPool.relativeTimeInNanos();
        try (RefCountingListener refCountingListener = new RefCountingListener(listener.delegateFailureAndWrap((l, unused) -> {
            cancelHotThreadsLogging();
            BatchedCompoundCommit uploadedBcc = virtualBcc.getFrozenBatchedCompoundCommit();
            assert uploadedBcc.lastCompoundCommit() != null;
            final long totalUploadTimeMs = threadPool.relativeTimeInMillis() - taskStartMillis;
            l.onResponse(new BccUploadResult(uploadedBcc, totalUploadTimeMs, List.copyOf(attemptTimings)));
        }))) {
            if (cacheWarmedAttempted.compareAndSet(false, true)) {
                cacheWarmingService.warmCacheBeforeUpload(virtualBcc, refCountingListener.acquire().delegateResponse((l, e) -> {
                    logger.warn(format("%s unexpected error warming cache for commit upload", shardId), e);
                    // A warm failure should not fail the upload
                    l.onResponse(null);
                }));
            }
            objectStoreService.uploadBatchedCompoundCommitFile(
                virtualBcc.getPrimaryTermAndGeneration().primaryTerm(),
                // TODO: The Directory is used to get the blobContainer which can be obtained by using
                // objectStoreService, shardId and primary term. So there is no need to depend on StatelessCommitRef which gets
                // awkward when there are multiple of them.
                // For now we sill use StatelessCommitRef since VBCC can only have a single CC
                virtualBcc.getPendingCompoundCommits().getFirst().getCommitReference().getDirectory(),
                enqueuedAtNanos,
                virtualBcc,
                refCountingListener.acquire().delegateFailure((l, objectStoreTiming) -> {
                    attemptTimings.add(
                        new BccUploadResult.BccUploadAttemptTiming(
                            attempt,
                            generationQueueWaitMs,
                            objectStoreTiming.objectStoreQueueWaitMs(),
                            objectStoreTiming.uploadIoMs(),
                            virtualBcc.getTotalSizeInBytes()
                        )
                    );
                    if (logger.isDebugEnabled()) {
                        final var timing = attemptTimings.getLast();
                        logger.debug(() -> {
                            int uploadedFileCount = 0;
                            long uploadedFileBytes = 0;
                            for (Map.Entry<String, BlobLocation> entry : virtualBcc.getInternalLocations().entrySet()) {
                                uploadedFileCount++;
                                uploadedFileBytes += entry.getValue().fileLength();
                            }
                            return format(
                                "%s commit [%s] uploaded in %s (%s files, %s total bytes)",
                                shardId,
                                virtualBcc.primaryTermAndGeneration(),
                                timing.toLogString(),
                                uploadedFileCount,
                                uploadedFileBytes
                            );
                        });
                    }
                    l.onResponse(null);
                })
            );
        }
    }

    @Override
    public boolean shouldRetry(Exception e) {
        return shouldRetryPredicate.test(e);
    }

    @Override
    public void onFinished() {
        cancelHotThreadsLogging();
        super.onFinished();
    }
}
