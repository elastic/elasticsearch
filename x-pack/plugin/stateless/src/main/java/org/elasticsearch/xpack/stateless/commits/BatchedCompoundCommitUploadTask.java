/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

/**
 * Runs a task to upload a new commit to the blob store. Handles errors and retrying by implementing the {@link RetryableAction} interface.
 */
public class BatchedCompoundCommitUploadTask extends RetryableAction<BatchedCompoundCommit> {

    private static final Logger logger = LogManager.getLogger(BatchedCompoundCommitUploadTask.class);

    private final ThreadPool threadPool;
    private final SharedBlobCacheWarmingService cacheWarmingService;
    private final ObjectStoreService objectStoreService;
    private final BooleanSupplier shouldRetrySupplier;
    private final Function<Long, Boolean> pauseUploadSupplier;
    private final TriConsumer<ActionListener<Void>, ActionListener<BatchedCompoundCommit>, Long> uploadWhenReady;
    private final VirtualBatchedCompoundCommit virtualBcc;
    private final ShardId shardId;
    private final long generation;
    private final long startNanos;
    private final AtomicBoolean cacheWarmedAttempted = new AtomicBoolean();
    private int uploadTryNumber = 0;

    public BatchedCompoundCommitUploadTask(
        ThreadPool threadPool,
        SharedBlobCacheWarmingService cacheWarmingService,
        ObjectStoreService objectStoreService,
        BooleanSupplier shouldRetrySupplier,
        Function<Long, Boolean> pauseUploadSupplier,
        TriConsumer<ActionListener<Void>, ActionListener<BatchedCompoundCommit>, Long> uploadWhenReady,
        VirtualBatchedCompoundCommit virtualBcc,
        TimeValue initialDelay,
        ActionListener<BatchedCompoundCommit> listener
    ) {
        super(
            logger,
            threadPool,
            initialDelay,
            TimeValue.timeValueSeconds(5),
            TimeValue.timeValueMillis(Long.MAX_VALUE),
            listener,
            threadPool.executor(Stateless.SHARD_WRITE_THREAD_POOL)
        );
        assert virtualBcc.isFrozen();

        this.threadPool = threadPool;
        this.cacheWarmingService = cacheWarmingService;
        this.objectStoreService = objectStoreService;
        this.shouldRetrySupplier = shouldRetrySupplier;
        this.pauseUploadSupplier = pauseUploadSupplier;
        this.uploadWhenReady = uploadWhenReady;
        this.virtualBcc = virtualBcc;
        this.shardId = virtualBcc.getShardId();
        this.generation = virtualBcc.getPrimaryTermAndGeneration().generation();
        this.startNanos = threadPool.relativeTimeInNanos();
    }

    // TODO: shouldRetrySupplier, pauseUploadSupplier and uploadWhenReady (in executeUpload)
    // could possibly be refactored into a single lambda, rather than three separate checks/delays on whether to upload, to simplify logic.
    @Override
    public void tryAction(ActionListener<BatchedCompoundCommit> listener) {
        ++uploadTryNumber;

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

    private void executeUpload(ActionListener<BatchedCompoundCommit> listener) {
        try {
            ActionListener<Void> uploadReadyListener = listener.delegateFailure((l, v) -> uploadBatchedCompoundCommitFile(l));
            uploadWhenReady.apply(uploadReadyListener, listener, generation);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void uploadBatchedCompoundCommitFile(ActionListener<BatchedCompoundCommit> listener) {
        try (RefCountingListener refCountingListener = new RefCountingListener(listener.delegateFailureAndWrap((l, unused) -> {
            BatchedCompoundCommit uploadedBcc = virtualBcc.getFrozenBatchedCompoundCommit();
            assert uploadedBcc.lastCompoundCommit() != null;
            l.onResponse(uploadedBcc);
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
                virtualBcc.getPendingCompoundCommits().get(0).getCommitReference().getDirectory(),
                startNanos,
                virtualBcc,
                refCountingListener.acquire().delegateFailure((l, uploadedBcc) -> {
                    logger.debug(() -> {
                        final long end = threadPool.relativeTimeInNanos();
                        int uploadedFileCount = 0;
                        long uploadedFileBytes = 0;
                        for (Map.Entry<String, BlobLocation> entry : virtualBcc.getInternalLocations().entrySet()) {
                            uploadedFileCount++;
                            uploadedFileBytes += entry.getValue().fileLength();
                        }
                        return format(
                            "%s commit [%s] uploaded in [%s] ms (%s files, %s total bytes)",
                            shardId,
                            virtualBcc.primaryTermAndGeneration(),
                            TimeValue.nsecToMSec(end - startNanos),
                            uploadedFileCount,
                            uploadedFileBytes
                        );
                    });
                    l.onResponse(null);
                })
            );
        }
    }

    @Override
    public boolean shouldRetry(Exception e) {
        return shouldRetrySupplier.getAsBoolean();
    }
}
