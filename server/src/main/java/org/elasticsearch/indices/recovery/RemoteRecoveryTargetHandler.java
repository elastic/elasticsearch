/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class RemoteRecoveryTargetHandler implements RecoveryTargetHandler {

    private static final Logger logger = LogManager.getLogger(RemoteRecoveryTargetHandler.class);

    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final long recoveryId;
    private final ShardId shardId;
    private final DiscoveryNode targetNode;
    private final RecoverySettings recoverySettings;
    private final Map<Object, RetryableAction<?>> onGoingRetryableActions = ConcurrentCollections.newConcurrentMap();

    private final TransportRequestOptions translogOpsRequestOptions;
    private final TransportRequestOptions fileChunkRequestOptions;
    private final TransportRequestOptions standardTimeoutRequestOptions;

    private final AtomicLong bytesSinceLastPause = new AtomicLong();
    private final AtomicLong requestSeqNoGenerator = new AtomicLong(0);

    private final Consumer<Long> onSourceThrottle;
    private final Task task;
    private volatile boolean isCancelled = false;

    public RemoteRecoveryTargetHandler(
        long recoveryId,
        ShardId shardId,
        TransportService transportService,
        DiscoveryNode targetNode,
        RecoverySettings recoverySettings,
        Consumer<Long> onSourceThrottle,
        Task task
    ) {
        this.transportService = transportService;
        this.threadPool = transportService.getThreadPool();
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
        this.onSourceThrottle = onSourceThrottle;
        this.translogOpsRequestOptions = TransportRequestOptions.of(
            recoverySettings.internalActionLongTimeout(),
            TransportRequestOptions.Type.RECOVERY
        );
        this.fileChunkRequestOptions = TransportRequestOptions.of(
            recoverySettings.internalActionTimeout(),
            TransportRequestOptions.Type.RECOVERY
        );
        this.standardTimeoutRequestOptions = TransportRequestOptions.timeout(recoverySettings.internalActionTimeout());
        this.task = task;
    }

    public DiscoveryNode targetNode() {
        return targetNode;
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryPrepareForTranslogOperationsRequest request = new RecoveryPrepareForTranslogOperationsRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            totalTranslogOps
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        executeRetryableAction(action, request, standardTimeoutRequestOptions, listener.map(r -> null), reader);
    }

    @Override
    public void finalizeRecovery(final long globalCheckpoint, final long trimAboveSeqNo, final ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.FINALIZE;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryFinalizeRecoveryRequest request = new RecoveryFinalizeRecoveryRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            globalCheckpoint,
            trimAboveSeqNo
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        executeRetryableAction(
            action,
            request,
            TransportRequestOptions.timeout(recoverySettings.internalActionLongTimeout()),
            listener.map(r -> null),
            reader
        );
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext, ActionListener<Void> listener) {
        transportService.sendRequest(
            targetNode,
            PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT,
            new RecoveryHandoffPrimaryContextRequest(recoveryId, shardId, primaryContext),
            standardTimeoutRequestOptions,
            new ActionListenerResponseHandler<>(listener.map(r -> null), in -> TransportResponse.Empty.INSTANCE, ThreadPool.Names.GENERIC)
        );
    }

    @Override
    public void indexTranslogOperations(
        final List<Translog.Operation> operations,
        final int totalTranslogOps,
        final long maxSeenAutoIdTimestampOnPrimary,
        final long maxSeqNoOfDeletesOrUpdatesOnPrimary,
        final RetentionLeases retentionLeases,
        final long mappingVersionOnPrimary,
        final ActionListener<Long> listener
    ) {
        final String action = PeerRecoveryTargetService.Actions.TRANSLOG_OPS;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryTranslogOperationsRequest request = new RecoveryTranslogOperationsRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            operations,
            totalTranslogOps,
            maxSeenAutoIdTimestampOnPrimary,
            maxSeqNoOfDeletesOrUpdatesOnPrimary,
            retentionLeases,
            mappingVersionOnPrimary
        );
        final Writeable.Reader<RecoveryTranslogOperationsResponse> reader = RecoveryTranslogOperationsResponse::new;
        executeRetryableAction(action, request, translogOpsRequestOptions, listener.map(r -> r.localCheckpoint), reader);
    }

    @Override
    public void receiveFileInfo(
        List<String> phase1FileNames,
        List<Long> phase1FileSizes,
        List<String> phase1ExistingFileNames,
        List<Long> phase1ExistingFileSizes,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        final String action = PeerRecoveryTargetService.Actions.FILES_INFO;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        RecoveryFilesInfoRequest request = new RecoveryFilesInfoRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            phase1FileNames,
            phase1FileSizes,
            phase1ExistingFileNames,
            phase1ExistingFileSizes,
            totalTranslogOps
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        executeRetryableAction(action, request, standardTimeoutRequestOptions, listener.map(r -> null), reader);
    }

    @Override
    public void cleanFiles(
        int totalTranslogOps,
        long globalCheckpoint,
        Store.MetadataSnapshot sourceMetadata,
        ActionListener<Void> listener
    ) {
        final String action = PeerRecoveryTargetService.Actions.CLEAN_FILES;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryCleanFilesRequest request = new RecoveryCleanFilesRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            sourceMetadata,
            totalTranslogOps,
            globalCheckpoint
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = listener.map(r -> null);
        executeRetryableAction(action, request, TransportRequestOptions.EMPTY, responseListener, reader);
    }

    @Override
    public void restoreFileFromSnapshot(
        String repository,
        IndexId indexId,
        BlobStoreIndexShardSnapshot.FileInfo snapshotFile,
        ActionListener<Void> listener
    ) {
        final String action = PeerRecoveryTargetService.Actions.RESTORE_FILE_FROM_SNAPSHOT;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoverySnapshotFileRequest request = new RecoverySnapshotFileRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            repository,
            indexId,
            snapshotFile
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = listener.map(r -> null);
        executeRetryableAction(action, request, TransportRequestOptions.EMPTY, responseListener, reader);
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        ReleasableBytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        // Pause using the rate limiter, if desired, to throttle the recovery
        final long throttleTimeInNanos;
        // always fetch the ratelimiter - it might be updated in real-time on the recovery settings
        final RateLimiter rl = recoverySettings.rateLimiter();
        if (rl != null) {
            long bytes = bytesSinceLastPause.addAndGet(content.length());
            if (bytes > rl.getMinPauseCheckBytes()) {
                // Time to pause
                bytesSinceLastPause.addAndGet(-bytes);
                try {
                    throttleTimeInNanos = rl.pause(bytes);
                    onSourceThrottle.accept(throttleTimeInNanos);
                } catch (IOException e) {
                    throw new ElasticsearchException("failed to pause recovery", e);
                }
            } else {
                throttleTimeInNanos = 0;
            }
        } else {
            throttleTimeInNanos = 0;
        }

        final String action = PeerRecoveryTargetService.Actions.FILE_CHUNK;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        /* we send estimateTotalOperations with every request since we collect stats on the target and that way we can
         * see how many translog ops we accumulate while copying files across the network. A future optimization
         * would be in to restart file copy again (new deltas) if we have too many translog ops are piling up.
         */
        final RecoveryFileChunkRequest request = new RecoveryFileChunkRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            fileMetadata,
            position,
            content,
            lastChunk,
            totalTranslogOps,
            throttleTimeInNanos
        );

        // Fork the actual sending onto a separate thread so we can send them concurrently even if CPU-bound (e.g. using compression).
        // The AsyncIOProcessor and MultiFileWriter both concentrate their work onto fewer threads if possible, but once we have
        // chunks to send we want to increase parallelism again.
        threadPool.generic()
            .execute(
                ActionRunnable.wrap(
                    ActionListener.<TransportResponse.Empty>runBefore(listener.map(r -> null), request::decRef),
                    l -> executeRetryableAction(action, request, fileChunkRequestOptions, l, in -> TransportResponse.Empty.INSTANCE)
                )
            );
    }

    @Override
    public void cancel() {
        isCancelled = true;
        if (onGoingRetryableActions.isEmpty()) {
            return;
        }
        final RuntimeException exception = new CancellableThreads.ExecutionCancelledException("recovery was cancelled");
        // Dispatch to generic as cancellation calls can come on the cluster state applier thread
        threadPool.generic().execute(() -> {
            for (RetryableAction<?> action : onGoingRetryableActions.values()) {
                action.cancel(exception);
            }
            onGoingRetryableActions.clear();
        });
    }

    private <T extends TransportResponse> void executeRetryableAction(
        String action,
        RecoveryTransportRequest request,
        TransportRequestOptions options,
        ActionListener<T> actionListener,
        Writeable.Reader<T> reader
    ) {
        final Object key = new Object();
        final ActionListener<T> removeListener = ActionListener.runBefore(actionListener, () -> onGoingRetryableActions.remove(key));
        final TimeValue initialDelay = TimeValue.timeValueMillis(200);
        final TimeValue timeout = recoverySettings.internalActionRetryTimeout();
        final RetryableAction<T> retryableAction = new RetryableAction<>(logger, threadPool, initialDelay, timeout, removeListener) {

            @Override
            public void tryAction(ActionListener<T> listener) {
                if (request.tryIncRef()) {
                    transportService.sendChildRequest(
                        targetNode,
                        action,
                        request,
                        task,
                        options,
                        new ActionListenerResponseHandler<>(
                            ActionListener.runBefore(listener, request::decRef),
                            reader,
                            ThreadPool.Names.GENERIC
                        )
                    );
                } else {
                    listener.onFailure(new AlreadyClosedException("already closed"));
                }
            }

            @Override
            public boolean shouldRetry(Exception e) {
                return retryableException(e);
            }
        };
        onGoingRetryableActions.put(key, retryableAction);
        retryableAction.run();
        if (isCancelled) {
            retryableAction.cancel(new CancellableThreads.ExecutionCancelledException("recovery was cancelled"));
        }
    }

    private static boolean retryableException(Exception e) {
        if (e instanceof ConnectTransportException) {
            return true;
        } else if (e instanceof SendRequestTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof ConnectTransportException;
        } else if (e instanceof RemoteTransportException) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException || cause instanceof EsRejectedExecutionException;
        }
        return false;
    }
}
