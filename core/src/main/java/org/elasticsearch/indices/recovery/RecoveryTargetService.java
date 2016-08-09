/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * The recovery target handles recoveries of peer shards of the shard+node to recover to.
 * <p>
 * Note, it can be safely assumed that there will only be a single recovery per shard (index+id) and
 * not several of them (since we don't allocate several shard replicas to the same node).
 */
public class RecoveryTargetService extends AbstractComponent implements IndexEventListener {

    public static class Actions {
        public static final String FILES_INFO = "internal:index/shard/recovery/filesInfo";
        public static final String FILE_CHUNK = "internal:index/shard/recovery/file_chunk";
        public static final String CLEAN_FILES = "internal:index/shard/recovery/clean_files";
        public static final String TRANSLOG_OPS = "internal:index/shard/recovery/translog_ops";
        public static final String PREPARE_TRANSLOG = "internal:index/shard/recovery/prepare_translog";
        public static final String FINALIZE = "internal:index/shard/recovery/finalize";
        public static final String WAIT_CLUSTERSTATE = "internal:index/shard/recovery/wait_clusterstate";
    }

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

    private final RecoveriesCollection onGoingRecoveries;

    @Inject
    public RecoveryTargetService(Settings settings, ThreadPool threadPool, TransportService transportService, RecoverySettings
            recoverySettings, ClusterService clusterService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
        this.onGoingRecoveries = new RecoveriesCollection(logger, threadPool, this::waitForClusterState);

        transportService.registerRequestHandler(Actions.FILES_INFO, RecoveryFilesInfoRequest::new, ThreadPool.Names.GENERIC, new
                FilesInfoRequestHandler());
        transportService.registerRequestHandler(Actions.FILE_CHUNK, RecoveryFileChunkRequest::new, ThreadPool.Names.GENERIC, new
                FileChunkTransportRequestHandler());
        transportService.registerRequestHandler(Actions.CLEAN_FILES, RecoveryCleanFilesRequest::new, ThreadPool.Names.GENERIC, new
                CleanFilesRequestHandler());
        transportService.registerRequestHandler(Actions.PREPARE_TRANSLOG, RecoveryPrepareForTranslogOperationsRequest::new, ThreadPool
                .Names.GENERIC, new PrepareForTranslogOperationsRequestHandler());
        transportService.registerRequestHandler(Actions.TRANSLOG_OPS, RecoveryTranslogOperationsRequest::new, ThreadPool.Names.GENERIC,
                new TranslogOperationsRequestHandler());
        transportService.registerRequestHandler(Actions.FINALIZE, RecoveryFinalizeRecoveryRequest::new, ThreadPool.Names.GENERIC, new
                FinalizeRecoveryRequestHandler());
        transportService.registerRequestHandler(Actions.WAIT_CLUSTERSTATE, RecoveryWaitForClusterStateRequest::new,
            ThreadPool.Names.GENERIC, new WaitForClusterStateRequestHandler());
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            onGoingRecoveries.cancelRecoveriesForShard(shardId, "shard closed");
        }
    }

    /**
     * cancel all ongoing recoveries for the given shard, if their status match a predicate
     *
     * @param reason       reason for cancellation
     * @param shardId      shardId for which to cancel recoveries
     * @return true if a recovery was cancelled
     */
    public boolean cancelRecoveriesForShard(ShardId shardId, String reason) {
        return onGoingRecoveries.cancelRecoveriesForShard(shardId, reason);
    }

    public void startRecovery(final IndexShard indexShard, final RecoveryState.Type recoveryType, final DiscoveryNode sourceNode, final
    RecoveryListener listener) {
        // create a new recovery status, and process...
        final long recoveryId = onGoingRecoveries.startRecovery(indexShard, sourceNode, listener, recoverySettings.activityTimeout());
        threadPool.generic().execute(new RecoveryRunner(recoveryId));
    }

    protected void retryRecovery(final RecoveryTarget recoveryTarget, final Throwable reason, TimeValue retryAfter, final
    StartRecoveryRequest currentRequest) {
        logger.trace("will retry recovery with id [{}] in [{}]", reason, recoveryTarget.recoveryId(), retryAfter);
        retryRecovery(recoveryTarget, retryAfter, currentRequest);
    }

    protected void retryRecovery(final RecoveryTarget recoveryTarget, final String reason, TimeValue retryAfter, final
    StartRecoveryRequest currentRequest) {
        logger.trace("will retry recovery with id [{}] in [{}] (reason [{}])", recoveryTarget.recoveryId(), retryAfter, reason);
        retryRecovery(recoveryTarget, retryAfter, currentRequest);
    }

    private void retryRecovery(final RecoveryTarget recoveryTarget, TimeValue retryAfter, final StartRecoveryRequest currentRequest) {
        try {
            onGoingRecoveries.resetRecovery(recoveryTarget.recoveryId(), recoveryTarget.shardId());
        } catch (Exception e) {
            onGoingRecoveries.failRecovery(recoveryTarget.recoveryId(), new RecoveryFailedException(currentRequest, e), true);
        }
        threadPool.schedule(retryAfter, ThreadPool.Names.GENERIC, new RecoveryRunner(recoveryTarget.recoveryId()));
    }

    private void doRecovery(final RecoveryTarget recoveryTarget) {
        assert recoveryTarget.sourceNode() != null : "can't do a recovery without a source node";

        logger.trace("collecting local files for {}", recoveryTarget);
        Store.MetadataSnapshot metadataSnapshot = null;
        try {
            if (recoveryTarget.indexShard().indexSettings().isOnSharedFilesystem()) {
                // we are not going to copy any files, so don't bother listing files, potentially running
                // into concurrency issues with the primary changing files underneath us.
                metadataSnapshot = Store.MetadataSnapshot.EMPTY;
            } else {
                metadataSnapshot = recoveryTarget.indexShard().snapshotStoreMetadata();
            }
        } catch (org.apache.lucene.index.IndexNotFoundException e) {
            // happens on an empty folder. no need to log
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        } catch (IOException e) {
            logger.warn("error while listing local files, recover as if there are none", e);
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        } catch (Exception e) {
            // this will be logged as warning later on...
            logger.trace("unexpected error while listing local files, failing recovery", e);
            onGoingRecoveries.failRecovery(recoveryTarget.recoveryId(),
                    new RecoveryFailedException(recoveryTarget.state(), "failed to list local files", e), true);
            return;
        }
        logger.trace("{} local file count: [{}]", recoveryTarget, metadataSnapshot.size());
        final StartRecoveryRequest request = new StartRecoveryRequest(recoveryTarget.shardId(), recoveryTarget.sourceNode(),
                clusterService.localNode(),
                metadataSnapshot, recoveryTarget.state().getType(), recoveryTarget.recoveryId());

        final AtomicReference<RecoveryResponse> responseHolder = new AtomicReference<>();
        try {
            logger.trace("[{}][{}] starting recovery from {}", request.shardId().getIndex().getName(), request.shardId().id(), request
                    .sourceNode());
            recoveryTarget.indexShard().prepareForIndexRecovery();
            recoveryTarget.CancellableThreads().execute(() -> responseHolder.set(
                    transportService.submitRequest(request.sourceNode(), RecoverySource.Actions.START_RECOVERY, request,
                            new FutureTransportResponseHandler<RecoveryResponse>() {
                                @Override
                                public RecoveryResponse newInstance() {
                                    return new RecoveryResponse();
                                }
                            }).txGet()));
            final RecoveryResponse recoveryResponse = responseHolder.get();
            assert responseHolder != null;
            final TimeValue recoveryTime = new TimeValue(recoveryTarget.state().getTimer().time());
            // do this through ongoing recoveries to remove it from the collection
            onGoingRecoveries.markRecoveryAsDone(recoveryTarget.recoveryId());
            if (logger.isTraceEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append('[').append(request.shardId().getIndex().getName()).append(']').append('[').append(request.shardId().id())
                        .append("] ");
                sb.append("recovery completed from ").append(request.sourceNode()).append(", took[").append(recoveryTime).append("]\n");
                sb.append("   phase1: recovered_files [").append(recoveryResponse.phase1FileNames.size()).append("]").append(" with " +
                        "total_size of [").append(new ByteSizeValue(recoveryResponse.phase1TotalSize)).append("]")
                        .append(", took [").append(timeValueMillis(recoveryResponse.phase1Time)).append("], throttling_wait [").append
                        (timeValueMillis(recoveryResponse.phase1ThrottlingWaitTime)).append(']')
                        .append("\n");
                sb.append("         : reusing_files   [").append(recoveryResponse.phase1ExistingFileNames.size()).append("] with " +
                        "total_size of [").append(new ByteSizeValue(recoveryResponse.phase1ExistingTotalSize)).append("]\n");
                sb.append("   phase2: start took [").append(timeValueMillis(recoveryResponse.startTime)).append("]\n");
                sb.append("         : recovered [").append(recoveryResponse.phase2Operations).append("]").append(" transaction log " +
                        "operations")
                        .append(", took [").append(timeValueMillis(recoveryResponse.phase2Time)).append("]")
                        .append("\n");
                logger.trace("{}", sb);
            } else {
                logger.debug("{} recovery done from [{}], took [{}]", request.shardId(), recoveryTarget.sourceNode(), recoveryTime);
            }
        } catch (CancellableThreads.ExecutionCancelledException e) {
            logger.trace("recovery cancelled", e);
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.trace("[{}][{}] Got exception on recovery", e, request.shardId().getIndex().getName(), request.shardId().id());
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof CancellableThreads.ExecutionCancelledException) {
                // this can also come from the source wrapped in a RemoteTransportException
                onGoingRecoveries.failRecovery(recoveryTarget.recoveryId(), new RecoveryFailedException(request, "source has canceled the" +
                        " recovery", cause), false);
                return;
            }
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }
            // do it twice, in case we have double transport exception
            cause = ExceptionsHelper.unwrapCause(cause);
            if (cause instanceof RecoveryEngineException) {
                // unwrap an exception that was thrown as part of the recovery
                cause = cause.getCause();
            }

            // here, we would add checks against exception that need to be retried (and not removeAndClean in this case)

            if (cause instanceof IllegalIndexShardStateException || cause instanceof IndexNotFoundException || cause instanceof
                    ShardNotFoundException) {
                // if the target is not ready yet, retry
                retryRecovery(recoveryTarget, "remote shard not ready", recoverySettings.retryDelayStateSync(), request);
                return;
            }

            if (cause instanceof DelayRecoveryException) {
                retryRecovery(recoveryTarget, cause, recoverySettings.retryDelayStateSync(), request);
                return;
            }

            if (cause instanceof ConnectTransportException) {
                logger.debug("delaying recovery of {} for [{}] due to networking error [{}]", recoveryTarget.shardId(), recoverySettings
                        .retryDelayNetwork(), cause.getMessage());
                retryRecovery(recoveryTarget, cause.getMessage(), recoverySettings.retryDelayNetwork(), request);
                return;
            }

            if (cause instanceof AlreadyClosedException) {
                onGoingRecoveries.failRecovery(recoveryTarget.recoveryId(), new RecoveryFailedException(request, "source shard is " +
                        "closed", cause), false);
                return;
            }
            onGoingRecoveries.failRecovery(recoveryTarget.recoveryId(), new RecoveryFailedException(request, e), true);
        }
    }

    public interface RecoveryListener {
        void onRecoveryDone(RecoveryState state);

        void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure);
    }

    class PrepareForTranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryPrepareForTranslogOperationsRequest> {

        @Override
        public void messageReceived(RecoveryPrepareForTranslogOperationsRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId()
            )) {
                recoveryRef.status().prepareForTranslogOperations(request.totalTranslogOps());
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class FinalizeRecoveryRequestHandler implements TransportRequestHandler<RecoveryFinalizeRecoveryRequest> {

        @Override
        public void messageReceived(RecoveryFinalizeRecoveryRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId()
            )) {
                recoveryRef.status().finalizeRecovery();
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class WaitForClusterStateRequestHandler implements TransportRequestHandler<RecoveryWaitForClusterStateRequest> {

        @Override
        public void messageReceived(RecoveryWaitForClusterStateRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId()
            )) {
                recoveryRef.status().ensureClusterStateVersion(request.clusterStateVersion());
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class TranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryTranslogOperationsRequest> {

        @Override
        public void messageReceived(final RecoveryTranslogOperationsRequest request, final TransportChannel channel) throws IOException {
            try (RecoveriesCollection.RecoveryRef recoveryRef =
                         onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
                final RecoveryTarget recoveryTarget = recoveryRef.status();
                try {
                    recoveryTarget.indexTranslogOperations(request.operations(), request.totalTranslogOps());
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } catch (TranslogRecoveryPerformer.BatchOperationException exception) {
                    MapperException mapperException = (MapperException) ExceptionsHelper.unwrap(exception, MapperException.class);
                    if (mapperException == null) {
                        throw exception;
                    }
                    // in very rare cases a translog replay from primary is processed before a mapping update on this node
                    // which causes local mapping changes since the mapping (clusterstate) might not have arrived on this node.
                    // we want to wait until these mappings are processed but also need to do some maintenance and roll back the
                    // number of processed (completed) operations in this batch to ensure accounting is correct.
                    logger.trace("delaying recovery due to missing mapping changes (rolling back stats for [{}] ops)", exception, exception
                            .completedOperations());
                    final RecoveryState.Translog translog = recoveryTarget.state().getTranslog();
                    translog.decrementRecoveredOperations(exception.completedOperations()); // do the maintainance and rollback competed ops
                    // we do not need to use a timeout here since the entire recovery mechanism has an inactivity protection (it will be
                    // canceled)
                    observer.waitForNextChange(new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            try {
                                messageReceived(request, channel);
                            } catch (Exception e) {
                                onFailure(e);
                            }
                        }

                        protected void onFailure(Exception e) {
                            try {
                                channel.sendResponse(e);
                            } catch (IOException e1) {
                                logger.warn("failed to send error back to recovery source", e1);
                            }
                        }

                        @Override
                        public void onClusterServiceClose() {
                            onFailure(new ElasticsearchException("cluster service was closed while waiting for mapping updates"));
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            // note that we do not use a timeout (see comment above)
                            onFailure(new ElasticsearchTimeoutException("timed out waiting for mapping updates (timeout [" + timeout +
                                    "])"));
                        }
                    });
                }
            }
        }
    }

    private void waitForClusterState(long clusterStateVersion) {
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, TimeValue.timeValueMinutes(5), logger,
            threadPool.getThreadContext());
        final ClusterState clusterState = observer.observedState();
        if (clusterState.getVersion() >= clusterStateVersion) {
            logger.trace("node has cluster state with version higher than {} (current: {})", clusterStateVersion,
                clusterState.getVersion());
            return;
        } else {
            logger.trace("waiting for cluster state version {} (current: {})", clusterStateVersion, clusterState.getVersion());
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {

                @Override
                public void onNewClusterState(ClusterState state) {
                    future.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    future.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    future.onFailure(new IllegalStateException("cluster state never updated to version " + clusterStateVersion));
                }
            }, new ClusterStateObserver.ValidationPredicate() {

                @Override
                protected boolean validate(ClusterState newState) {
                    return newState.getVersion() >= clusterStateVersion;
                }
            });
            try {
                future.get();
                logger.trace("successfully waited for cluster state with version {} (current: {})", clusterStateVersion,
                    observer.observedState().getVersion());
            } catch (Exception e) {
                logger.debug("failed waiting for cluster state with version {} (current: {})", e, clusterStateVersion,
                    observer.observedState());
                throw ExceptionsHelper.convertToRuntime(e);
            }
        }
    }

    class FilesInfoRequestHandler implements TransportRequestHandler<RecoveryFilesInfoRequest> {

        @Override
        public void messageReceived(RecoveryFilesInfoRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId()
            )) {
                recoveryRef.status().receiveFileInfo(request.phase1FileNames, request.phase1FileSizes, request.phase1ExistingFileNames,
                        request.phase1ExistingFileSizes, request.totalTranslogOps);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    class CleanFilesRequestHandler implements TransportRequestHandler<RecoveryCleanFilesRequest> {

        @Override
        public void messageReceived(RecoveryCleanFilesRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId()
            )) {
                recoveryRef.status().cleanFiles(request.totalTranslogOps(), request.sourceMetaSnapshot());
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    class FileChunkTransportRequestHandler implements TransportRequestHandler<RecoveryFileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final RecoveryFileChunkRequest request, TransportChannel channel) throws Exception {
            try (RecoveriesCollection.RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId()
            )) {
                final RecoveryTarget status = recoveryRef.status();
                final RecoveryState.Index indexState = status.state().getIndex();
                if (request.sourceThrottleTimeInNanos() != RecoveryState.Index.UNKNOWN) {
                    indexState.addSourceThrottling(request.sourceThrottleTimeInNanos());
                }

                RateLimiter rateLimiter = recoverySettings.rateLimiter();
                if (rateLimiter != null) {
                    long bytes = bytesSinceLastPause.addAndGet(request.content().length());
                    if (bytes > rateLimiter.getMinPauseCheckBytes()) {
                        // Time to pause
                        bytesSinceLastPause.addAndGet(-bytes);
                        long throttleTimeInNanos = rateLimiter.pause(bytes);
                        indexState.addTargetThrottling(throttleTimeInNanos);
                        status.indexShard().recoveryStats().addThrottleTime(throttleTimeInNanos);
                    }
                }

                status.writeFileChunk(request.metadata(), request.position(), request.content(),
                        request.lastChunk(), request.totalTranslogOps()
                );
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class RecoveryRunner extends AbstractRunnable {

        final long recoveryId;

        RecoveryRunner(long recoveryId) {
            this.recoveryId = recoveryId;
        }

        @Override
        public void onFailure(Exception e) {
            try (RecoveriesCollection.RecoveryRef recoveryRef = onGoingRecoveries.getRecovery(recoveryId)) {
                if (recoveryRef != null) {
                    logger.error("unexpected error during recovery [{}], failing shard", e, recoveryId);
                    onGoingRecoveries.failRecovery(recoveryId,
                            new RecoveryFailedException(recoveryRef.status().state(), "unexpected error", e),
                            true // be safe
                    );
                } else {
                    logger.debug("unexpected error during recovery, but recovery id [{}] is finished", e, recoveryId);
                }
            }
        }

        @Override
        public void doRun() {
            RecoveriesCollection.RecoveryRef recoveryRef = onGoingRecoveries.getRecovery(recoveryId);
            if (recoveryRef == null) {
                logger.trace("not running recovery with id [{}] - can't find it (probably finished)", recoveryId);
                return;
            }
            try {
                doRecovery(recoveryRef.status());
            } finally {
                recoveryRef.close();
            }
        }
    }

}
