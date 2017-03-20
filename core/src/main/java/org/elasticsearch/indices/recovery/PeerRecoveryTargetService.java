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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveriesCollection.RecoveryRef;
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

/**
 * The recovery target handles recoveries of peer shards of the shard+node to recover to.
 * <p>
 * Note, it can be safely assumed that there will only be a single recovery per shard (index+id) and
 * not several of them (since we don't allocate several shard replicas to the same node).
 */
public class PeerRecoveryTargetService extends AbstractComponent implements IndexEventListener {

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

    public PeerRecoveryTargetService(Settings settings, ThreadPool threadPool, TransportService transportService, RecoverySettings
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
     * Cancel all ongoing recoveries for the given shard.
     *
     * @param reason  reason for cancellation
     * @param shardId shard ID for which to cancel recoveries
     * @return {@code true} if a recovery was cancelled
     */
    public boolean cancelRecoveriesForShard(ShardId shardId, String reason) {
        return onGoingRecoveries.cancelRecoveriesForShard(shardId, reason);
    }

    public void startRecovery(final IndexShard indexShard, final DiscoveryNode sourceNode,
                              final RecoveryListener listener) {
        if (sourceNode.getVersion().before(Version.V_6_0_0_alpha1_UNRELEASED)) {
            startLegacyRecovery(indexShard, sourceNode, listener);
            return;
        }
        if (indexShard.indexSettings().isOnSharedFilesystem()) {
            startRecovery(onGoingRecoveries.startFileRecovery(indexShard, sourceNode, listener,
                recoverySettings.activityTimeout()));
            return;
        }
        // nocommit check cancellation race
        final RecoveryListener originalListener = listener;
        final RecoveryListener primaryHandoffIfNeeded;
        if (indexShard.routingEntry().primary()) {
            assert indexShard.routingEntry().relocating() : indexShard.routingEntry();
            primaryHandoffIfNeeded = wrapWithRecoveryStep(indexShard, sourceNode, originalListener,
                recoverySettings.activityTimeout(), onGoingRecoveries::startPrimaryHandoff);
        } else {
            primaryHandoffIfNeeded = originalListener;
        }

        final RecoveryListener fullRecovery;
        final ShardId shardId = indexShard.shardId();
        {
            RecoveryListener nextListener = primaryHandoffIfNeeded;

            nextListener = wrapWithRecoveryStep(indexShard, sourceNode, nextListener,
                recoverySettings.activityTimeout(), onGoingRecoveries::startFileRecovery);

            nextListener = wrapWithRunnable(indexShard::prepareForIndexRecovery,
                "prepare for index recovery", shardId, nextListener);

            fullRecovery = nextListener;
        }
        final RecoveryListener startListener;
        if (shouldTryOpsRecovery(indexShard)) {
            RecoveryListener seqNoListener = wrapWithRecoveryStep(indexShard, sourceNode,
                new RecoveryListener() {
                    @Override
                    public void onRecoveryDone(RecoveryState state) {
                        primaryHandoffIfNeeded.onRecoveryDone(state);
                    }

                    @Override
                    public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e,
                                                  boolean sendShardFailure) {
                        try {
                            logger.debug(shardId + " falling back to full file recovery", e);
                            indexShard.performRecoveryRestart();
                            fullRecovery.onRecoveryDone(state);
                        } catch (Exception e1) {
                            e1.addSuppressed(e);
                            originalListener.onRecoveryFailure(state,
                                new RecoveryFailedException(state,
                                    "failed to fall back to full file recovery", e1),
                                true);
                        }
                    }
                }, recoverySettings.activityTimeout(), onGoingRecoveries::startOpsRecovery);
            startListener = wrapWithRunnable(() -> {
                // nocommit: remove auto gen timestamp
                indexShard.prepareForIndexRecovery();
                indexShard.skipTranslogRecovery(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP);
            }, "preparing for seq no recovery", shardId, seqNoListener);
        } else {
            startListener = fullRecovery;
        }
        startListener.onRecoveryDone(indexShard.recoveryState());
    }

    private void startLegacyRecovery(IndexShard indexShard, DiscoveryNode sourceNode,
                                     RecoveryListener listener) {
        startRecovery(onGoingRecoveries.startLegacyRecovery(indexShard, sourceNode, listener,
            recoverySettings.activityTimeout()));
    }

    private interface RecoveryTargetSupplier {
        long startRecovery(IndexShard shard, DiscoveryNode sourceNode, RecoveryListener listener,
                           TimeValue activityTimeOut);
    }

    private interface IORunnable {
        void run() throws IOException;
    }

    private RecoveryListener wrapWithRecoveryStep(final IndexShard shard,
                                                  final DiscoveryNode sourceNode,
                                                  final RecoveryListener listener,
                                                  final TimeValue activityTimeOut,
                                                  final RecoveryTargetSupplier targetSupplier) {
        return new RecoveryListener() {
            @Override
            public void onRecoveryDone(RecoveryState state) {
                long recoveryId =
                    targetSupplier.startRecovery(shard, sourceNode, listener, activityTimeOut);
                startRecovery(recoveryId);
            }

            @Override
            public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e,
                                          boolean sendShardFailure) {
                listener.onRecoveryFailure(state, e, sendShardFailure);
            }
        };
    }

    private void startRecovery(long recoveryId) {
        threadPool.generic().execute(new RecoveryRunner(recoveryId));
    }

    private RecoveryListener wrapWithRunnable(IORunnable runnable,
                                              String name, ShardId shardId,
                                              RecoveryListener listener) {
        return new RecoveryListener() {
            @Override
            public void onRecoveryDone(RecoveryState state) {
                boolean success = false;
                try {
                    logger.trace("{} {}", shardId, name);
                    runnable.run();
                    success = true;
                } catch (Exception e) {
                    listener.onRecoveryFailure(state,
                        new RecoveryFailedException(state, "unexpected error while [" + name + "]",
                            e), true);
                }
                if (success) {
                    listener.onRecoveryDone(state);
                }
            }

            @Override
            public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e,
                                          boolean sendShardFailure) {
                listener.onRecoveryFailure(state, e, sendShardFailure);
            }
        };
    }

    protected void retryRecovery(final long recoveryId, final Throwable reason, TimeValue retryAfter, TimeValue activityTimeout) {
        logger.trace(
            (Supplier<?>) () -> new ParameterizedMessage(
                "will retry recovery with id [{}] in [{}]", recoveryId, retryAfter), reason);
        retryRecovery(recoveryId, retryAfter, activityTimeout);
    }

    protected void retryRecovery(final long recoveryId, final String reason, TimeValue retryAfter, TimeValue activityTimeout) {
        logger.trace("will retry recovery with id [{}] in [{}] (reason [{}])", recoveryId, retryAfter, reason);
        retryRecovery(recoveryId, retryAfter, activityTimeout);
    }

    private void retryRecovery(final long recoveryId, final TimeValue retryAfter, final TimeValue activityTimeout) {
        RecoveryTarget newTarget = onGoingRecoveries.resetRecovery(recoveryId, activityTimeout);
        if (newTarget != null) {
            threadPool.schedule(retryAfter, ThreadPool.Names.GENERIC, new RecoveryRunner(newTarget.recoveryId()));
        }
    }

    private void doRecovery(final long recoveryId) {
        final StartRecoveryRequest request;
        final CancellableThreads cancellableThreads;
        final RecoveryState.Timer timer;
        final String startRecoveryAction;
        final Supplier<RecoveryResponse> responseSupplier;

        try (RecoveryRef recoveryRef = onGoingRecoveries.getRecovery(recoveryId)) {
            if (recoveryRef == null) {
                logger.trace("not running recovery with id [{}] - can not find it (probably finished)", recoveryId);
                return;
            }
            final RecoveryTarget recoveryTarget = recoveryRef.target();
            cancellableThreads = recoveryTarget.cancellableThreads();
            timer = recoveryTarget.state().getTimer();
            try {
                assert recoveryTarget.sourceNode() != null :
                    "can not do a recovery without a source node";
                request = recoveryTarget.createStartRecoveryRequest(logger,
                    clusterService.localNode());
                startRecoveryAction = recoveryTarget.startRecoveryActionName();
                responseSupplier = recoveryTarget::createRecoveryResponse;
            } catch (final Exception e) {
                // this will be logged as warning later on...
                logger.trace("unexpected error while preparing shard for peer recovery, failing recovery", e);
                onGoingRecoveries.failRecovery(recoveryId,
                    new RecoveryFailedException(recoveryTarget.state(), "failed to prepare shard for recovery", e), true);
                return;
            }
        }

        try {
            logger.trace("{} starting recovery from {}", request.shardId(), request.sourceNode());
            final AtomicReference<RecoveryResponse> responseHolder = new AtomicReference<>();
            cancellableThreads.execute(() -> responseHolder.set(
                    transportService.submitRequest(request.sourceNode(), startRecoveryAction,
                        request, new FutureTransportResponseHandler<RecoveryResponse>() {
                                @Override
                                public RecoveryResponse newInstance() {
                                    return responseSupplier.get();
                                }
                            }).txGet()));
            final RecoveryResponse recoveryResponse = responseHolder.get();
            final TimeValue recoveryTime = new TimeValue(timer.time());
            // do this through ongoing recoveries to remove it from the collection
            onGoingRecoveries.markRecoveryAsDone(recoveryId);
            if (logger.isTraceEnabled()) {
                recoveryResponse.logTraceSummary(logger, request, recoveryTime);
            } else {
                logger.debug("{} recovery done from [{}], took [{}]", request.shardId(), request.sourceNode(), recoveryTime);
            }
        } catch (CancellableThreads.ExecutionCancelledException e) {
            logger.trace("recovery cancelled", e);
        } catch (Exception e) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "[{}][{}] Got exception on recovery",
                        request.shardId().getIndex().getName(),
                        request.shardId().id()),
                    e);
            }
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof CancellableThreads.ExecutionCancelledException) {
                // this can also come from the source wrapped in a RemoteTransportException
                onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request,
                    "source has canceled the recovery", cause), false);
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

            if (cause instanceof IllegalIndexShardStateException || cause instanceof IndexNotFoundException ||
                cause instanceof ShardNotFoundException) {
                // if the target is not ready yet, retry
                retryRecovery(
                    recoveryId,
                    "remote shard not ready",
                    recoverySettings.retryDelayStateSync(),
                    recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof DelayRecoveryException) {
                retryRecovery(recoveryId, cause, recoverySettings.retryDelayStateSync(), recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof ConnectTransportException) {
                logger.debug("delaying recovery of {} for [{}] due to networking error [{}]", request.shardId(),
                    recoverySettings.retryDelayNetwork(), cause.getMessage());
                retryRecovery(recoveryId, cause.getMessage(), recoverySettings.retryDelayNetwork(), recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof AlreadyClosedException) {
                onGoingRecoveries.failRecovery(recoveryId,
                    new RecoveryFailedException(request, "source shard is closed", cause), false);
                return;
            }

            onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request, e), true);
        }
    }

    /**
     *
     */
    public static boolean shouldTryOpsRecovery(final IndexShard targetShard) {
        try {
            final long globalCheckpoint =
                Translog.readGlobalCheckpoint(targetShard.shardPath().resolveTranslog());
            final SeqNoStats seqNoStats = targetShard.store().loadSeqNoStats(globalCheckpoint);
            if (seqNoStats.getMaxSeqNo() <= seqNoStats.getGlobalCheckpoint()) {
                // commit point is good for seq no based recovery as the maximum seq# including in
                // it is below the global checkpoint (i.e., it excludes any ops thay may not be
                // on the primary) Recovery will start at the first op after the local check
                // point stored in the commit.
                return true;
            } else {
                return false;
            }
        } catch (final IOException e) {
            // this can happen, for example, if a phase one of the recovery completed successfully,
            // a network partition happens before the translog on the recovery target is opened,
            // the recovery enters a retry loop seeing now that the index files are on disk and
            // proceeds to attempt a sequence-number-based recovery
            return false;
        }
    }

    public interface RecoveryListener {
        void onRecoveryDone(RecoveryState state);

        void onRecoveryFailure(RecoveryState state, RecoveryFailedException e,
                               boolean sendShardFailure);
    }

    class PrepareForTranslogOperationsRequestHandler implements
        TransportRequestHandler<RecoveryPrepareForTranslogOperationsRequest> {

        @Override
        public void messageReceived(RecoveryPrepareForTranslogOperationsRequest request,
                                    TransportChannel channel) throws Exception {
            try (RecoveryRef recoveryRef =
                     onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final FileRecoveryTargetHandler target = recoveryRef.target();
                target.prepareForTranslogOperations(
                    request.totalTranslogOps(), request.getMaxUnsafeAutoIdTimestamp());
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class FinalizeRecoveryRequestHandler
        implements TransportRequestHandler<RecoveryFinalizeRecoveryRequest> {

        @Override
        public void messageReceived(RecoveryFinalizeRecoveryRequest request,
                                    TransportChannel channel) throws Exception {
            try (RecoveryRef recoveryRef =
                     onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final OpsRecoveryTargetHandler target = recoveryRef.target();
                target.finalizeRecovery(request.globalCheckpoint());
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class WaitForClusterStateRequestHandler
        implements TransportRequestHandler<RecoveryWaitForClusterStateRequest> {

        @Override
        public void messageReceived(RecoveryWaitForClusterStateRequest request,
                                    TransportChannel channel) throws Exception {
            try (RecoveryRef recoveryRef =
                     onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final PrimaryHandoffRecoveryTargetHandler target = recoveryRef.target();
                target.ensureClusterStateVersion(request.clusterStateVersion());
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class TranslogOperationsRequestHandler
        implements TransportRequestHandler<RecoveryTranslogOperationsRequest> {

        @Override
        public void messageReceived(final RecoveryTranslogOperationsRequest request,
                                    final TransportChannel channel) throws IOException {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(),
                request.shardId())) {
                final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null,
                    logger, threadPool.getThreadContext());
                final OpsRecoveryTargetHandler recoveryTarget = recoveryRef.target();
                try {
                    recoveryTarget.indexTranslogOperations(request.operations(),
                        request.totalTranslogOps());
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } catch (TranslogRecoveryPerformer.BatchOperationException exception) {
                    MapperException mapperException =
                        (MapperException) ExceptionsHelper.unwrap(exception, MapperException.class);
                    if (mapperException == null) {
                        throw exception;
                    }
                    // in very rare cases a translog replay from primary is processed before a
                    // mapping update on this node which causes local mapping changes since the
                    // mapping (clusterstate) might not have arrived on this node. we want to
                    // wait until these mappings are processed but also need to do some
                    // maintenance and roll back the number of processed (completed) operations
                    // in this batch to ensure accounting is correct.
                    logger.trace(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "delaying recovery due to missing mapping changes " +
                                "(rolling back stats for [{}] ops)",
                            exception.completedOperations()),
                        exception);
                    final RecoveryState.Translog translog =
                        recoveryRef.target().state().getTranslog();
                    // do the maintainance and rollback competed ops
                    translog.decrementRecoveredOperations(exception.completedOperations());
                    // we do not need to use a timeout here since the entire recovery mechanism
                    // has an inactivity protection (it will be canceled)
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
                            onFailure(new ElasticsearchException("cluster service was closed while"
                                + " waiting for mapping updates"));
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            // note that we do not use a timeout (see comment above)
                            onFailure(new ElasticsearchTimeoutException("timed out waiting for " +
                                "mapping updates (timeout [" + timeout + "])"));
                        }
                    });
                }
            }
        }
    }

    private void waitForClusterState(long clusterStateVersion) {
        final ClusterState clusterState = clusterService.state();
        ClusterStateObserver observer = new ClusterStateObserver(clusterState, clusterService,
            TimeValue.timeValueMinutes(5), logger,threadPool.getThreadContext());
        if (clusterState.getVersion() >= clusterStateVersion) {
            logger.trace("node has cluster state with version higher than {} (current: {})",
                clusterStateVersion, clusterState.getVersion());
            return;
        } else {
            logger.trace("waiting for cluster state version {} (current: {})", clusterStateVersion,
                clusterState.getVersion());
            final PlainActionFuture<Long> future = new PlainActionFuture<>();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {

                @Override
                public void onNewClusterState(ClusterState state) {
                    future.onResponse(state.getVersion());
                }

                @Override
                public void onClusterServiceClose() {
                    future.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    future.onFailure(
                        new IllegalStateException("cluster state never updated to version " +
                            clusterStateVersion));
                }
            }, newState -> newState.getVersion() >= clusterStateVersion);
            try {
                long currentVersion = future.get();
                logger.trace("successfully waited for cluster state with version {} (current: {})",
                    clusterStateVersion, currentVersion);
            } catch (Exception e) {
                logger.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "failed waiting for cluster state with version {} (current: {})",
                        clusterStateVersion,
                        clusterService.state().getVersion()),
                    e);
                throw ExceptionsHelper.convertToRuntime(e);
            }
        }
    }

    class FilesInfoRequestHandler implements TransportRequestHandler<RecoveryFilesInfoRequest> {

        @Override
        public void messageReceived(RecoveryFilesInfoRequest request, TransportChannel channel)
            throws Exception {
            try (RecoveryRef recoveryRef =
                     onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                FileRecoveryTargetHandler target = recoveryRef.target();
                target.receiveFileInfo(request.phase1FileNames, request.phase1FileSizes, request
                        .phase1ExistingFileNames,
                        request.phase1ExistingFileSizes, request.totalTranslogOps);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    class CleanFilesRequestHandler implements TransportRequestHandler<RecoveryCleanFilesRequest> {

        @Override
        public void messageReceived(RecoveryCleanFilesRequest request, TransportChannel channel)
            throws Exception {
            try (RecoveryRef recoveryRef =
                     onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                FileRecoveryTargetHandler target = recoveryRef.target();
                target.cleanFiles(request.totalTranslogOps(), request.sourceMetaSnapshot());
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    class FileChunkTransportRequestHandler implements TransportRequestHandler<RecoveryFileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final RecoveryFileChunkRequest request,
                                    TransportChannel channel) throws Exception {
            try (RecoveryRef recoveryRef =
                     onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                RecoveryTarget target = recoveryRef.target();
                final RecoveryState.Index indexState = target.state().getIndex();
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
                        target.indexShard().recoveryStats().addThrottleTime(throttleTimeInNanos);
                    }
                }

                ((FileRecoveryTargetHandler)target).writeFileChunk(request.metadata(), request
                        .position(), request.content(),
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
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecovery(recoveryId)) {
                if (recoveryRef != null) {
                    logger.error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "unexpected error during recovery [{}], failing shard", recoveryId), e);
                    onGoingRecoveries.failRecovery(recoveryId,
                            new RecoveryFailedException(recoveryRef.target().state(), "unexpected error", e),
                            true // be safe
                    );
                } else {
                    logger.debug(
                        (Supplier<?>) () -> new ParameterizedMessage(
                            "unexpected error during recovery, but recovery id [{}] is finished", recoveryId), e);
                }
            }
        }

        @Override
        public void doRun() {
            doRecovery(recoveryId);
        }
    }

}
