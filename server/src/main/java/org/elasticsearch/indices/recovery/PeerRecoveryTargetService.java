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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
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
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.indices.recovery.RecoveriesCollection.RecoveryRef;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * The recovery target handles recoveries of peer shards of the shard+node to recover to.
 * <p>
 * Note, it can be safely assumed that there will only be a single recovery per shard (index+id) and
 * not several of them (since we don't allocate several shard replicas to the same node).
 */
public class PeerRecoveryTargetService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(PeerRecoveryTargetService.class);

    public static class Actions {
        public static final String FILES_INFO = "internal:index/shard/recovery/filesInfo";
        public static final String FILE_CHUNK = "internal:index/shard/recovery/file_chunk";
        public static final String CLEAN_FILES = "internal:index/shard/recovery/clean_files";
        public static final String TRANSLOG_OPS = "internal:index/shard/recovery/translog_ops";
        public static final String PREPARE_TRANSLOG = "internal:index/shard/recovery/prepare_translog";
        public static final String FINALIZE = "internal:index/shard/recovery/finalize";
        public static final String HANDOFF_PRIMARY_CONTEXT = "internal:index/shard/recovery/handoff_primary_context";
    }

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

    private final RecoveriesCollection onGoingRecoveries;

    public PeerRecoveryTargetService(ThreadPool threadPool, TransportService transportService,
            RecoverySettings recoverySettings, ClusterService clusterService) {
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
        this.onGoingRecoveries = new RecoveriesCollection(logger, threadPool);

        transportService.registerRequestHandler(Actions.FILES_INFO, ThreadPool.Names.GENERIC, RecoveryFilesInfoRequest::new,
            new FilesInfoRequestHandler());
        transportService.registerRequestHandler(Actions.FILE_CHUNK, ThreadPool.Names.GENERIC, RecoveryFileChunkRequest::new,
            new FileChunkTransportRequestHandler());
        transportService.registerRequestHandler(Actions.CLEAN_FILES, ThreadPool.Names.GENERIC,
            RecoveryCleanFilesRequest::new, new CleanFilesRequestHandler());
        transportService.registerRequestHandler(Actions.PREPARE_TRANSLOG, ThreadPool.Names.GENERIC,
                RecoveryPrepareForTranslogOperationsRequest::new, new PrepareForTranslogOperationsRequestHandler());
        transportService.registerRequestHandler(Actions.TRANSLOG_OPS, ThreadPool.Names.GENERIC, RecoveryTranslogOperationsRequest::new,
            new TranslogOperationsRequestHandler());
        transportService.registerRequestHandler(Actions.FINALIZE, ThreadPool.Names.GENERIC, RecoveryFinalizeRecoveryRequest::new,
            new FinalizeRecoveryRequestHandler());
        transportService.registerRequestHandler(
                Actions.HANDOFF_PRIMARY_CONTEXT,
                ThreadPool.Names.GENERIC,
                RecoveryHandoffPrimaryContextRequest::new,
                new HandoffPrimaryContextRequestHandler());
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            onGoingRecoveries.cancelRecoveriesForShard(shardId, "shard closed");
        }
    }

    public void startRecovery(final IndexShard indexShard, final DiscoveryNode sourceNode, final RecoveryListener listener) {
        // create a new recovery status, and process...
        final long recoveryId = onGoingRecoveries.startRecovery(indexShard, sourceNode, listener, recoverySettings.activityTimeout());
        // we fork off quickly here and go async but this is called from the cluster state applier thread too and that can cause
        // assertions to trip if we executed it on the same thread hence we fork off to the generic threadpool.
        threadPool.generic().execute(new RecoveryRunner(recoveryId));
    }

    protected void retryRecovery(final long recoveryId, final Throwable reason, TimeValue retryAfter, TimeValue activityTimeout) {
        logger.trace(() -> new ParameterizedMessage(
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
            threadPool.schedule(new RecoveryRunner(newTarget.recoveryId()), retryAfter, ThreadPool.Names.GENERIC);
        }
    }

    private void doRecovery(final long recoveryId) {
        final StartRecoveryRequest request;
        final RecoveryState.Timer timer;
        CancellableThreads cancellableThreads;
        try (RecoveryRef recoveryRef = onGoingRecoveries.getRecovery(recoveryId)) {
            if (recoveryRef == null) {
                logger.trace("not running recovery with id [{}] - can not find it (probably finished)", recoveryId);
                return;
            }
            final RecoveryTarget recoveryTarget = recoveryRef.target();
            timer = recoveryTarget.state().getTimer();
            cancellableThreads = recoveryTarget.cancellableThreads();
            try {
                assert recoveryTarget.sourceNode() != null : "can not do a recovery without a source node";
                logger.trace("{} preparing shard for peer recovery", recoveryTarget.shardId());
                recoveryTarget.indexShard().prepareForIndexRecovery();
                final long startingSeqNo = recoveryTarget.indexShard().recoverLocallyUpToGlobalCheckpoint();
                assert startingSeqNo == UNASSIGNED_SEQ_NO || recoveryTarget.state().getStage() == RecoveryState.Stage.TRANSLOG :
                    "unexpected recovery stage [" + recoveryTarget.state().getStage() + "] starting seqno [ " + startingSeqNo + "]";
                request = getStartRecoveryRequest(logger, clusterService.localNode(), recoveryTarget, startingSeqNo);
            } catch (final Exception e) {
                // this will be logged as warning later on...
                logger.trace("unexpected error while preparing shard for peer recovery, failing recovery", e);
                onGoingRecoveries.failRecovery(recoveryId,
                    new RecoveryFailedException(recoveryTarget.state(), "failed to prepare shard for recovery", e), true);
                return;
            }
        }
        Consumer<Exception> handleException = e -> {
            if (logger.isTraceEnabled()) {
                logger.trace(() -> new ParameterizedMessage(
                    "[{}][{}] Got exception on recovery", request.shardId().getIndex().getName(),
                    request.shardId().id()), e);
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
                retryRecovery(recoveryId, cause, recoverySettings.retryDelayStateSync(),
                    recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof ConnectTransportException) {
                logger.debug("delaying recovery of {} for [{}] due to networking error [{}]", request.shardId(),
                    recoverySettings.retryDelayNetwork(), cause.getMessage());
                retryRecovery(recoveryId, cause.getMessage(), recoverySettings.retryDelayNetwork(),
                    recoverySettings.activityTimeout());
                return;
            }

            if (cause instanceof AlreadyClosedException) {
                onGoingRecoveries.failRecovery(recoveryId,
                    new RecoveryFailedException(request, "source shard is closed", cause), false);
                return;
            }

            onGoingRecoveries.failRecovery(recoveryId, new RecoveryFailedException(request, e), true);
        };

        try {
            logger.trace("{} starting recovery from {}", request.shardId(), request.sourceNode());
            cancellableThreads.executeIO(() ->
                // we still execute under cancelableThreads here to ensure we interrupt any blocking call to the network if any
                // on the underlying transport. It's unclear if we need this here at all after moving to async execution but
                // the issues that a missing call to this could cause are sneaky and hard to debug. If we don't need it on this
                // call we can potentially remove it altogether which we should do it in a major release only with enough
                // time to test. This shoudl be done for 7.0 if possible
                transportService.sendRequest(request.sourceNode(), PeerRecoverySourceService.Actions.START_RECOVERY, request,
                    new TransportResponseHandler<RecoveryResponse>() {
                        @Override
                        public void handleResponse(RecoveryResponse recoveryResponse) {
                            final TimeValue recoveryTime = new TimeValue(timer.time());
                            // do this through ongoing recoveries to remove it from the collection
                            onGoingRecoveries.markRecoveryAsDone(recoveryId);
                            if (logger.isTraceEnabled()) {
                                StringBuilder sb = new StringBuilder();
                                sb.append('[').append(request.shardId().getIndex().getName()).append(']')
                                    .append('[').append(request.shardId().id()).append("] ");
                                sb.append("recovery completed from ").append(request.sourceNode()).append(", took[").append(recoveryTime)
                                    .append("]\n");
                                sb.append("   phase1: recovered_files [").append(recoveryResponse.phase1FileNames.size()).append("]")
                                    .append(" with total_size of [").append(new ByteSizeValue(recoveryResponse.phase1TotalSize)).append("]")
                                    .append(", took [").append(timeValueMillis(recoveryResponse.phase1Time)).append("], throttling_wait [")
                                    .append(timeValueMillis(recoveryResponse.phase1ThrottlingWaitTime)).append(']').append("\n");
                                sb.append("         : reusing_files   [").append(recoveryResponse.phase1ExistingFileNames.size())
                                    .append("] with total_size of [").append(new ByteSizeValue(recoveryResponse.phase1ExistingTotalSize))
                                    .append("]\n");
                                sb.append("   phase2: start took [").append(timeValueMillis(recoveryResponse.startTime)).append("]\n");
                                sb.append("         : recovered [").append(recoveryResponse.phase2Operations).append("]")
                                    .append(" transaction log operations")
                                    .append(", took [").append(timeValueMillis(recoveryResponse.phase2Time)).append("]")
                                    .append("\n");
                                logger.trace("{}", sb);
                            } else {
                                logger.debug("{} recovery done from [{}], took [{}]", request.shardId(), request.sourceNode(),
                                    recoveryTime);
                            }
                        }

                        @Override
                        public void handleException(TransportException e) {
                            handleException.accept(e);
                        }

                        @Override
                        public String executor() {
                            // we do some heavy work like refreshes in the response so fork off to the generic threadpool
                            return ThreadPool.Names.GENERIC;
                        }

                        @Override
                        public RecoveryResponse read(StreamInput in) throws IOException {
                            return new RecoveryResponse(in);
                        }
                    })
            );
        } catch (CancellableThreads.ExecutionCancelledException e) {
            logger.trace("recovery cancelled", e);
        } catch (Exception e) {
            handleException.accept(e);
        }
    }

    /**
     * Prepare the start recovery request.
     *
     * @param logger         the logger
     * @param localNode      the local node of the recovery target
     * @param recoveryTarget the target of the recovery
     * @param startingSeqNo  a sequence number that an operation-based peer recovery can start with.
     *                       This is the first operation after the local checkpoint of the safe commit if exists.
     * @return a start recovery request
     */
    public static StartRecoveryRequest getStartRecoveryRequest(Logger logger, DiscoveryNode localNode,
                                                               RecoveryTarget recoveryTarget, long startingSeqNo) {
        final StartRecoveryRequest request;
        logger.trace("{} collecting local files for [{}]", recoveryTarget.shardId(), recoveryTarget.sourceNode());

        Store.MetadataSnapshot metadataSnapshot;
        try {
            metadataSnapshot = recoveryTarget.indexShard().snapshotStoreMetadata();
            // Make sure that the current translog is consistent with the Lucene index; otherwise, we have to throw away the Lucene index.
            try {
                final String expectedTranslogUUID = metadataSnapshot.getCommitUserData().get(Translog.TRANSLOG_UUID_KEY);
                final long globalCheckpoint = Translog.readGlobalCheckpoint(recoveryTarget.translogLocation(), expectedTranslogUUID);
                assert globalCheckpoint + 1 >= startingSeqNo : "invalid startingSeqNo " + startingSeqNo + " >= " + globalCheckpoint;
            } catch (IOException | TranslogCorruptedException e) {
                logger.warn(new ParameterizedMessage("error while reading global checkpoint from translog, " +
                    "resetting the starting sequence number from {} to unassigned and recovering as if there are none", startingSeqNo), e);
                metadataSnapshot = Store.MetadataSnapshot.EMPTY;
                startingSeqNo = UNASSIGNED_SEQ_NO;
            }
        } catch (final org.apache.lucene.index.IndexNotFoundException e) {
            // happens on an empty folder. no need to log
            assert startingSeqNo == UNASSIGNED_SEQ_NO : startingSeqNo;
            logger.trace("{} shard folder empty, recovering all files", recoveryTarget);
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        } catch (final IOException e) {
            if (startingSeqNo != UNASSIGNED_SEQ_NO) {
                logger.warn(new ParameterizedMessage("error while listing local files, resetting the starting sequence number from {} " +
                    "to unassigned and recovering as if there are none", startingSeqNo), e);
                startingSeqNo = UNASSIGNED_SEQ_NO;
            } else {
                logger.warn("error while listing local files, recovering as if there are none", e);
            }
            metadataSnapshot = Store.MetadataSnapshot.EMPTY;
        }
        logger.trace("{} local file count [{}]", recoveryTarget.shardId(), metadataSnapshot.size());
        request = new StartRecoveryRequest(
            recoveryTarget.shardId(),
            recoveryTarget.indexShard().routingEntry().allocationId().getId(),
            recoveryTarget.sourceNode(),
            localNode,
            metadataSnapshot,
            recoveryTarget.state().getPrimary(),
            recoveryTarget.recoveryId(),
            startingSeqNo);
        return request;
    }

    public interface RecoveryListener {
        void onRecoveryDone(RecoveryState state);

        void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure);
    }

    class PrepareForTranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryPrepareForTranslogOperationsRequest> {

        @Override
        public void messageReceived(RecoveryPrepareForTranslogOperationsRequest request, TransportChannel channel, Task task) {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.PREPARE_TRANSLOG, request);
                recoveryRef.target().prepareForTranslogOperations(request.totalTranslogOps(),
                    ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
        }
    }

    class FinalizeRecoveryRequestHandler implements TransportRequestHandler<RecoveryFinalizeRecoveryRequest> {

        @Override
        public void messageReceived(RecoveryFinalizeRecoveryRequest request, TransportChannel channel, Task task) throws Exception {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.FINALIZE, request);
                recoveryRef.target().finalizeRecovery(request.globalCheckpoint(), request.trimAboveSeqNo(),
                    ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
        }
    }

    class HandoffPrimaryContextRequestHandler implements TransportRequestHandler<RecoveryHandoffPrimaryContextRequest> {

        @Override
        public void messageReceived(final RecoveryHandoffPrimaryContextRequest request, final TransportChannel channel,
                                    Task task) throws Exception {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                recoveryRef.target().handoffPrimaryContext(request.primaryContext());
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

    }

    class TranslogOperationsRequestHandler implements TransportRequestHandler<RecoveryTranslogOperationsRequest> {

        @Override
        public void messageReceived(final RecoveryTranslogOperationsRequest request, final TransportChannel channel,
                                    Task task) throws IOException {
            try (RecoveryRef recoveryRef =
                     onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
                final RecoveryTarget recoveryTarget = recoveryRef.target();
                final ActionListener<RecoveryTranslogOperationsResponse> listener =
                    new ChannelActionListener<>(channel, Actions.TRANSLOG_OPS, request);
                final Consumer<Exception> retryOnMappingException = exception -> {
                    // in very rare cases a translog replay from primary is processed before a mapping update on this node
                    // which causes local mapping changes since the mapping (clusterstate) might not have arrived on this node.
                    logger.debug("delaying recovery due to missing mapping changes", exception);
                    // we do not need to use a timeout here since the entire recovery mechanism has an inactivity protection (it will be
                    // canceled)
                    observer.waitForNextChange(new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            try {
                                messageReceived(request, channel, task);
                            } catch (Exception e) {
                                listener.onFailure(e);
                            }
                        }

                        @Override
                        public void onClusterServiceClose() {
                            listener.onFailure(new ElasticsearchException(
                                "cluster service was closed while waiting for mapping updates"));
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            // note that we do not use a timeout (see comment above)
                            listener.onFailure(new ElasticsearchTimeoutException("timed out waiting for mapping updates " +
                                "(timeout [" + timeout + "])"));
                        }
                    });
                };
                final IndexMetaData indexMetaData = clusterService.state().metaData().index(request.shardId().getIndex());
                final long mappingVersionOnTarget = indexMetaData != null ? indexMetaData.getMappingVersion() : 0L;
                recoveryTarget.indexTranslogOperations(
                        request.operations(),
                        request.totalTranslogOps(),
                        request.maxSeenAutoIdTimestampOnPrimary(),
                        request.maxSeqNoOfUpdatesOrDeletesOnPrimary(),
                        request.retentionLeases(),
                        request.mappingVersionOnPrimary(),
                        ActionListener.wrap(
                                checkpoint -> listener.onResponse(new RecoveryTranslogOperationsResponse(checkpoint)),
                                e -> {
                                    // do not retry if the mapping on replica is at least as recent as the mapping
                                    // that the primary used to index the operations in the request.
                                    if (mappingVersionOnTarget < request.mappingVersionOnPrimary() && e instanceof MapperException) {
                                        retryOnMappingException.accept(e);
                                    } else {
                                        listener.onFailure(e);
                                    }
                                })
                );
            }
        }
    }

    class FilesInfoRequestHandler implements TransportRequestHandler<RecoveryFilesInfoRequest> {

        @Override
        public void messageReceived(RecoveryFilesInfoRequest request, TransportChannel channel, Task task) throws Exception {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.FILES_INFO, request);
                recoveryRef.target().receiveFileInfo(
                    request.phase1FileNames, request.phase1FileSizes, request.phase1ExistingFileNames, request.phase1ExistingFileSizes,
                    request.totalTranslogOps, ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
        }
    }

    class CleanFilesRequestHandler implements TransportRequestHandler<RecoveryCleanFilesRequest> {

        @Override
        public void messageReceived(RecoveryCleanFilesRequest request, TransportChannel channel, Task task) throws Exception {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.CLEAN_FILES, request);
                recoveryRef.target().cleanFiles(request.totalTranslogOps(), request.getGlobalCheckpoint(), request.sourceMetaSnapshot(),
                    ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
        }
    }

    class FileChunkTransportRequestHandler implements TransportRequestHandler<RecoveryFileChunkRequest> {

        // How many bytes we've copied since we last called RateLimiter.pause
        final AtomicLong bytesSinceLastPause = new AtomicLong();

        @Override
        public void messageReceived(final RecoveryFileChunkRequest request, TransportChannel channel, Task task) throws Exception {
            try (RecoveryRef recoveryRef = onGoingRecoveries.getRecoverySafe(request.recoveryId(), request.shardId())) {
                final RecoveryTarget recoveryTarget = recoveryRef.target();
                final RecoveryState.Index indexState = recoveryTarget.state().getIndex();
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
                        recoveryTarget.indexShard().recoveryStats().addThrottleTime(throttleTimeInNanos);
                    }
                }
                final ActionListener<TransportResponse> listener = new ChannelActionListener<>(channel, Actions.FILE_CHUNK, request);
                recoveryTarget.writeFileChunk(request.metadata(), request.position(), request.content(), request.lastChunk(),
                    request.totalTranslogOps(), ActionListener.map(listener, nullVal -> TransportResponse.Empty.INSTANCE));
            }
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
                    logger.error(() -> new ParameterizedMessage("unexpected error during recovery [{}], failing shard", recoveryId), e);
                    onGoingRecoveries.failRecovery(recoveryId,
                            new RecoveryFailedException(recoveryRef.target().state(), "unexpected error", e),
                            true // be safe
                    );
                } else {
                    logger.debug(() -> new ParameterizedMessage(
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
