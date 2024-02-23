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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceClusterStateDelay;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction.TYPE;

public class TransportStatelessPrimaryRelocationAction extends TransportAction<
    StatelessPrimaryRelocationAction.Request,
    ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportStatelessPrimaryRelocationAction.class);

    public static final String START_RELOCATION_ACTION_NAME = TYPE.name() + "/start";
    public static final String PRIMARY_CONTEXT_HANDOFF_ACTION_NAME = TYPE.name() + "/primary_context_handoff";

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final PeerRecoveryTargetService peerRecoveryTargetService;
    private final StatelessCommitService statelessCommitService;
    private final Executor recoveryExecutor;
    private final ThreadContext threadContext;

    @Inject
    public TransportStatelessPrimaryRelocationAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        PeerRecoveryTargetService peerRecoveryTargetService,
        StatelessCommitService statelessCommitService
    ) {
        super(TYPE.name(), actionFilters, transportService.getTaskManager());
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.peerRecoveryTargetService = peerRecoveryTargetService;
        this.statelessCommitService = statelessCommitService;

        final var threadPool = transportService.getThreadPool();
        this.recoveryExecutor = threadPool.generic();
        this.threadContext = threadPool.getThreadContext();

        transportService.registerRequestHandler(
            START_RELOCATION_ACTION_NAME,
            recoveryExecutor,
            StatelessPrimaryRelocationAction.Request::new,
            (request, channel, task) -> handleStartRelocation(
                task,
                request,
                new ChannelActionListener<>(channel).map(ignored -> TransportResponse.Empty.INSTANCE)
            )
        );

        transportService.registerRequestHandler(
            PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
            recoveryExecutor,
            PrimaryContextHandoffRequest::new,
            (request, channel, task) -> handlePrimaryContextHandoff(
                request,
                new ChannelActionListener<>(channel).map(ignored -> TransportResponse.Empty.INSTANCE)
            )
        );
    }

    @Override
    protected void doExecute(Task task, StatelessPrimaryRelocationAction.Request request, ActionListener<ActionResponse.Empty> listener) {
        // executed locally by `PeerRecoveryTargetService` (i.e. we are on the target node here)
        logger.trace("{} preparing unsearchable shard for primary relocation", request.shardId());

        try (var recoveryRef = peerRecoveryTargetService.getRecoveryRef(request.recoveryId(), request.shardId())) {
            final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            final var indexShard = indexService.getShard(request.shardId().id());
            indexShard.prepareForIndexRecovery();
            transportService.sendChildRequest(
                recoveryRef.target().sourceNode(),
                START_RELOCATION_ACTION_NAME,
                request,
                task,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, in -> ActionResponse.Empty.INSTANCE, recoveryExecutor)
            );
        }
    }

    private void handleStartRelocation(Task task, StatelessPrimaryRelocationAction.Request request, ActionListener<Void> listener) {
        PeerRecoverySourceClusterStateDelay.ensureClusterStateVersion(
            request.clusterStateVersion(),
            clusterService,
            recoveryExecutor,
            threadContext,
            listener,
            new Consumer<>() {
                @Override
                public void accept(ActionListener<Void> l) {
                    handleStartRelocationWithFreshClusterState(task, request, l);
                }

                @Override
                public String toString() {
                    return "recovery [" + request + "]";
                }
            }
        );
    }

    private void handleStartRelocationWithFreshClusterState(
        Task task,
        StatelessPrimaryRelocationAction.Request request,
        ActionListener<Void> listener
    ) {
        // executed remotely by `TransportStatelessPrimaryRelocationAction#doExecute` (i.e. we are on the source node here)
        logger.debug(
            "[{}]: starting unsearchable primary relocation to [{}] with allocation ID [{}]",
            request.shardId(),
            request.targetNode().descriptionWithoutAttributes(),
            request.targetAllocationId()
        );

        final IndexShard indexShard;
        final IndexEngine engine;
        try {
            final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
            indexShard = indexService.getShard(request.shardId().id());
            engine = ensureIndexEngine(indexShard.getEngineOrNull(), indexShard.state(), indexShard.routingEntry());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        indexShard.recoveryStats().incCurrentAsSource();

        // Flushing before blocking operations because we expect this to reduce the amount of work done by the flush that happens while
        // operations are blocked. NB the flush has force=false so may do nothing.
        final var preFlushStep = new SubscribableListener<Engine.FlushResult>();

        logShardStats("flushing before acquiring all primary operation permits", indexShard, engine);
        ActionListener.run(preFlushStep, l -> engine.flush(false, false, l));
        logger.debug("[{}] completed the flush, waiting to upload", request.shardId());

        preFlushStep.addListener(listener.delegateResponse((l, e) -> {
            indexShard.recoveryStats().decCurrentAsSource();
            l.onFailure(e);
        }).delegateFailureAndWrap((listener0, preFlushResult) -> {
            logger.debug("[{}] acquiring all primary operation permits", request.shardId());
            indexShard.relocated(request.targetAllocationId(), (primaryContext, handoffResultListener) -> {
                logShardStats("obtained primary context", indexShard, engine);
                logger.debug("[{}] obtained primary context: [{}]", request.shardId(), primaryContext);

                final var markedShardAsRelocating = new SubscribableListener<Void>();
                // Do not wait on flush durability as we will wait at the stateless commit service level for the upload
                engine.flush(false, true, ActionListener.noop());
                logShardStats("flush after acquiring primary context completed", indexShard, engine);
                long lastFlushedGeneration = engine.getLastCommittedSegmentInfos().getGeneration();

                final var localCheckpoints = new HashMap<>(primaryContext.getCheckpointStates());
                final var sourceCheckpoints = localCheckpoints.get(indexShard.routingEntry().allocationId().getId());
                localCheckpoints.put(request.targetAllocationId(), sourceCheckpoints);

                final var targetNodeId = request.targetNode().getId();
                try {
                    indexShard.removePeerRecoveryRetentionLease(targetNodeId, ActionListener.noop());
                } catch (RetentionLeaseNotFoundException e) {
                    // ok, we don't know it exists here
                }
                indexShard.cloneLocalPeerRecoveryRetentionLease(targetNodeId, ActionListener.noop());

                final var retentionLeases = indexShard.getRetentionLeases();
                final var leaseId = ReplicationTracker.getPeerRecoveryRetentionLeaseId(targetNodeId);
                if (retentionLeases.contains(leaseId) == false) {
                    // This is practically impossible, we only just created this lease, but in theory it could happen since leases have
                    // time-based expiry.
                    throw new RetentionLeaseNotFoundException(leaseId);
                }

                ActionListener<Void> handoffCompleteListener = statelessCommitService.markRelocating(
                    indexShard.shardId(),
                    lastFlushedGeneration,
                    markedShardAsRelocating
                );

                // Create a compound listener which will trigger both the stateless commit service listener and top-level
                // handoffResultListener
                ActionListener<TransportResponse.Empty> compoundHandoffListener = new ActionListener<>() {
                    @Override
                    public void onResponse(TransportResponse.Empty unused) {
                        logger.debug("[{}] primary context handoff succeeded", request.shardId());
                        try {
                            handoffCompleteListener.onResponse(null);
                            indexShard.recoveryStats().decCurrentAsSource();
                        } finally {
                            handoffResultListener.onResponse(null);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            handoffCompleteListener.onFailure(e);
                        } finally {
                            handoffResultListener.onFailure(e);
                        }
                    }
                };

                markedShardAsRelocating.addListener(compoundHandoffListener.delegateFailureAndWrap((finalHandoffListener, v) -> {
                    logger.debug("[{}] flush complete, handing off primary context", request.shardId());

                    assert assertLastCommitSequenceNumberConsistency(indexShard, sourceCheckpoints, false);
                    transportService.sendChildRequest(
                        request.targetNode(),
                        PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
                        new PrimaryContextHandoffRequest(
                            request.recoveryId(),
                            request.shardId(),
                            new ReplicationTracker.PrimaryContext(
                                primaryContext.clusterStateVersion(),
                                localCheckpoints,
                                primaryContext.getRoutingTable()
                            ),
                            retentionLeases
                        ),
                        task,
                        TransportRequestOptions.EMPTY,
                        new ActionListenerResponseHandler<>(finalHandoffListener, in -> TransportResponse.Empty.INSTANCE, recoveryExecutor)
                    );
                }), recoveryExecutor, threadContext);
            }, listener0);
        }), recoveryExecutor, threadContext);
    }

    private void logShardStats(String message, IndexShard indexShard, Engine engine) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "{}: {}.Flush stats [{}], Translog stats [{}], Merge stats [{}], Segments {}",
                indexShard.shardId(),
                message,
                Strings.toString(indexShard.flushStats()),
                Strings.toString(indexShard.translogStats()),
                Strings.toString(indexShard.mergeStats()),
                engine.segments()
            );
        }
    }

    private IndexEngine ensureIndexEngine(Engine engine, IndexShardState indexShardState, ShardRouting shardRouting) {
        if (engine instanceof IndexEngine indexEngine) {
            return indexEngine;
        } else if (engine == null) {
            throw new AlreadyClosedException("source shard closed before recovery started: " + shardRouting);
        } else {
            final var message = Strings.format(
                "not an IndexEngine: %s [indexShardState=%s, shardRouting=%s]",
                engine,
                indexShardState,
                shardRouting
            );
            assert false : message;
            throw new IllegalStateException(message);
        }
    }

    private void handlePrimaryContextHandoff(PrimaryContextHandoffRequest request, ActionListener<Void> listener) {
        // executed remotely by `TransportStatelessPrimaryRelocationAction#handleStartRelocation` (i.e. we are on the target node here)
        logger.debug("[{}] received primary context", request.shardId());

        final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final var indexShard = indexService.getShard(request.shardId().id());

        final var recoveryRef = peerRecoveryTargetService.getRecoveryRef(request.recoveryId(), request.shardId());
        ActionListener.run(
            ActionListener.releaseAfter(listener, Releasables.wrap(recoveryRef.target().disableRecoveryMonitor(), recoveryRef)),
            l -> indexShard.preRecovery(l.map(ignored -> {
                indexShard.updateRetentionLeasesOnReplica(request.retentionLeases());
                indexShard.recoveryState().setStage(RecoveryState.Stage.VERIFY_INDEX);
                indexShard.recoveryState().setStage(RecoveryState.Stage.TRANSLOG);
                indexShard.openEngineAndSkipTranslogRecovery();

                // Should not actually have recovered anything from the translog, so the MSN and LCP should remain equal and unchanged
                // from the ones we received in the primary context handoff.
                assert assertLastCommitSequenceNumberConsistency(
                    indexShard,
                    request.primaryContext.getCheckpointStates().get(indexShard.routingEntry().allocationId().getId()),
                    true
                );

                final var recoveryState = recoveryRef.target().state();
                recoveryState.getIndex().setFileDetailsComplete();
                recoveryState.setStage(RecoveryState.Stage.FINALIZE);
                indexShard.activateWithPrimaryContext(request.primaryContext());
                return null;
            }))
        );
    }

    private static boolean assertLastCommitSequenceNumberConsistency(
        IndexShard indexShard,
        ReplicationTracker.CheckpointState sourceCheckpoints,
        boolean flushFirst
    ) {
        if (Randomness.get().nextBoolean()) {
            // don't acquire a commit every time, lest it disturb something else
            final var engine = indexShard.getEngineOrNull();
            if (engine == null) {
                assert indexShard.state() == IndexShardState.CLOSED : indexShard.shardId() + " engine null but index not closed";
            } else {
                try (var commitRef = engine.acquireLastIndexCommit(flushFirst)) {
                    final var indexCommit = commitRef.getIndexCommit();
                    final var userData = indexCommit.getUserData();
                    final var localCheckpoint = Long.toString(sourceCheckpoints.getLocalCheckpoint());
                    assert localCheckpoint.equals(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY))
                        && localCheckpoint.equals(userData.get(SequenceNumbers.MAX_SEQ_NO))
                        : indexShard.shardId() + ": " + sourceCheckpoints + " vs " + userData;
                } catch (IOException e) {
                    throw new AssertionError("unexpected", e);
                } catch (IllegalStateException e) {
                    assert indexShard.state() == IndexShardState.CLOSED : e;
                }
            }
        }
        return true;
    }

    public static class PrimaryContextHandoffRequest extends TransportRequest {

        private final long recoveryId;
        private final ShardId shardId;
        private final ReplicationTracker.PrimaryContext primaryContext;
        private final RetentionLeases retentionLeases;

        PrimaryContextHandoffRequest(
            long recoveryId,
            ShardId shardId,
            ReplicationTracker.PrimaryContext primaryContext,
            RetentionLeases retentionLeases
        ) {
            this.recoveryId = recoveryId;
            this.shardId = shardId;
            this.primaryContext = primaryContext;
            this.retentionLeases = retentionLeases;
        }

        PrimaryContextHandoffRequest(StreamInput in) throws IOException {
            super(in);
            recoveryId = in.readVLong();

            if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.DUMMY_PRIMARY_RELOCATION_CHANGE)) {
                boolean value = in.readBoolean();
                if (value != in.getTransportVersion().onOrAfter(ServerlessTransportVersions.DUMMY_PRIMARY_RELOCATION_CHANGE_FIX)) {
                    throw new AssertionError("Transport api patch test failed");
                }
            }

            shardId = new ShardId(in);
            primaryContext = new ReplicationTracker.PrimaryContext(in);
            retentionLeases = new RetentionLeases(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(recoveryId);

            // See note in StreamInput ctor about this temporary boolean for testing transport patching
            if (out.getTransportVersion().onOrAfter(ServerlessTransportVersions.DUMMY_PRIMARY_RELOCATION_CHANGE)) {
                boolean value = out.getTransportVersion().onOrAfter(ServerlessTransportVersions.DUMMY_PRIMARY_RELOCATION_CHANGE_FIX);
                out.writeBoolean(value);
            }

            shardId.writeTo(out);
            primaryContext.writeTo(out);
            retentionLeases.writeTo(out);
        }

        public long recoveryId() {
            return recoveryId;
        }

        public ShardId shardId() {
            return shardId;
        }

        public ReplicationTracker.PrimaryContext primaryContext() {
            return primaryContext;
        }

        public RetentionLeases retentionLeases() {
            return retentionLeases;
        }
    }
}
