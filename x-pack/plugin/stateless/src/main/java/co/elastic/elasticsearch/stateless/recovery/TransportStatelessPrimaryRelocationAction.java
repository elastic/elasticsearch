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

import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;

import static org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction.INSTANCE;

public class TransportStatelessPrimaryRelocationAction extends HandledTransportAction<
    StatelessPrimaryRelocationAction.Request,
    ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportStatelessPrimaryRelocationAction.class);

    public static final String START_RELOCATION_ACTION_NAME = INSTANCE.name() + "/start";
    public static final String PRIMARY_CONTEXT_HANDOFF_ACTION_NAME = INSTANCE.name() + "/primary_context_handoff";

    private final TransportService transportService;
    private final IndicesService indicesService;
    private final PeerRecoveryTargetService peerRecoveryTargetService;

    @Inject
    public TransportStatelessPrimaryRelocationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        PeerRecoveryTargetService peerRecoveryTargetService
    ) {
        super(INSTANCE.name(), transportService, actionFilters, StatelessPrimaryRelocationAction.Request::new);
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.peerRecoveryTargetService = peerRecoveryTargetService;

        transportService.registerRequestHandler(
            START_RELOCATION_ACTION_NAME,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC),
            StatelessPrimaryRelocationAction.Request::new,
            (request, channel, task) -> handleStartRelocation(task, request, new ChannelActionListener<>(channel))
        );

        transportService.registerRequestHandler(
            PRIMARY_CONTEXT_HANDOFF_ACTION_NAME,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC),
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
                new ActionListenerResponseHandler<>(
                    listener,
                    in -> ActionResponse.Empty.INSTANCE,
                    transportService.getThreadPool().generic()
                )
            );
        }
    }

    private void handleStartRelocation(
        Task task,
        StatelessPrimaryRelocationAction.Request request,
        ActionListener<ActionResponse.Empty> listener
    ) {
        // executed remotely by `TransportStatelessPrimaryRelocationAction#doExecute` (i.e. we are on the source node here)
        logger.debug(
            "[{}]: starting unsearchable primary relocation to [{}] with allocation ID [{}]",
            request.shardId(),
            request.targetNode().descriptionWithoutAttributes(),
            request.targetAllocationId()
        );

        final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final var indexShard = indexService.getShard(request.shardId().id());
        final var engine = ensureIndexEngine(indexShard.getEngineOrNull(), indexShard.state(), indexShard.routingEntry());
        indexShard.recoveryStats().incCurrentAsSource();

        // Flushing before blocking operations because we expect this to reduce the amount of work done by the flush that happens while
        // operations are blocked. NB the flush has force=false so may do nothing.
        final var preFlushStep = new SubscribableListener<Engine.FlushResult>();
        logger.trace("[{}] flushing before acquiring all primary operation permits", request.shardId());
        ActionListener.run(preFlushStep, l -> engine.flush(false, false, l));

        preFlushStep.addListener(listener.delegateResponse((l, e) -> {
            indexShard.recoveryStats().decCurrentAsSource();
            l.onFailure(e);
        }).delegateFailureAndWrap((listener0, preFlushResult) -> {
            logger.trace("[{}] acquiring all primary operation permits", request.shardId());
            indexShard.relocated(request.targetAllocationId(), (primaryContext, handoffResultListener) -> {
                logger.trace("[{}] obtained primary context: [{}]", request.shardId(), primaryContext);

                final var flushStep = new SubscribableListener<Engine.FlushResult>();
                engine.flush(false, true, flushStep);

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

                flushStep.addListener(handoffResultListener.delegateFailureAndWrap((handoffResultListener0, flushResult) -> {
                    logger.trace("[{}] flush complete, handing off primary context", request.shardId());

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
                        new ActionListenerResponseHandler<>(handoffResultListener0.map(ignored -> {
                            logger.trace("[{}] primary context handoff succeeded", request.shardId());
                            indexShard.recoveryStats().decCurrentAsSource();
                            return null;
                        }), in -> TransportResponse.Empty.INSTANCE, transportService.getThreadPool().generic())
                    );
                }), indexShard.getThreadPool().generic(), indexShard.getThreadPool().getThreadContext());
            }, listener0.map(ignored -> ActionResponse.Empty.INSTANCE));
        }), indexShard.getThreadPool().generic(), indexShard.getThreadPool().getThreadContext());
    }

    private IndexEngine ensureIndexEngine(Engine engine, IndexShardState indexShardState, ShardRouting shardRouting) {
        if (engine instanceof IndexEngine indexEngine) {
            return indexEngine;
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
        logger.trace("[{}] received primary context", request.shardId());

        final var indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        final var indexShard = indexService.getShard(request.shardId().id());

        final var recoveryRef = peerRecoveryTargetService.getRecoveryRef(request.recoveryId(), request.shardId());
        ActionListener.run(
            ActionListener.releaseAfter(listener, Releasables.wrap(recoveryRef.target().disableRecoveryMonitor(), recoveryRef)),
            l -> indexShard.preRecovery(l.map(ignored -> {
                indexShard.updateRetentionLeasesOnReplica(request.retentionLeases());
                indexShard.openEngineAndRecoverFromTranslog();

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
            assert engine != null : indexShard.shardId();
            try (var commitRef = engine.acquireLastIndexCommit(flushFirst)) {
                final var indexCommit = commitRef.getIndexCommit();
                final var userData = indexCommit.getUserData();
                final var localCheckpoint = Long.toString(sourceCheckpoints.getLocalCheckpoint());
                assert localCheckpoint.equals(userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY))
                    && localCheckpoint.equals(userData.get(SequenceNumbers.MAX_SEQ_NO))
                    : indexShard.shardId() + ": " + sourceCheckpoints + " vs " + userData;

            } catch (IOException e) {
                throw new AssertionError("unexpected", e);
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
            shardId = new ShardId(in);
            primaryContext = new ReplicationTracker.PrimaryContext(in);
            retentionLeases = new RetentionLeases(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(recoveryId);
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
