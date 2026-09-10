/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.action.shard.FailedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardFailedTaskExecutor;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.CancelRecoveriesAction;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.indices.recovery.ShardRecoveryCancellation;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

/// Master-side service that proactively cancels shard recoveries that are no longer wanted.
///
/// Two scenarios are currently supported:
///
/// - Desired-balance cancellations ([cancelUndesiredRecoveries]): when the desired balance changes and an
///   initializing shard is no longer assigned to its current node, a [CancelRecoveriesAction] request is sent to
///   the data node so the recovery is aborted as soon as possible rather than waiting for it to complete before
///   the next allocation round can move the shard.
///
/// - Snapshot-blocking cancellations ([cancelRecoveriesBlockingSnapshots]): when a snapshot has
///   [SnapshotsInProgress.ShardState#WAITING] shards blocked by a primary relocation, we attempt to cancel the
///   relocation target recovery if it has not started yet, so the snapshot can proceed. Relocations driven by node
///   removal are left untouched. Skipped entirely when relocation is decoupled from snapshots
///   ([SnapshotInProgressAllocationDecider#RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING_NAME]). This path is driven by
///   [ClusterStateListener#clusterChanged].
///
/// Every operation in this service is fire-and-forget. Errors are logged as warnings or silently ignored; in all
/// failure cases the affected shards are eventually reassigned through the normal reroute/shard-failed path.
public class RecoveryDirectCancellationService extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(RecoveryDirectCancellationService.class);

    /// This limit is conservative (compared to [org.elasticsearch.indices.ShardLimitValidator] limits), but it should
    /// still capture all "still in use" cancellations for the majority of clusters. Each entry is expected to be less
    /// than 200 bytes, including the allocation ID key, the cache entry wrapper and SentCancellation object.
    /// The estimated max cache size is then ~4MB (0.2% of a 2GB heap).
    private static final int MAX_CANCELLATIONS_CACHE_SIZE = 20_000;

    /// Should exceed the expected lifetime of a cancelIfStarted=true recovery in the majority of cases. Ensures stale
    /// entries are eventually evicted in clusters where the size bound is rarely reached.
    private static final TimeValue CANCELLATION_CACHE_TTL = TimeValue.timeValueHours(6);

    public static final Setting<Boolean> ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING = Setting.boolSetting(
        "indices.recovery.enable_direct_cancellations",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final MasterServiceTaskQueue<ShardFailedTaskExecutor.Task> failedShardTaskQueue;
    private final Executor genericExecutor;
    private volatile boolean enableDirectRecoveryCancellations = false;
    private volatile boolean relocationDuringSnapshotEnabled = false;

    /// Single permit used to coalesce snapshot-cancellation runs.
    /// Acquired when a run is queued, released at the start of each run (or on rejection).
    private final Semaphore pendingSnapshotCancellationPermit = new Semaphore(1);
    private final CancelRecoveriesBlockingSnapshotRunnable snapshotCancellationRunnable = new CancelRecoveriesBlockingSnapshotRunnable();

    /// LRU bounded cache of allocation IDs for which a cancellation request was recently sent. Used to deduplicate
    /// requests, e.g. when multiple desired balance computations arrive in quick succession before prior cancellations
    /// have taken effect on the data nodes.
    final Cache<String, SentCancellation> sentCancellations;

    record SentCancellation(long term, boolean cancelIfStarted) {}

    public RecoveryDirectCancellationService(
        TransportService transportService,
        ClusterService clusterService,
        AllocationService allocationService,
        RerouteService rerouteService
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.genericExecutor = transportService.getThreadPool().generic();
        this.failedShardTaskQueue = clusterService.createTaskQueue(
            "direct-cancellation-shard-failed",
            Priority.HIGH,
            new ShardFailedTaskExecutor(allocationService, rerouteService)
        );
        this.sentCancellations = CacheBuilder.<String, SentCancellation>builder()
            .setMaximumWeight(MAX_CANCELLATIONS_CACHE_SIZE)
            .setExpireAfterWrite(CANCELLATION_CACHE_TTL)
            .build();
    }

    @Override
    protected void doStart() {
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.initializeAndWatchIfRegistered(
            ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING,
            value -> this.enableDirectRecoveryCancellations = value
        );
        // Only registered on stateless.
        final Setting<?> relocationDuringSnapshotSetting = clusterSettings.get(
            SnapshotInProgressAllocationDecider.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING_NAME
        );
        if (relocationDuringSnapshotSetting != null) {
            assert relocationDuringSnapshotSetting.isDynamic();
            clusterSettings.initializeAndWatch(relocationDuringSnapshotSetting, value -> relocationDuringSnapshotEnabled = (boolean) value);
        }
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {}

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            return;
        }
        if (enableDirectRecoveryCancellations == false || relocationDuringSnapshotEnabled) {
            return;
        }
        final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(event.state());
        final boolean newMaster = event.previousState().nodes().isLocalNodeElectedMaster() == false;
        if (newMaster || snapshotsInProgress != SnapshotsInProgress.get(event.previousState()) || event.routingTableChanged()) {
            if (snapshotsInProgress.asStream().anyMatch(SnapshotsInProgress.Entry::hasShardsInWaitingState)) {
                cancelRecoveriesBlockingSnapshots();
            }
        }
    }

    /// Asynchronously computes which initializing shards are no longer desired on their current nodes according to the given
    /// [DesiredBalance] and sends cancellation requests to the affected data nodes.
    ///
    /// Direct cancellation is a best-effort optimization. Reconciliation and desired balance computation run
    /// concurrently and continuously, so `routingAllocation` may already be stale by the time cancellations are sent.
    /// Stale cancellations are safe. The data node validates each request against its local recovery state and ignores
    /// any that no longer apply.
    ///
    /// @param desiredBalance the desired balance used to determine which recoveries are no longer heading to a desired node
    /// @param routingAllocation the routing allocation snapshot the desired balance was derived from, used to identify
    /// which shards are currently initializing on an undesired node
    public void cancelUndesiredRecoveries(DesiredBalance desiredBalance, RoutingAllocation routingAllocation) {
        genericExecutor.execute(new CancelUndesiredRecoveriesRunnable(desiredBalance, routingAllocation));
    }

    /// Returns a map of [CancelRecoveriesAction.Request] per relevant data node. Each request lists the initializing
    /// shards on that node that are no longer heading to a desired location according to `desiredBalance` and for
    /// which a recovery cancellation will be requested. Each [ShardRecoveryCancellation] carries a `cancelIfStarted`
    /// flag, determined by recovery type and allocation decider result, indicating whether the recovery should be
    /// interrupted even after it has started work.
    static Map<DiscoveryNode, CancelRecoveriesAction.Request> computeUndesiredRecoveryCancellations(
        DesiredBalance desiredBalance,
        RoutingAllocation allocation
    ) {
        final long term = allocation.getClusterState().term();
        final long version = allocation.getClusterState().version();
        final RoutingNodes routingNodes = allocation.routingNodes();
        final Map<DiscoveryNode, CancelRecoveriesAction.Request> cancellationRequests = new HashMap<>();
        for (RoutingNode routingNode : routingNodes) {
            List<ShardRecoveryCancellation> nodeCancellations = new ArrayList<>();
            for (ShardRouting shardRouting : routingNode) {
                if (shardRouting.initializing() == false) {
                    continue;
                }

                final var assignment = desiredBalance.getAssignment(shardRouting.shardId());
                if (assignment == null || assignment.nodeIds().contains(shardRouting.currentNodeId())) {
                    continue;
                }

                boolean cancelIfRecoveryStarted = false;
                if (recoveryCanBeCancelledIfStarted(shardRouting, routingNodes)) {
                    final var canRemainDecision = allocation.deciders().canRemain(shardRouting, routingNode, allocation);
                    cancelIfRecoveryStarted = canRemainDecision.type() == Decision.Type.NO;
                }
                nodeCancellations.add(
                    new ShardRecoveryCancellation(shardRouting.shardId(), shardRouting.allocationId().getId(), cancelIfRecoveryStarted)
                );
            }
            if (nodeCancellations.isEmpty() == false) {
                cancellationRequests.put(routingNode.node(), new CancelRecoveriesAction.Request(term, version, nodeCancellations));
            }
        }
        return cancellationRequests;
    }

    private static boolean recoveryCanBeCancelledIfStarted(ShardRouting shardRouting, RoutingNodes routingNodes) {
        assert shardRouting.initializing() : "calling recoveryCanBeCancelledIfStarted for non-initializing shard " + shardRouting;
        if (shardRouting.primary()) {
            return shardRouting.relocatingNodeId() != null;
        }
        if (shardRouting.role().equals(ShardRouting.Role.SEARCH_ONLY) == false) {
            return true;
        }
        return routingNodes.assignedShards(shardRouting.shardId())
            .stream()
            .filter(ShardRouting::started)
            .anyMatch(s -> s.role().equals(ShardRouting.Role.SEARCH_ONLY));
    }

    private class CancelUndesiredRecoveriesRunnable extends AbstractRunnable {
        private final DesiredBalance desiredBalance;
        private final RoutingAllocation routingAllocation;

        CancelUndesiredRecoveriesRunnable(DesiredBalance desiredBalance, RoutingAllocation routingAllocation) {
            this.desiredBalance = desiredBalance;
            this.routingAllocation = routingAllocation;
        }

        @Override
        protected void doRun() {
            final Map<DiscoveryNode, CancelRecoveriesAction.Request> requests = computeUndesiredRecoveryCancellations(
                desiredBalance,
                routingAllocation
            );
            if (requests.isEmpty()) {
                return;
            }
            sendCancellations(requests);
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(
                () -> "failed to compute or send direct recovery cancellations for desired balance ["
                    + desiredBalance.lastConvergedIndex()
                    + "] and cluster state version ["
                    + routingAllocation.getClusterState().version()
                    + "]",
                e
            );
        }
    }

    /// Grabs the latest cluster state and checks for WAITING snapshot shards blocked by queued primary relocations.
    /// Cancels those recoveries if they have not yet started. Concurrent triggers are coalesced via
    /// [pendingSnapshotCancellationPermit].
    private void cancelRecoveriesBlockingSnapshots() {
        if (pendingSnapshotCancellationPermit.tryAcquire() == false) {
            return;
        }
        genericExecutor.execute(snapshotCancellationRunnable);
    }

    // visible for testing
    static Map<DiscoveryNode, CancelRecoveriesAction.Request> computeCancellationCandidatesForSnapshots(ClusterState state) {
        final Set<ShardId> waitingSnapshotShards = new HashSet<>();
        final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(state);

        snapshotsInProgress.asStream().forEach(snapshotInProgress -> {
            if (snapshotInProgress.isClone() || snapshotInProgress.hasShardsInWaitingState() == false) {
                return;
            }
            for (Map.Entry<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shard : snapshotInProgress.shards().entrySet()) {
                if (shard.getValue().state() == SnapshotsInProgress.ShardState.WAITING) {
                    waitingSnapshotShards.add(shard.getKey());
                }
            }
        });
        if (waitingSnapshotShards.isEmpty()) {
            return Map.of();
        }

        final RoutingNodes routingNodes = state.getRoutingNodes();
        final NodesShutdownMetadata nodesShutdownMetadata = state.metadata().nodeShutdowns();
        final long term = state.term();
        final long version = state.version();

        final Map<DiscoveryNode, List<ShardRecoveryCancellation>> nodeToBlockingShards = new HashMap<>();
        for (ShardId shardId : waitingSnapshotShards) {
            final ShardRouting primary = routingNodes.activePrimary(shardId);
            if (primary == null || primary.relocating() == false) {
                // only cancel relocations, let new initializing primaries finish their recovery
                continue;
            }
            if (nodesShutdownMetadata.isNodeMarkedForRemoval(primary.currentNodeId())) {
                // Leave removal-driven moves alone to avoid delaying evacuation.
                continue;
            }
            final ShardRouting target = primary.getTargetRelocatingShard();
            final DiscoveryNode targetNode = state.nodes().get(target.currentNodeId());
            assert targetNode != null : "unexpected missing target node from cluster state " + target.currentNodeId();
            nodeToBlockingShards.computeIfAbsent(targetNode, n -> new ArrayList<>())
                .add(new ShardRecoveryCancellation(primary.shardId(), target.allocationId().getId(), false));
        }

        final Map<DiscoveryNode, CancelRecoveriesAction.Request> cancellationRequests = new HashMap<>();
        nodeToBlockingShards.forEach(
            (node, cancellations) -> cancellationRequests.put(node, new CancelRecoveriesAction.Request(term, version, cancellations))
        );
        return cancellationRequests;
    }

    /// Given the `requests` map of [CancelRecoveriesAction] request per node, sends each request to its target node.
    /// This method is synchronized to prevent concurrent invocations from sending duplicate cancellations.
    private synchronized void sendCancellations(Map<DiscoveryNode, CancelRecoveriesAction.Request> requests) {
        if (enableDirectRecoveryCancellations == false) {
            logger.debug(
                "[{}] is disabled, would have sent direct recovery cancellations {}",
                ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING.getKey(),
                requests
            );
            return;
        }
        final TransportVersion clusterTransportVersion = clusterService.state().getMinTransportVersion();
        if (clusterTransportVersion.supports(CancelRecoveriesAction.DIRECT_RECOVERY_CANCELLATION) == false) {
            logger.debug(
                "not every node in the cluster supports direct recovery cancellation yet, "
                    + "would have sent direct recovery cancellations {}",
                requests
            );
            return;
        }
        final var deduplicatedRequests = deduplicateAndUpdateCache(requests);
        if (deduplicatedRequests.isEmpty()) {
            return;
        }
        logger.debug("sending direct cancellation requests {}", deduplicatedRequests);
        for (var nodeRequest : deduplicatedRequests.entrySet()) {
            sendDirectCancelRecoveriesRequest(nodeRequest.getKey(), nodeRequest.getValue());
        }
    }

    /// Reused across schedule attempts. Releases [pendingSnapshotCancellationPermit] at the start of each run so a
    /// concurrent trigger can queue a follow-up against a (possibly) fresher cluster state.
    /// Each run ([#doRun]) is synchronized, in order to serialize close-in-time executions.
    private class CancelRecoveriesBlockingSnapshotRunnable extends AbstractRunnable {
        @Override
        protected synchronized void doRun() {
            pendingSnapshotCancellationPermit.release();
            final ClusterState currentState = clusterService.state();
            final Map<DiscoveryNode, CancelRecoveriesAction.Request> requests = computeCancellationCandidatesForSnapshots(currentState);
            if (requests.isEmpty()) {
                return;
            }
            sendCancellations(requests);
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("failed to compute or send snapshot recovery cancellations", e);
        }

        @Override
        public void onRejection(Exception e) {
            pendingSnapshotCancellationPermit.release();
            onFailure(e);
        }
    }

    /// Removes cancellations that were already sent recently and updates the cache. A cached entry can be bypassed and
    /// the cancellation re-sent in two cases:
    /// - the cluster term has changed (the data node may have discarded the request from the previous term)
    /// - the new request escalates `cancelIfStarted` from `false` to `true`
    private Map<DiscoveryNode, CancelRecoveriesAction.Request> deduplicateAndUpdateCache(
        Map<DiscoveryNode, CancelRecoveriesAction.Request> requests
    ) {
        final Map<DiscoveryNode, CancelRecoveriesAction.Request> deduped = new HashMap<>();
        for (var nodeRequest : requests.entrySet()) {
            final CancelRecoveriesAction.Request request = nodeRequest.getValue();
            final List<ShardRecoveryCancellation> dedupedCancellations = new ArrayList<>();
            for (ShardRecoveryCancellation cancellation : request.cancellations()) {
                final SentCancellation cached = sentCancellations.get(cancellation.allocationId());
                if (cached == null
                    || cached.term() != request.term()
                    || (cached.cancelIfStarted() == false && cancellation.cancelIfStarted())) {
                    dedupedCancellations.add(cancellation);
                    sentCancellations.put(
                        cancellation.allocationId(),
                        new SentCancellation(request.term(), cancellation.cancelIfStarted())
                    );
                }
            }
            if (dedupedCancellations.isEmpty() == false) {
                final var updatedNodeRequest = new CancelRecoveriesAction.Request(
                    request.term(),
                    request.clusterStateVersion(),
                    dedupedCancellations
                );
                deduped.put(nodeRequest.getKey(), updatedNodeRequest);
            }
        }
        return deduped;
    }

    private void sendDirectCancelRecoveriesRequest(DiscoveryNode node, CancelRecoveriesAction.Request request) {
        transportService.sendRequest(
            node,
            CancelRecoveriesAction.TYPE.name(),
            request,
            new ActionListenerResponseHandler<>(ActionListener.wrap(response -> failShardsCancelledInQueue(node, response), e -> {
                // Request was unsuccessful, invalidate cached entries so another request can try again later.
                // There is a possibility that a close-in-time subsequent request was deduplicated from this one while we
                // were waiting for it to respond. That should be fine, as in all likelihood, this subsequent request
                // would have faced the same transport error and direct cancellation is best-effort anyway.
                for (ShardRecoveryCancellation cancellation : request.cancellations()) {
                    sentCancellations.invalidate(
                        cancellation.allocationId(),
                        new SentCancellation(request.term(), cancellation.cancelIfStarted())
                    );
                }
                // TODO: Retry cancellations on transport failure, and have the data node re-report
                // recoveries it already cancelled-in-queue so the master can still fail those shards
                // if the original response was lost.
                logger.warn(() -> "failed to cancel recoveries on [" + node + "]", e);
            }), CancelRecoveriesAction.Response::new, genericExecutor)
        );
    }

    private void failShardsCancelledInQueue(DiscoveryNode node, CancelRecoveriesAction.Response response) {
        final var state = clusterService.state();
        for (CancelRecoveriesAction.CancelledInQueue cancelled : response.cancelledInQueue()) {
            final ShardId shardId = cancelled.shardId();
            final IndexMetadata indexMetadata = state.metadata().findIndex(shardId.getIndex()).orElse(null);
            if (indexMetadata == null) {
                // index was concurrently deleted, nothing to fail
                continue;
            }

            final var failedShardEntry = new FailedShardEntry(
                shardId,
                cancelled.allocationId(),
                0L,
                "recovery direct cancelled while still queued on the data node",
                new RecoveryCancelledException(shardId, null, node),
                false
            );
            failedShardTaskQueue.submitTask(
                "recovery-direct-cancelled-shard-failed " + failedShardEntry.toStringNoFailureStackTrace(),
                new ShardFailedTaskExecutor.Task(failedShardEntry, ActionListener.noop()),
                null
            );
        }
    }
}
