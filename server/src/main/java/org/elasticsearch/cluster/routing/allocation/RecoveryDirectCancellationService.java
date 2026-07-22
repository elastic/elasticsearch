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
import org.elasticsearch.cluster.action.shard.FailedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardFailedTaskExecutor;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.CancelRecoveriesAction;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.indices.recovery.ShardRecoveryCancellation;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/// Master-side service that proactively cancels shard recoveries that are no longer wanted according to the current
/// [DesiredBalance].
///
/// When the desired balance changes and an initializing shard is no longer assigned to its current node, this service
/// sends a [CancelRecoveriesAction] transport request to the data node so that the recovery is cancelled as soon as
/// possible rather than waiting for it to complete before the next allocation round can move the shard.
///
/// Every operation in this service is fire-and-forget. Errors are all handled by logging a warning or silently ignoring
/// the result. In all failure cases the affected shards are eventually reassigned through the normal reroute/shard-failed
/// path.
public class RecoveryDirectCancellationService {

    private static final Logger logger = LogManager.getLogger(RecoveryDirectCancellationService.class);

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
    private volatile boolean enableDirectRecoveryCancellations;

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
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(
                ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING,
                value -> this.enableDirectRecoveryCancellations = value
            );
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
    public void computeAndSubmitCancellations(DesiredBalance desiredBalance, RoutingAllocation routingAllocation) {
        genericExecutor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                final var requests = computeDirectCancellationCandidates(desiredBalance, routingAllocation);
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
        });
    }

    /// Given the `requests` map of [CancelRecoveriesAction] request per node, sends each request to its target node.
    private void sendCancellations(Map<DiscoveryNode, CancelRecoveriesAction.Request> requests) {
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
        // TODO: we should deduplicate those requests. Indeed, we might get several new desired balances close in time,
        // that will trigger new cancellations before the previous ones have had time to take effect.
        for (var nodeRequest : requests.entrySet()) {
            sendDirectCancelRecoveriesRequest(nodeRequest.getKey(), nodeRequest.getValue());
        }
    }

    void sendDirectCancelRecoveriesRequest(DiscoveryNode node, CancelRecoveriesAction.Request request) {
        transportService.sendRequest(
            node,
            CancelRecoveriesAction.TYPE.name(),
            request,
            new ActionListenerResponseHandler<>(
                ActionListener.wrap(
                    response -> failShardsCancelledInQueue(node, response),
                    e -> logger.warn(() -> "failed to cancel recoveries on [" + node + "]", e)
                ),
                CancelRecoveriesAction.Response::new,
                genericExecutor
            )
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

    /// Returns a map of [CancelRecoveriesAction.Request] per relevant data node. Each request lists the initializing
    /// shards on that node that are no longer heading to a desired location according to `desiredBalance` and for
    /// which a recovery cancellation will be requested. Each [ShardRecoveryCancellation] carries a `cancelIfStarted`
    /// flag, determined by recovery type and allocation decider result, indicating whether the recovery should be
    /// interrupted even after it has started work.
    static Map<DiscoveryNode, CancelRecoveriesAction.Request> computeDirectCancellationCandidates(
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
}
