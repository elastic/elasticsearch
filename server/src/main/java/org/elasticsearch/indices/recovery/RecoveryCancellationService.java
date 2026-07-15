/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.BatchedRerouteService;
import org.elasticsearch.cluster.routing.RerouteClusterStatePublicationListener;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.PendingDirectCancellations;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class RecoveryCancellationService implements ClusterStateListener, RerouteClusterStatePublicationListener {

    private static final Logger logger = LogManager.getLogger(RecoveryCancellationService.class);

    public static final Setting<Boolean> ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING = Setting.boolSetting(
        "indices.recovery.enable_direct_cancellations",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ShardStateAction shardStateAction;
    private final Executor executor;
    private final Supplier<DesiredBalance> desiredBalanceSupplier;
    private final AtomicReference<PendingDirectCancellations> pendingDirectCancellations = new AtomicReference<>(
        PendingDirectCancellations.EMPTY
    );
    private volatile boolean enableDirectRecoveryCancellations;

    @Inject
    @SuppressWarnings("this-escape")
    public RecoveryCancellationService(
        TransportService transportService,
        ClusterService clusterService,
        ShardStateAction shardStateAction,
        ShardsAllocator shardsAllocator,
        RerouteService rerouteService
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.shardStateAction = shardStateAction;
        this.executor = transportService.getThreadPool().generic();
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(
                ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING,
                value -> this.enableDirectRecoveryCancellations = value
            );
        if (shardsAllocator instanceof DesiredBalanceShardsAllocator desiredBalanceShardsAllocator) {
            desiredBalanceShardsAllocator.setDirectCancellationConsumer(this::recordPendingDirectCancellations);
            desiredBalanceShardsAllocator.addListener(this);
            desiredBalanceSupplier = desiredBalanceShardsAllocator::getDesiredBalance;
        } else {
            desiredBalanceSupplier = () -> DesiredBalance.NOT_MASTER;
        }
        if (rerouteService instanceof BatchedRerouteService batchedRerouteService) {
            batchedRerouteService.addListener(this);
        }
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final var pending = pendingDirectCancellations.get();
        if (pending.isEmpty()) {
            return;
        }
        if (event.localNodeMaster() == false) {
            pendingDirectCancellations.set(PendingDirectCancellations.EMPTY);
        }

        ClusterState previousState = event.previousState();
        if (pending.isOutOfDate(previousState.term(), previousState.version(), desiredBalanceSupplier.get().lastConvergedIndex())) {
            pendingDirectCancellations.set(PendingDirectCancellations.EMPTY);
        }
    }

    /// Sends a batch of direct recovery cancellations to a specific data node, lets the node decide
    /// whether to honor each cancellation and fails any shard the data node cancelled straight out of its queue.
    public void sendDirectCancelRecoveriesRequest(DiscoveryNode node, CancelRecoveriesAction.Request request) {
        if (enableDirectRecoveryCancellations == false) {
            logger.debug(
                "[{}] is disabled, would have sent direct recovery cancellations {} to [{}]",
                ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING.getKey(),
                request.cancellations(),
                node
            );
            return;
        }
        final TransportVersion clusterTransportVersion = clusterService.state().getMinTransportVersion();
        if (clusterTransportVersion.supports(CancelRecoveriesAction.DIRECT_RECOVERY_CANCELLATION) == false) {
            logger.debug(
                "not every node in the cluster supports direct recovery cancellation yet, "
                    + "would have sent direct recovery cancellations {} to [{}]",
                request.cancellations(),
                node
            );
            return;
        }
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
                executor
            )
        );
    }

    private void recordPendingDirectCancellations(PendingDirectCancellations pendingDirectCancellations) {
        this.pendingDirectCancellations.set(pendingDirectCancellations);
    }

    @Override
    public void onSuccessfulPublication(long baseStateTerm, long baseStateVersion) {
        applyPendingDirectCancellations(baseStateTerm, baseStateVersion);
    }

    @Override
    public void onAbortedPublication(Exception e) {
        pendingDirectCancellations.getAndSet(PendingDirectCancellations.EMPTY);
    }

    private void applyPendingDirectCancellations(long expectedTerm, long expectedVersion) {
        final var pending = pendingDirectCancellations.getAndSet(PendingDirectCancellations.EMPTY);
        if (pending.isEmpty()) {
            return;
        }
        if (pending.isOutOfDate(expectedTerm, expectedVersion, desiredBalanceSupplier.get().lastConvergedIndex())) {
            return;
        }
        sendCancellations(expectedVersion, pending);
    }

    // TODO: should we try to avoid sending duplicate cancellations by caching what we have already sent?
    private void sendCancellations(long clusterStateVersion, PendingDirectCancellations pendingDirectCancellations) {
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    () -> "failed to send direct recovery cancellations ["
                        + pendingDirectCancellations
                        + "] for cluster state version ["
                        + clusterStateVersion
                        + "]",
                    e
                );
            }

            @Override
            protected void doRun() {
                for (PendingDirectCancellations.Candidates nodeCandidates : pendingDirectCancellations.candidates()) {
                    sendDirectCancelRecoveriesRequest(
                        nodeCandidates.node(),
                        new CancelRecoveriesAction.Request(clusterStateVersion, nodeCandidates.cancellations())
                    );
                }
            }
        });
    }

    private void failShardsCancelledInQueue(DiscoveryNode node, CancelRecoveriesAction.Response response) {
        final var state = clusterService.state();
        for (CancelRecoveriesAction.CancelledInQueue cancelled : response.cancelledInQueue()) {
            final ShardId shardId = cancelled.shardId();
            final IndexMetadata indexMetadata = state.metadata().findIndex(shardId.getIndex()).orElse(null);
            if (indexMetadata == null) {
                // index was concurrently deleted, nothing to fail? TODO: is this safe?
                continue;
            }

            shardStateAction.remoteShardFailed(
                shardId,
                cancelled.allocationId(),
                indexMetadata.primaryTerm(shardId.id()),
                true,
                "recovery direct cancelled while still queued on the data node",
                new RecoveryCancelledException(shardId, null, node),
                ActionListener.noop()
            );
        }
    }
}
