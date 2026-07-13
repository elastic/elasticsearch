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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DirectCancellationCandidates;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

public class RecoveryCancellationService {

    private static final Logger logger = LogManager.getLogger(RecoveryCancellationService.class);

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ShardStateAction shardStateAction;
    private final Executor executor;

    @Inject
    @SuppressWarnings("this-escape")
    public RecoveryCancellationService(
        TransportService transportService,
        ClusterService clusterService,
        ShardStateAction shardStateAction,
        ShardsAllocator shardsAllocator
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.shardStateAction = shardStateAction;
        this.executor = transportService.getThreadPool().generic();
        if (shardsAllocator instanceof DesiredBalanceShardsAllocator desiredBalanceShardsAllocator) {
            desiredBalanceShardsAllocator.setDirectCancellationConsumer(this::directCancelRecoveries);
        }
    }

    /// Sends a batch of direct recovery cancellations to a specific data node, lets the node decide
    /// whether to honor each cancellation and fails any shard the data node cancelled straight out of its queue.
    public void sendDirectCancelRecoveriesRequest(DiscoveryNode node, CancelRecoveriesAction.Request request) {
        transportService.sendRequest(
            node,
            CancelRecoveriesAction.TYPE.name(),
            request,
            new ActionListenerResponseHandler<>(
                ActionListener.wrap(
                    this::failShardsCancelledInQueue,
                    e -> logger.warn(() -> "failed to cancel recoveries on [" + node + "]", e)
                ),
                CancelRecoveriesAction.Response::new,
                executor
            )
        );
    }

    // TODO: should we try to avoid sending duplicate cancellations by caching what we have already sent?
    private void directCancelRecoveries(long clusterStateVersion, DirectCancellationCandidates candidates) {
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                logger.warn(
                    () -> "failed to send direct recovery cancellations ["
                        + candidates
                        + "] for cluster state version ["
                        + clusterStateVersion
                        + "]",
                    e
                );
            }

            @Override
            protected void doRun() {
                for (DirectCancellationCandidates.Candidates nodeCandidates : candidates.candidates()) {
                    sendDirectCancelRecoveriesRequest(
                        nodeCandidates.node(),
                        new CancelRecoveriesAction.Request(clusterStateVersion, nodeCandidates.cancellations())
                    );
                }
            }
        });
    }

    private void failShardsCancelledInQueue(CancelRecoveriesAction.Response response) {
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
                null,
                ActionListener.noop()
            );
        }
    }
}
