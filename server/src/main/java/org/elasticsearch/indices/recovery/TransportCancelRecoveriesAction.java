/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/// Transport action for batch cancellation of recoveries on a data node.
/// Note that cancellation is best-effort. Recoveries may complete before the cancellation goes through or the request
/// may be ignored past a certain point in the recovery process.
public class TransportCancelRecoveriesAction extends HandledTransportAction<
    CancelRecoveriesAction.Request,
    CancelRecoveriesAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportCancelRecoveriesAction.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final PeerRecoveryTargetService peerRecoveryTargetService;
    private final ThrottlingRecoveryService throttlingRecoveryService;
    private final ExecutorService executor;

    @Inject
    public TransportCancelRecoveriesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndicesService indicesService,
        PeerRecoveryTargetService peerRecoveryTargetService,
        ThrottlingRecoveryService throttlingRecoveryService
    ) {
        super(
            CancelRecoveriesAction.TYPE.name(),
            transportService,
            actionFilters,
            CancelRecoveriesAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.peerRecoveryTargetService = peerRecoveryTargetService;
        this.throttlingRecoveryService = throttlingRecoveryService;
        this.executor = transportService.getThreadPool().executor(ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doExecute(Task task, CancelRecoveriesAction.Request request, ActionListener<CancelRecoveriesAction.Response> listener) {
        RecoveryClusterStateDelay.ensureClusterStateVersion(
            request.clusterStateVersion(),
            clusterService,
            executor,
            null,
            listener,
            l -> processCancellations(request, l)
        );
    }

    private void processCancellations(CancelRecoveriesAction.Request request, ActionListener<CancelRecoveriesAction.Response> listener) {
        final Map<String, ShardId> toCancel = new HashMap<>(request.cancellations().size());
        for (CancelRecoveriesAction.ShardRecoveryCancellation cancellation : request.cancellations()) {
            toCancel.put(cancellation.allocationId(), cancellation.shardId());
        }
        final Set<String> cancelledInQueue = throttlingRecoveryService.cancelRecoveries(toCancel);
        for (CancelRecoveriesAction.ShardRecoveryCancellation cancellation : request.cancellations()) {
            if (cancelledInQueue.contains(cancellation.allocationId()) == false && cancellation.cancelIfStarted()) {
                tryCancelStartedRecovery(cancellation.shardId(), cancellation.allocationId());
            }
        }
        listener.onResponse(new CancelRecoveriesAction.Response(cancelledInQueue));
    }

    private void tryCancelStartedRecovery(ShardId shardId, String allocationId) {
        try {
            tryDirectCancelStartedRecovery(shardId, allocationId);
        } catch (IndexNotFoundException | ShardNotFoundException | IndexShardNotRecoveringException e) {
            logger.debug(
                "unable to directly cancel recovery of shard {} with allocation {}, cancellation stored for later: {}",
                shardId,
                allocationId,
                e
            );
        } catch (Exception e) {
            logger.warn("encountered error when direct cancelling shard {} with allocation {}", shardId, allocationId, e);
        }
    }

    private void tryDirectCancelStartedRecovery(ShardId shardId, String allocationId) {
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.id());

        if (indexShard.routingEntry().allocationId().getId().equals(allocationId) == false) {
            logger.debug(
                "allocation ID mismatch for shard {}, requested={}, actual={}. Ignoring cancellation request",
                shardId,
                allocationId,
                indexShard.routingEntry().allocationId().getId()
            );
            return;
        }
        final IndexShardState state = indexShard.state();

        if (state != IndexShardState.RECOVERING && state != IndexShardState.CREATED) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }

        final ShardRouting shardRouting = indexShard.routingEntry();
        final RecoverySource recoverySource = shardRouting.recoverySource();
        assert recoverySource != null : "recovery source cannot be null when shard is recovering";
        final RecoverySource.Type recoveryType = recoverySource.getType();

        switch (recoveryType) {
            case PEER -> {
                try {
                    // [IndexShard#requestRecoveryCancellation] is used for primary relocation. It sets the pre-handover
                    // cancellation flag to block the potential handover if it hasn't happened yet.
                    // If this is a primary relocation, and the primary handover is already done,
                    // [IndexShard#requestRecoveryCancellation] throws an UnsupportedOperationException and the
                    // cancellation is skipped.
                    // Both this flag and directCancelRecovery below may independently trigger a shard-failure notification
                    // to the master, but the master handles duplicate failures for the same allocation ID gracefully.
                    indexShard.requestRecoveryCancellation(new RecoveryCancelledException(shardId, null, clusterService.localNode()));
                    peerRecoveryTargetService.directCancelRecovery(shardId, allocationId);
                } catch (UnsupportedOperationException e) {
                    logger.debug("cancellation flag cannot be set on {}: {}", shardId, e.getMessage());
                }
            }
            case EXISTING_STORE, SNAPSHOT, LOCAL_SHARDS, EMPTY_STORE -> indexShard.requestRecoveryCancellation(
                new RecoveryCancelledException(shardId, null, clusterService.localNode())
            );
            case RESHARD_SPLIT -> throw new UnsupportedOperationException("direct cancellation during a reshard split is unsupported");
        }
    }
}
