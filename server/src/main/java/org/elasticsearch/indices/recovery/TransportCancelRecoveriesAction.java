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
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotRecoveringException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.ShardRecoveryNotCancellableException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;

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
    private final ThrottlingRecoveryService throttlingRecoveryService;
    private final ExecutorService executor;

    @Inject
    public TransportCancelRecoveriesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        IndicesService indicesService,
        ThrottlingRecoveryService throttlingRecoveryService
    ) {
        super(
            CancelRecoveriesAction.TYPE.name(),
            transportService,
            actionFilters,
            CancelRecoveriesAction.Request::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.clusterService = clusterService;
        this.indicesService = indicesService;
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
        assert Transports.assertNotTransportThread("TransportCancelRecoveriesAction must not run on a transport thread");
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
        } catch (IndexShardNotRecoveringException e) {
            logger.debug(
                "unable to directly cancel recovery of shard {} with allocation {}, shard is not recovering",
                shardId,
                allocationId,
                e
            );
        } catch (IndexNotFoundException | ShardNotFoundException e) {
            logger.debug(
                "unable to directly cancel recovery of shard {} with allocation {}, cancellation was recorded "
                    + "in ThrottlingRecoveryService in case the shard is being created and has not yet reached the queue: {}",
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

        try {
            switch (recoveryType) {
                case EXISTING_STORE, SNAPSHOT, LOCAL_SHARDS, EMPTY_STORE, PEER -> indexShard.requestRecoveryCancellation();
                case RESHARD_SPLIT -> throw new ShardRecoveryNotCancellableException(
                    shardId,
                    recoveryType + " recoveries do not currently support direct cancellation of started recoveries"
                );
            }
        } catch (ShardRecoveryNotCancellableException e) {
            logger.debug("cancellation flag cannot be set on {} for recovery type {}: {}", shardId, recoveryType, e.getMessage());
        }
    }
}
