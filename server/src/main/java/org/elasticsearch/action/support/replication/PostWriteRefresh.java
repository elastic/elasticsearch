/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class PostWriteRefresh {

    public static final String FORCED_REFRESH_AFTER_INDEX = "refresh_flag_index";
    private final TransportService transportService;

    public PostWriteRefresh(@Nullable TransportService transportService) {
        this.transportService = transportService;
    }

    public void refreshShard(
        WriteRequest.RefreshPolicy policy,
        IndexShard indexShard,
        Translog.Location location,
        ActionListener<Boolean> listener
    ) {
        doRefreshShard(policy, true, transportService, indexShard, location, listener);
    }

    public static void refreshReplicaShard(
        WriteRequest.RefreshPolicy policy,
        IndexShard indexShard,
        Translog.Location location,
        ActionListener<Boolean> listener
    ) {
        doRefreshShard(policy, false, null, indexShard, location, listener);
    }

    private static void doRefreshShard(
        WriteRequest.RefreshPolicy policy,
        boolean isPrimary,
        @Nullable TransportService transportService,
        IndexShard indexShard,
        Translog.Location location,
        ActionListener<Boolean> listener
    ) {
        switch (policy) {
            case NONE -> listener.onResponse(false);
            case WAIT_UNTIL -> {
                if (location != null) {
                    indexShard.addRefreshListener(location, refreshResult -> {
                        Engine engineOrNull = indexShard.getEngineOrNull();
                        if (engineOrNull == null) {
                            listener.onFailure(new EngineException(indexShard.shardId(), "Engine closed during refresh."));
                        } else {
                            afterRefresh(
                                indexShard,
                                isPrimary,
                                transportService,
                                listener,
                                refreshResult.refreshForced(),
                                engineOrNull.getCurrentGeneration()
                            );
                        }
                    });
                } else {
                    listener.onResponse(false);
                }
            }
            case IMMEDIATE -> {
                Engine.RefreshResult refreshResult = indexShard.refresh(FORCED_REFRESH_AFTER_INDEX);
                afterRefresh(indexShard, isPrimary, transportService, listener, true, refreshResult.generation());
            }
            default -> throw new IllegalArgumentException("unknown refresh policy: " + policy);
        }
    }

    private static void afterRefresh(
        IndexShard indexShard,
        boolean isPrimary,
        @Nullable TransportService transportService,
        ActionListener<Boolean> listener,
        boolean wasForced,
        long generation
    ) {
        if (isPrimary && indexShard.getReplicationGroup().getRoutingTable().unpromotableShards().size() > 0) {
            assert transportService != null : "TransportService cannot be null if unpromotables present";
            UnpromotableShardRefreshRequest unpromotableReplicaRequest = new UnpromotableShardRefreshRequest(
                indexShard.getReplicationGroup().getRoutingTable(),
                generation
            );
            transportService.sendRequest(
                transportService.getLocalNode(),
                TransportUnpromotableShardRefreshAction.NAME,
                unpromotableReplicaRequest,
                new ActionListenerResponseHandler<>(
                    listener.delegateFailure((l, r) -> l.onResponse(wasForced)),
                    (in) -> ActionResponse.Empty.INSTANCE,
                    ThreadPool.Names.REFRESH
                )
            );
        } else {
            listener.onResponse(wasForced);
        }
    }

}
