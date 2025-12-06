/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.Executor;

public class PostWriteRefresh {

    public static final String POST_WRITE_REFRESH_ORIGIN = "post_write_refresh";
    public static final String FORCED_REFRESH_AFTER_INDEX = "refresh_flag_index";
    private final TransportService transportService;
    private final Executor refreshExecutor;

    public PostWriteRefresh(final TransportService transportService) {
        this.transportService = transportService;
        this.refreshExecutor = transportService.getThreadPool().executor(ThreadPool.Names.REFRESH);
    }

    public void refreshShard(
        WriteRequest.RefreshPolicy policy,
        IndexShard indexShard,
        @Nullable Translog.Location location,
        ActionListener<Boolean> listener,
        @Nullable TimeValue postWriteRefreshTimeout
    ) {
        switch (policy) {
            case NONE -> listener.onResponse(false);
            case WAIT_UNTIL -> waitUntil(indexShard, location, new ActionListener<>() {
                @Override
                public void onResponse(Boolean forced) {
                    if (location != null && indexShard.routingEntry().isSearchable() == false) {
                        refreshUnpromotables(indexShard, location, listener, forced, postWriteRefreshTimeout);
                    } else {
                        listener.onResponse(forced);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
            case IMMEDIATE -> immediate(indexShard, listener.delegateFailureAndWrap((l, r) -> {
                if (indexShard.getReplicationGroup().getRoutingTable().unpromotableShards().size() > 0) {
                    sendUnpromotableRequests(indexShard, r.generation(), true, l, postWriteRefreshTimeout);
                } else {
                    l.onResponse(true);
                }
            }));
            default -> throw new IllegalArgumentException("unknown refresh policy: " + policy);
        }
    }

    public static void refreshReplicaShard(
        WriteRequest.RefreshPolicy policy,
        IndexShard indexShard,
        @Nullable Translog.Location location,
        ActionListener<Boolean> listener
    ) {
        switch (policy) {
            case NONE -> listener.onResponse(false);
            case WAIT_UNTIL -> waitUntil(indexShard, location, listener);
            case IMMEDIATE -> immediate(indexShard, listener.map(r -> true));
            default -> throw new IllegalArgumentException("unknown refresh policy: " + policy);
        }
    }

    private static void immediate(IndexShard indexShard, ActionListener<Engine.RefreshResult> listener) {
        indexShard.externalRefresh(FORCED_REFRESH_AFTER_INDEX, listener);
    }

    private static void waitUntil(IndexShard indexShard, Translog.Location location, ActionListener<Boolean> listener) {
        if (location != null) {
            indexShard.addRefreshListener(location, listener::onResponse);
        } else {
            listener.onResponse(false);
        }
    }

    private void refreshUnpromotables(
        IndexShard indexShard,
        Translog.Location location,
        ActionListener<Boolean> listener,
        boolean forced,
        @Nullable TimeValue postWriteRefreshTimeout
    ) {
        Engine engineOrNull = indexShard.getEngineOrNull();
        if (engineOrNull == null) {
            listener.onFailure(new AlreadyClosedException("Engine closed during refresh."));
            return;
        }

        engineOrNull.addFlushListener(location, listener.delegateFailureAndWrap((l, generation) -> {
            try (
                ThreadContext.StoredContext ignore = transportService.getThreadPool()
                    .getThreadContext()
                    .stashWithOrigin(POST_WRITE_REFRESH_ORIGIN)
            ) {
                sendUnpromotableRequests(indexShard, generation, forced, l, postWriteRefreshTimeout);
            }
        }));
    }

    private void sendUnpromotableRequests(
        IndexShard indexShard,
        long generation,
        boolean wasForced,
        ActionListener<Boolean> listener,
        @Nullable TimeValue postWriteRefreshTimeout
    ) {
        UnpromotableShardRefreshRequest unpromotableReplicaRequest = new UnpromotableShardRefreshRequest(
            indexShard.getReplicationGroup().getRoutingTable(),
            indexShard.getOperationPrimaryTerm(),
            generation,
            true,
            postWriteRefreshTimeout
        );
        transportService.sendRequest(
            transportService.getLocalNode(),
            TransportUnpromotableShardRefreshAction.NAME,
            unpromotableReplicaRequest,
            TransportRequestOptions.timeout(postWriteRefreshTimeout),
            new ActionListenerResponseHandler<>(listener.safeMap(r -> wasForced), in -> ActionResponse.Empty.INSTANCE, refreshExecutor)
        );
    }

}
