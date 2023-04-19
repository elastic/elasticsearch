/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class UnpromotableRefreshService {

    private final TransportService transportService;
    private static final Logger logger = LogManager.getLogger(UnpromotableRefreshService.class);

    @Inject
    public UnpromotableRefreshService(TransportService transportService) {
        this.transportService = transportService;
    }

    private UnpromotableRefreshService() {
        this.transportService = null;
    }

    public UnpromotableRefresher getShardUnpromotableRefresher(IndexShard indexShard) {
        return (generation, listener) -> refresh(generation, indexShard, listener);
    }

    private void refresh(long generation, IndexShard indexShard, ActionListener<Void> listener) {
        UnpromotableShardRefreshRequest unpromotableReplicaRequest = new UnpromotableShardRefreshRequest(
            indexShard.getReplicationGroup().getRoutingTable(),
            generation
        );

        transportService.sendRequest(
            transportService.getLocalNode(),
            TransportUnpromotableShardRefreshAction.NAME,
            unpromotableReplicaRequest,
            new ActionListenerResponseHandler<>(
                listener.delegateFailure((l, o) -> l.onResponse(null)),
                (in) -> ActionResponse.Empty.INSTANCE,
                ThreadPool.Names.REFRESH
            )
        );
    }

    @FunctionalInterface
    public interface UnpromotableRefresher {
        void refresh(long generation, ActionListener<Void> listener);
    }

    public static final UnpromotableRefreshService EMPTY = new UnpromotableRefreshService() {
        @Override
        public UnpromotableRefresher getShardUnpromotableRefresher(IndexShard indexShard) {
            return (generation, listener) -> {};
        }
    };
}
