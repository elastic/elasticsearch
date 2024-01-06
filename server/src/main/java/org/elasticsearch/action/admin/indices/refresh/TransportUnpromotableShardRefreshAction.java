/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.unpromotable.TransportBroadcastUnpromotableAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

public class TransportUnpromotableShardRefreshAction extends TransportBroadcastUnpromotableAction<
    UnpromotableShardRefreshRequest,
    ActionResponse.Empty> {

    public static final String NAME = RefreshAction.NAME + "/unpromotable";

    private final IndicesService indicesService;

    @Inject
    public TransportUnpromotableShardRefreshAction(
        ClusterService clusterService,
        TransportService transportService,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        IndicesService indicesService
    ) {
        super(
            NAME,
            clusterService,
            transportService,
            shardStateAction,
            actionFilters,
            UnpromotableShardRefreshRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.REFRESH)
        );
        this.indicesService = indicesService;
    }

    @Override
    protected void unpromotableShardOperation(
        Task task,
        UnpromotableShardRefreshRequest request,
        ActionListener<ActionResponse.Empty> responseListener
    ) {
        ActionListener.run(responseListener, listener -> {
            IndexShard shard = indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
            shard.waitForPrimaryTermAndGeneration(
                request.getPrimaryTerm(),
                request.getSegmentGeneration(),
                listener.map(l -> ActionResponse.Empty.INSTANCE)
            );
        });
    }

    @Override
    protected ActionResponse.Empty combineUnpromotableShardResponses(List<ActionResponse.Empty> empties) {
        return ActionResponse.Empty.INSTANCE;
    }

    @Override
    protected ActionResponse.Empty readResponse(StreamInput in) {
        return ActionResponse.Empty.INSTANCE;
    }

    @Override
    protected ActionResponse.Empty emptyResponse() {
        return ActionResponse.Empty.INSTANCE;
    }
}
