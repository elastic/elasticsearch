/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.rollup.action.RollupIndexerAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.xpack.rollup.Rollup.TASK_THREAD_POOL_NAME;

/**
 * A {@link TransportBroadcastAction} that rollups all the shards of a single index into a new one.
 *
 * TODO: Enforce that we don't retry on another replica if we throw an error after sending some buckets.
 */
public class TransportRollupIndexerAction extends TransportBroadcastAction<
    RollupIndexerAction.Request,
    RollupIndexerAction.Response,
    RollupIndexerAction.ShardRequest,
    RollupIndexerAction.ShardResponse> {

    private static final int SORTER_RAM_SIZE_MB = 100;

    private final Client client;
    private final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportRollupIndexerAction(
        Client client,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            RollupIndexerAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            RollupIndexerAction.Request::new,
            RollupIndexerAction.ShardRequest::new,
            TASK_THREAD_POOL_NAME
        );
        this.client = new OriginSettingClient(client, ClientHelper.ROLLUP_ORIGIN);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected GroupShardsIterator<ShardIterator> shards(
        ClusterState clusterState,
        RollupIndexerAction.Request request,
        String[] concreteIndices
    ) {
        if (concreteIndices.length > 1) {
            throw new IllegalArgumentException("multiple indices: " + Arrays.toString(concreteIndices));
        }
        // Random routing to limit request to a single shard
        String routing = Integer.toString(Randomness.get().nextInt(1000));
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, routing, request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, null);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, RollupIndexerAction.Request request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, RollupIndexerAction.Request request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }

    @Override
    protected void doExecute(Task task, RollupIndexerAction.Request request, ActionListener<RollupIndexerAction.Response> listener) {
        new Async(task, request, listener).start();
    }

    @Override
    protected RollupIndexerAction.ShardRequest newShardRequest(int numShards, ShardRouting shard, RollupIndexerAction.Request request) {
        return new RollupIndexerAction.ShardRequest(shard.shardId(), request);
    }

    @Override
    protected RollupIndexerAction.ShardResponse shardOperation(RollupIndexerAction.ShardRequest request, Task task) throws IOException {
        IndexService indexService = indicesService.indexService(request.shardId().getIndex());
        String tmpIndexName = ".rolluptmp-" + request.getRollupIndex();
        RollupShardIndexer indexer = new RollupShardIndexer(
            client,
            indexService,
            request.shardId(),
            request.getRollupConfig(),
            tmpIndexName,
            SORTER_RAM_SIZE_MB
        );
        indexer.execute();
        return new RollupIndexerAction.ShardResponse(request.shardId());
    }

    @Override
    protected RollupIndexerAction.ShardResponse readShardResponse(StreamInput in) throws IOException {
        return new RollupIndexerAction.ShardResponse(in);
    }

    @Override
    protected RollupIndexerAction.Response newResponse(
        RollupIndexerAction.Request request,
        AtomicReferenceArray<?> shardsResponses,
        ClusterState clusterState
    ) {
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                throw new ElasticsearchException("missing shard");
            } else if (shardResponse instanceof Exception) {
                throw new ElasticsearchException((Exception) shardResponse);
            }
        }
        return new RollupIndexerAction.Response(true);
    }

    private class Async extends AsyncBroadcastAction {
        private final RollupIndexerAction.Request request;
        private final ActionListener<RollupIndexerAction.Response> listener;

        protected Async(Task task, RollupIndexerAction.Request request, ActionListener<RollupIndexerAction.Response> listener) {
            super(task, request, listener);
            this.request = request;
            this.listener = listener;
        }

        @Override
        protected void finishHim() {
            try {
                RollupIndexerAction.Response resp = newResponse(request, shardsResponses, clusterService.state());
                listener.onResponse(resp);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }
}
