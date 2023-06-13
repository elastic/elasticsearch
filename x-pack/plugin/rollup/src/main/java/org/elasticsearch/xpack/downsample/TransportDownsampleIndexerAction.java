/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.downsample;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.downsample.DownsampleIndexerAction;
import org.elasticsearch.xpack.core.rollup.action.RollupShardTask;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.xpack.rollup.Rollup.TASK_THREAD_POOL_NAME;

/**
 * A {@link TransportBroadcastAction} that rollups all the shards of a source index into a new rollup index.
 *
 * TODO: Enforce that we don't retry on another replica if we throw an error after sending some buckets.
 */
public class TransportDownsampleIndexerAction extends TransportBroadcastAction<
    DownsampleIndexerAction.Request,
    DownsampleIndexerAction.Response,
    DownsampleIndexerAction.ShardDownsampleRequest,
    DownsampleIndexerAction.ShardDownsampleResponse> {

    private final Client client;
    private final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportDownsampleIndexerAction(
        Client client,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            DownsampleIndexerAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            DownsampleIndexerAction.Request::new,
            DownsampleIndexerAction.ShardDownsampleRequest::new,
            TASK_THREAD_POOL_NAME
        );
        this.client = new OriginSettingClient(client, ClientHelper.ROLLUP_ORIGIN);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected GroupShardsIterator<ShardIterator> shards(
        ClusterState clusterState,
        DownsampleIndexerAction.Request request,
        String[] concreteIndices
    ) {
        if (concreteIndices.length > 1) {
            throw new IllegalArgumentException("multiple indices: " + Arrays.toString(concreteIndices));
        }

        final GroupShardsIterator<ShardIterator> groups = clusterService.operationRouting()
            .searchShards(clusterState, concreteIndices, null, null);
        for (ShardIterator group : groups) {
            // fails fast if any non-active groups
            if (group.size() == 0) {
                throw new NoShardAvailableActionException(group.shardId());
            }
        }
        return groups;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, DownsampleIndexerAction.Request request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(
        ClusterState state,
        DownsampleIndexerAction.Request request,
        String[] concreteIndices
    ) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }

    @Override
    protected void doExecute(
        Task task,
        DownsampleIndexerAction.Request request,
        ActionListener<DownsampleIndexerAction.Response> listener
    ) {
        new Async(task, request, listener).start();
    }

    @Override
    protected DownsampleIndexerAction.ShardDownsampleRequest newShardRequest(
        int numShards,
        ShardRouting shard,
        DownsampleIndexerAction.Request request
    ) {
        long totalDocCount = client.admin()
            .indices()
            .stats(new IndicesStatsRequest().indices(request.getDownsampleRequest().getSourceIndex()))
            .actionGet()
            .getTotal().docs.getCount();
        long totalShardDocCount = Arrays.stream(
            client.admin()
                .indices()
                .stats(new IndicesStatsRequest().indices(request.getDownsampleRequest().getSourceIndex()))
                .actionGet()
                .getShards()
        ).filter(shardStats -> shardStats.getShardRouting().shardId().equals(shard.shardId())).toList().get(0).getStats().docs.getCount();
        return new DownsampleIndexerAction.ShardDownsampleRequest(shard.shardId(), totalDocCount, totalShardDocCount, request);
    }

    @Override
    protected DownsampleIndexerAction.ShardDownsampleResponse shardOperation(
        DownsampleIndexerAction.ShardDownsampleRequest request,
        Task task
    ) throws IOException {
        IndexService indexService = indicesService.indexService(request.shardId().getIndex());
        RollupShardIndexer indexer = new RollupShardIndexer(
            (RollupShardTask) task,
            client,
            indexService,
            request.shardId(),
            request.getRollupIndex(),
            request.getRollupConfig(),
            request.getMetricFields(),
            request.getLabelFields()
        );
        return indexer.execute();
    }

    @Override
    protected DownsampleIndexerAction.ShardDownsampleResponse readShardResponse(StreamInput in) throws IOException {
        return new DownsampleIndexerAction.ShardDownsampleResponse(in);
    }

    @Override
    protected DownsampleIndexerAction.Response newResponse(
        DownsampleIndexerAction.Request request,
        AtomicReferenceArray<?> shardsResponses,
        ClusterState clusterState
    ) {
        long numIndexed = 0;
        int successfulShards = 0;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                throw new ElasticsearchException("missing shard");
            } else if (shardResponse instanceof DownsampleIndexerAction.ShardDownsampleResponse r) {
                successfulShards++;
                numIndexed += r.getNumIndexed();
            } else if (shardResponse instanceof Exception e) {
                throw new ElasticsearchException(e);
            } else {
                assert false : "unknown response [" + shardResponse + "]";
                throw new IllegalStateException("unknown response [" + shardResponse + "]");
            }
        }
        return new DownsampleIndexerAction.Response(true, shardsResponses.length(), successfulShards, 0, numIndexed);
    }

    private class Async extends AsyncBroadcastAction {
        private final DownsampleIndexerAction.Request request;
        private final ActionListener<DownsampleIndexerAction.Response> listener;

        protected Async(Task task, DownsampleIndexerAction.Request request, ActionListener<DownsampleIndexerAction.Response> listener) {
            super(task, request, listener);
            this.request = request;
            this.listener = listener;
        }

        @Override
        protected void finishHim() {
            try {
                DownsampleIndexerAction.Response resp = newResponse(request, shardsResponses, clusterService.state());
                listener.onResponse(resp);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }
}
