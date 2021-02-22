/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.TransportBroadcastAction;
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
import org.elasticsearch.xpack.core.spatial.action.VectorTileAction;
import org.elasticsearch.xpack.spatial.proto.VectorTile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;


public class TransportVectorTileAction
    extends TransportBroadcastAction<VectorTileAction.Request, VectorTileAction.Response,
    VectorTileAction.ShardRequest, VectorTileAction.ShardResponse> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    @Inject
    public TransportVectorTileAction(ClusterService clusterService,
                                     TransportService transportService,
                                     IndicesService indicesService,
                                     ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver) {
        super(VectorTileAction.NAME, clusterService, transportService, actionFilters,
            indexNameExpressionResolver, VectorTileAction.Request::new, VectorTileAction.ShardRequest::new, "vector_tile_generation");
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    protected GroupShardsIterator<ShardIterator> shards(ClusterState clusterState,
                                                        VectorTileAction.Request request, String[] concreteIndices) {
        // Random routing to limit request to a single shard
        String routing = Integer.toString(Randomness.get().nextInt(1000));
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(clusterState, routing, request.indices());
        return clusterService.operationRouting().searchShards(clusterState, concreteIndices, routingMap, null);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, VectorTileAction.Request request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, VectorTileAction.Request request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }

    @Override
    protected void doExecute(Task task, VectorTileAction.Request request, ActionListener<VectorTileAction.Response> listener) {
        new Async(task, request, listener).start();
    }

    @Override
    protected VectorTileAction.ShardRequest newShardRequest(int numShards, ShardRouting shard, VectorTileAction.Request request) {
        return new VectorTileAction.ShardRequest(shard.shardId(), request);
    }

    @Override
    protected VectorTileAction.ShardResponse shardOperation(VectorTileAction.ShardRequest request, Task task) throws IOException {
        IndexService indexService = indicesService.indexService(request.shardId().getIndex());
        ShardVectorTileBuilder builder = new ShardVectorTileBuilder(indexService, request.shardId(), request.getGeoField(),
            request.getZ(), request.getX(), request.getY());
        return new VectorTileAction.ShardResponse(request.shardId(), builder.execute());
    }

    @Override
    protected VectorTileAction.ShardResponse readShardResponse(StreamInput in) throws IOException {
        return new VectorTileAction.ShardResponse(in);
    }

    @Override
    protected VectorTileAction.Response newResponse(VectorTileAction.Request request,
                                                    AtomicReferenceArray<?> shardsResponses,
                                                    ClusterState clusterState) {
        final List<byte[]> vectorTiles = new ArrayList<>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                throw new ElasticsearchException("missing shard");
            } else if (shardResponse instanceof Exception) {
                throw new ElasticsearchException((Exception) shardResponse);
            } else if (shardResponse instanceof VectorTileAction.ShardResponse) {
                VectorTileAction.ShardResponse vectorTileShardResponse = (VectorTileAction.ShardResponse) shardResponse;
                vectorTiles.add(vectorTileShardResponse.getVectorTile());
            } else {
                throw new ElasticsearchException("invalid shard response, is this even possible?");
            }
        }
        final byte[] merged = vectorTiles.size() == 1 ? vectorTiles.get(0) : merge(vectorTiles);
        return new VectorTileAction.Response(merged);
    }

    private byte[] merge(final List<byte[]> vectorTile) {
        try {
            // TODO: This does not merge layers with the same name?
            // TODO: This deserialize the array in memory, can we avoid it?
            final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
            for (final byte[] tile : vectorTile) {
                tileBuilder.mergeFrom(tile);
            }
            return tileBuilder.build().toByteArray();
        } catch (IOException e) {
            throw new ElasticsearchException("invalid shard response, is this even possible?");
        }
    }

    private class Async extends AsyncBroadcastAction {
        private final VectorTileAction.Request request;
        private final ActionListener<VectorTileAction.Response> listener;

        protected Async(Task task, VectorTileAction.Request request, ActionListener<VectorTileAction.Response> listener) {
            super(task, request, listener);
            this.request = request;
            this.listener = listener;
        }

        @Override
        protected void finishHim() {
            try {
                VectorTileAction.Response resp = newResponse(request, shardsResponses, clusterService.state());
                listener.onResponse(resp);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }
}
