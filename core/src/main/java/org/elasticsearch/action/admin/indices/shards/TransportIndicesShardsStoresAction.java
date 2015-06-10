/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.shards;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.elasticsearch.action.admin.indices.shards.IndicesShardsStoresRequest.*;

/**
 * Transport action that reads the cluster state for shards with the requested criteria (see {@link ShardState}) of specific indices
 * and fetches shard store information from all the nodes using {@link TransportNodesListGatewayStartedShards}
 */
public class TransportIndicesShardsStoresAction extends TransportMasterNodeReadAction<IndicesShardsStoresRequest, IndicesShardsStoresResponse> {

    private final TransportNodesListGatewayStartedShards listShardStoresInfo;

    @Inject
    public TransportIndicesShardsStoresAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                              TransportNodesListGatewayStartedShards listShardStoresInfo) {
        super(settings, IndicesShardsStoresAction.NAME, transportService, clusterService, threadPool, actionFilters, IndicesShardsStoresRequest.class);
        this.listShardStoresInfo = listShardStoresInfo;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IndicesShardsStoresResponse newResponse() {
        return new IndicesShardsStoresResponse();
    }

    @Override
    protected void masterOperation(IndicesShardsStoresRequest request, ClusterState state, ActionListener<IndicesShardsStoresResponse> listener) {
        final RoutingTable indexRoutingTables = state.routingTable();
        // collect all relevant shards of the requested indices
        // for fetching store infos
        final List<ShardRouting> shardRoutings;
        switch (request.shardState()) {
            case ALLOCATED:
                // TODO: should we also add shards with RELOCATING or INITIALIZING state here?
                shardRoutings = indexRoutingTables.shardsWithState(ShardRoutingState.STARTED);
                break;
            case UNALLOCATED:
                shardRoutings = indexRoutingTables.shardsWithState(ShardRoutingState.UNASSIGNED);
                break;
            case ALL:
                shardRoutings = indexRoutingTables.allShards();
                break;
            default:
                // bogus
                listener.onFailure(new IllegalArgumentException("Invalid shard state criteria"));
                return;
        }
        final Set<String> requestedIndices = new HashSet<>(Arrays.asList(request.indices()));
        final Set<ShardId> shardsToFetch = new HashSet<>();
        for (ShardRouting shard : shardRoutings) {
            if (requestedIndices.size() == 0 || requestedIndices.contains(shard.shardId().index().getName())) {
                // Should we also filter by shard.primary(),
                // in doing so, no store info will be fetched
                // for UNASSIGNED replicas (yellow state)
                shardsToFetch.add(shard.shardId());
            }
        }

        // async fetch store infos for relevant shard from all the nodes
        if (shardsToFetch.size() > 0) {
            final DiscoveryNodes nodes = state.nodes();
            final MetaData metaData = state.metaData();
            // NOTE: instead of fetching shard store info one by one from every node (nShards * nNodes requests)
            // we could fetch all shard store info from every node once (nNodes requests)
            // we have to implement a TransportNodesAction instead of using TransportNodesListGatewayStartedShards
            // for fetching shard stores info, that operates on a list of shards instead of a single shard
            AsyncShardsStoresInfoFetches asyncShardsStoresInfoFetches = new AsyncShardsStoresInfoFetches(nodes, metaData, shardsToFetch.size(), listener);
            asyncShardsStoresInfoFetches.fetch(shardsToFetch);
        } else {
            listener.onResponse(new IndicesShardsStoresResponse());
        }
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesShardsStoresRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, request.indices());
    }

    private class AsyncShardsStoresInfoFetches {
        private final DiscoveryNodes nodes;
        private final MetaData metaData;
        private final CountDown expectedOps;
        private final Queue<InternalAsyncFetch.Response> fetchResponses;
        private final ActionListener<IndicesShardsStoresResponse> listener;

        AsyncShardsStoresInfoFetches(DiscoveryNodes nodes, MetaData metaData, int expectedOps, ActionListener<IndicesShardsStoresResponse> listener) {
            this.nodes = nodes;
            this.metaData = metaData;
            this.listener = listener;
            this.expectedOps = new CountDown(expectedOps);
            this.fetchResponses = new ConcurrentLinkedQueue<>();
        }

        void fetch(Collection<ShardId> shardIds) {
            for (ShardId shardId : shardIds) {
                InternalAsyncFetch fetch = new InternalAsyncFetch(logger, "shard_stores", shardId, listShardStoresInfo);
                fetch.fetchData(nodes, metaData, Collections.<String>emptySet());
            }
        }

        private class InternalAsyncFetch extends AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> {

            InternalAsyncFetch(ESLogger logger, String type, ShardId shardId, TransportNodesListGatewayStartedShards action) {
                super(logger, type, shardId, action);
            }

            @Override
            protected synchronized void processAsyncFetch(ShardId shardId, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards[] responses, FailedNodeException[] failures) {
                fetchResponses.add(new Response(shardId, responses, failures));
                if (expectedOps.countDown()) {
                    finish();
                }
            }

            void finish() {
                ImmutableOpenMap.Builder<String, ImmutableOpenIntMap<java.util.List<IndicesShardsStoresResponse.ShardStoreStatus>>> shardsResponseBuilder = ImmutableOpenMap.builder();
                ImmutableList.Builder<IndicesShardsStoresResponse.Failure> failureBuilder = ImmutableList.builder();
                while (!fetchResponses.isEmpty()) {
                    Response res = fetchResponses.remove();
                    ImmutableOpenIntMap<java.util.List<IndicesShardsStoresResponse.ShardStoreStatus>> indexShards = shardsResponseBuilder.get(res.shardId.getIndex());
                    final ImmutableOpenIntMap.Builder<java.util.List<IndicesShardsStoresResponse.ShardStoreStatus>> indexShardsBuilder;
                    if (indexShards == null) {
                        indexShardsBuilder = ImmutableOpenIntMap.builder();
                    } else {
                        indexShardsBuilder = ImmutableOpenIntMap.builder(indexShards);
                    }
                    java.util.List<IndicesShardsStoresResponse.ShardStoreStatus> shardStoreStatuses = indexShardsBuilder.get(res.shardId.id());
                    if (shardStoreStatuses == null) {
                        shardStoreStatuses = new ArrayList<>();
                    }
                    for (TransportNodesListGatewayStartedShards.NodeGatewayStartedShards response : res.responses) {
                        if (shardExistsInNode(response)) {
                            shardStoreStatuses.add(new IndicesShardsStoresResponse.ShardStoreStatus(response.getNode(), response.version(), response.exception()));
                        }
                    }
                    indexShardsBuilder.put(res.shardId.id(), shardStoreStatuses);
                    shardsResponseBuilder.put(res.shardId.getIndex(), indexShardsBuilder.build());
                    for (FailedNodeException failure : res.failures) {
                        failureBuilder.add(new IndicesShardsStoresResponse.Failure(failure.nodeId(), res.shardId.getIndex(), res.shardId.id(), failure.getCause()));
                    }
                }
                listener.onResponse(new IndicesShardsStoresResponse(shardsResponseBuilder.build(), failureBuilder.build()));
            }

            /**
             * A shard exists/existed in a node only if shard state file exists in the node
             */
            private boolean shardExistsInNode(final TransportNodesListGatewayStartedShards.NodeGatewayStartedShards response) {
                return response.exception() != null || response.version() != -1;
            }

            @Override
            protected void reroute(ShardId shardId, String reason) {
                // no-op
            }

            public class Response {
                private final ShardId shardId;
                private final TransportNodesListGatewayStartedShards.NodeGatewayStartedShards[] responses;
                private final FailedNodeException[] failures;

                public Response(ShardId shardId, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards[] responses, FailedNodeException[] failures) {
                    this.shardId = shardId;
                    this.responses = responses;
                    this.failures = failures;
                }
            }
        }
    }

}
