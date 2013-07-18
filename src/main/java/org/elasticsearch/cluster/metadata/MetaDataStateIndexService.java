/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class MetaDataStateIndexService extends AbstractComponent {

    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.READ_WRITE);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    @Inject
    public MetaDataStateIndexService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        super(settings);
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    public void closeIndex(final Request request, final Listener listener) {
        if (request.indices == null || request.indices.length == 0) {
            throw new ElasticSearchIllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices);
        clusterService.submitStateUpdateTask("close-indices " + indicesAsString, Priority.URGENT, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                List<String> indicesToClose = new ArrayList<String>();
                for (String index : request.indices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        listener.onFailure(new IndexMissingException(new Index(index)));
                        return currentState;
                    }
                    IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
                    for (IndexShardRoutingTable shard: indexRoutingTable) {
                        if (!shard.primaryAllocatedPostApi()) {
                            listener.onFailure(new IndexPrimaryShardNotAllocatedException(new Index(index)));
                            return currentState;
                        }
                    }

                    if (indexMetaData.state() != IndexMetaData.State.CLOSE) {
                        indicesToClose.add(index);
                    }
                }

                if (indicesToClose.isEmpty()) {
                    listener.onResponse(new Response(true));
                    return currentState;
                }

                logger.info("closing indices [{}]", indicesAsString);

                MetaData.Builder mdBuilder = MetaData.builder()
                        .metaData(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                for (String index : indicesToClose) {
                    mdBuilder.put(IndexMetaData.newIndexMetaDataBuilder(currentState.metaData().index(index)).state(IndexMetaData.State.CLOSE));
                    blocksBuilder.addIndexBlock(index, INDEX_CLOSED_BLOCK);
                }

                ClusterState updatedState = ClusterState.builder().state(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder()
                        .routingTable(currentState.routingTable());
                for (String index : indicesToClose) {
                    rtBuilder.remove(index);
                }

                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder().state(updatedState).routingTable(rtBuilder).build());

                return ClusterState.builder().state(updatedState).routingResult(routingResult).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
                listener.onResponse(new Response(true));
            }
        });
    }

    public void openIndex(final Request request, final Listener listener) {
        if (request.indices == null || request.indices.length == 0) {
            throw new ElasticSearchIllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices);
        clusterService.submitStateUpdateTask("open-indices " + indicesAsString, Priority.URGENT, new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                List<String> indicesToOpen = new ArrayList<String>();
                for (String index : request.indices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        listener.onFailure(new IndexMissingException(new Index(index)));
                        return currentState;
                    }
                    if (indexMetaData.state() != IndexMetaData.State.OPEN) {
                        indicesToOpen.add(index);
                    }
                }

                if (indicesToOpen.isEmpty()) {
                    listener.onResponse(new Response(true));
                    return currentState;
                }

                logger.info("opening indices [{}]", indicesAsString);

                MetaData.Builder mdBuilder = MetaData.builder()
                        .metaData(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                for (String index : indicesToOpen) {
                    mdBuilder.put(IndexMetaData.newIndexMetaDataBuilder(currentState.metaData().index(index)).state(IndexMetaData.State.OPEN));
                    blocksBuilder.removeIndexBlock(index, INDEX_CLOSED_BLOCK);
                }

                ClusterState updatedState = ClusterState.builder().state(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder()
                        .routingTable(updatedState.routingTable());
                for (String index : indicesToOpen) {
                    rtBuilder.addAsRecovery(updatedState.metaData().index(index));
                }

                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder().state(updatedState).routingTable(rtBuilder).build());

                return ClusterState.builder().state(updatedState).routingResult(routingResult).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
                listener.onResponse(new Response(true));
            }
        });
    }

    public static interface Listener {

        void onResponse(Response response);

        void onFailure(Throwable t);
    }

    public static class Request {

        final String[] indices;

        TimeValue timeout = TimeValue.timeValueSeconds(10);

        public Request(String[] indices) {
            this.indices = indices;
        }

        public Request timeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }
    }

    public static class Response {
        private final boolean acknowledged;

        public Response(boolean acknowledged) {
            this.acknowledged = acknowledged;
        }

        public boolean acknowledged() {
            return acknowledged;
        }
    }
}
