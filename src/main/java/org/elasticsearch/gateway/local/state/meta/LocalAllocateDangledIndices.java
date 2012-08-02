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

package org.elasticsearch.gateway.local.state.meta;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;

/**
 */
public class LocalAllocateDangledIndices extends AbstractComponent {

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    @Inject
    public LocalAllocateDangledIndices(Settings settings, TransportService transportService, ClusterService clusterService, AllocationService allocationService) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        transportService.registerHandler(new AllocateDangledRequestHandler());
    }

    public void allocateDangled(IndexMetaData[] indices, final Listener listener) {
        ClusterState clusterState = clusterService.state();
        DiscoveryNode masterNode = clusterState.nodes().masterNode();
        if (masterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException("no master to send allocate dangled request"));
            return;
        }
        AllocateDangledRequest request = new AllocateDangledRequest(clusterState.nodes().localNode(), indices);
        transportService.sendRequest(masterNode, AllocateDangledRequestHandler.ACTION, request, new TransportResponseHandler<AllocateDangledResponse>() {
            @Override
            public AllocateDangledResponse newInstance() {
                return new AllocateDangledResponse();
            }

            @Override
            public void handleResponse(AllocateDangledResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void handleException(TransportException exp) {
                listener.onFailure(exp);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        });
    }

    public static interface Listener {
        void onResponse(AllocateDangledResponse response);

        void onFailure(Throwable e);
    }

    class AllocateDangledRequestHandler implements ActionTransportRequestHandler<AllocateDangledRequest> {

        public static final String ACTION = "/gateway/local/allocate_dangled";

        @Override
        public String action() {
            return ACTION;
        }

        @Override
        public AllocateDangledRequest newInstance() {
            return new AllocateDangledRequest();
        }

        @Override
        public void messageReceived(final AllocateDangledRequest request, final TransportChannel channel) throws Exception {
            String[] indexNames = new String[request.indices.length];
            for (int i = 0; i < request.indices.length; i++) {
                indexNames[i] = request.indices[i].index();
            }
            clusterService.submitStateUpdateTask("allocation dangled indices " + Arrays.toString(indexNames), new ProcessedClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (currentState.blocks().disableStatePersistence()) {
                        return currentState;
                    }
                    MetaData.Builder metaData = MetaData.builder()
                            .metaData(currentState.metaData());
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder().routingTable(currentState.routingTable());

                    boolean importNeeded = false;
                    StringBuilder sb = new StringBuilder();
                    for (IndexMetaData indexMetaData : request.indices) {
                        if (currentState.metaData().hasIndex(indexMetaData.index())) {
                            continue;
                        }
                        importNeeded = true;
                        metaData.put(indexMetaData, false);
                        blocks.addBlocks(indexMetaData);
                        routingTableBuilder.add(indexMetaData, false);
                        sb.append("[").append(indexMetaData.index()).append("/").append(indexMetaData.state()).append("]");
                    }
                    if (!importNeeded) {
                        return currentState;
                    }
                    logger.info("auto importing dangled indices {} from [{}]", sb.toString(), request.fromNode);

                    ClusterState updatedState = ClusterState.builder().state(currentState).metaData(metaData).blocks(blocks).routingTable(routingTableBuilder).build();

                    // now, reroute
                    RoutingAllocation.Result routingResult = allocationService.reroute(newClusterStateBuilder().state(updatedState).routingTable(routingTableBuilder).build());

                    return ClusterState.builder().state(updatedState).routingResult(routingResult).build();
                }

                @Override
                public void clusterStateProcessed(ClusterState clusterState) {
                    try {
                        channel.sendResponse(new AllocateDangledResponse(true));
                    } catch (IOException e) {
                        logger.error("failed send response for allocating dangled", e);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    static class AllocateDangledRequest implements Streamable {

        DiscoveryNode fromNode;
        IndexMetaData[] indices;

        AllocateDangledRequest() {
        }

        AllocateDangledRequest(DiscoveryNode fromNode, IndexMetaData[] indices) {
            this.fromNode = fromNode;
            this.indices = indices;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            fromNode = DiscoveryNode.readNode(in);
            indices = new IndexMetaData[in.readVInt()];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = IndexMetaData.Builder.readFrom(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            fromNode.writeTo(out);
            out.writeVInt(indices.length);
            for (IndexMetaData indexMetaData : indices) {
                IndexMetaData.Builder.writeTo(indexMetaData, out);
            }
        }
    }

    public static class AllocateDangledResponse implements Streamable {

        private boolean ack;

        AllocateDangledResponse() {
        }

        AllocateDangledResponse(boolean ack) {
            this.ack = ack;
        }

        public boolean ack() {
            return ack;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            ack = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(ack);
        }
    }
}
