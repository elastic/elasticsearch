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

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 */
public class LocalAllocateDangledIndices extends AbstractComponent {

    public static final String ACTION_NAME = "internal:gateway/local/allocate_dangled";

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;

    @Inject
    public LocalAllocateDangledIndices(Settings settings, TransportService transportService, ClusterService clusterService,
                                       AllocationService allocationService, MetaDataIndexUpgradeService metaDataIndexUpgradeService) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
        transportService.registerRequestHandler(ACTION_NAME, AllocateDangledRequest::new, ThreadPool.Names.SAME, new AllocateDangledRequestHandler());
    }

    public void allocateDangled(Collection<IndexMetaData> indices, final Listener listener) {
        ClusterState clusterState = clusterService.state();
        DiscoveryNode masterNode = clusterState.nodes().masterNode();
        if (masterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException("no master to send allocate dangled request"));
            return;
        }
        AllocateDangledRequest request = new AllocateDangledRequest(clusterService.localNode(), indices.toArray(new IndexMetaData[indices.size()]));
        transportService.sendRequest(masterNode, ACTION_NAME, request, new TransportResponseHandler<AllocateDangledResponse>() {
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

    class AllocateDangledRequestHandler implements TransportRequestHandler<AllocateDangledRequest> {
        @Override
        public void messageReceived(final AllocateDangledRequest request, final TransportChannel channel) throws Exception {
            String[] indexNames = new String[request.indices.length];
            for (int i = 0; i < request.indices.length; i++) {
                indexNames[i] = request.indices[i].getIndex();
            }
            clusterService.submitStateUpdateTask("allocation dangled indices " + Arrays.toString(indexNames), new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (currentState.blocks().disableStatePersistence()) {
                        return currentState;
                    }
                    MetaData.Builder metaData = MetaData.builder(currentState.metaData());
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());

                    boolean importNeeded = false;
                    StringBuilder sb = new StringBuilder();
                    for (IndexMetaData indexMetaData : request.indices) {
                        if (currentState.metaData().hasIndex(indexMetaData.getIndex())) {
                            continue;
                        }
                        if (currentState.metaData().hasAlias(indexMetaData.getIndex())) {
                            logger.warn("ignoring dangled index [{}] on node [{}] due to an existing alias with the same name",
                                    indexMetaData.getIndex(), request.fromNode);
                            continue;
                        }
                        importNeeded = true;

                        IndexMetaData upgradedIndexMetaData;
                        try {
                            // The dangled index might be from an older version, we need to make sure it's compatible
                            // with the current version and upgrade it if needed.
                            upgradedIndexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData);
                        } catch (Exception ex) {
                            // upgrade failed - adding index as closed
                            logger.warn("found dangled index [{}] on node [{}]. This index cannot be upgraded to the latest version, adding as closed", ex,
                                    indexMetaData.getIndex(), request.fromNode);
                            upgradedIndexMetaData = IndexMetaData.builder(indexMetaData).state(IndexMetaData.State.CLOSE).version(indexMetaData.getVersion() + 1).build();
                        }
                        metaData.put(upgradedIndexMetaData, false);
                        blocks.addBlocks(upgradedIndexMetaData);
                        if (upgradedIndexMetaData.getState() == IndexMetaData.State.OPEN) {
                            routingTableBuilder.addAsFromDangling(upgradedIndexMetaData);
                        }
                        sb.append("[").append(upgradedIndexMetaData.getIndex()).append("/").append(upgradedIndexMetaData.getState()).append("]");
                    }
                    if (!importNeeded) {
                        return currentState;
                    }
                    logger.info("auto importing dangled indices {} from [{}]", sb.toString(), request.fromNode);

                    RoutingTable routingTable = routingTableBuilder.build();
                    ClusterState updatedState = ClusterState.builder(currentState).metaData(metaData).blocks(blocks).routingTable(routingTable).build();

                    // now, reroute
                    RoutingAllocation.Result routingResult = allocationService.reroute(
                            ClusterState.builder(updatedState).routingTable(routingTable).build(), "dangling indices allocated");

                    return ClusterState.builder(updatedState).routingResult(routingResult).build();
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("unexpected failure during [{}]", t, source);
                    try {
                        channel.sendResponse(t);
                    } catch (Exception e) {
                        logger.warn("failed send response for allocating dangled", e);
                    }
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    try {
                        channel.sendResponse(new AllocateDangledResponse(true));
                    } catch (IOException e) {
                        logger.warn("failed send response for allocating dangled", e);
                    }
                }
            });
        }
    }

    public static class AllocateDangledRequest extends TransportRequest {

        DiscoveryNode fromNode;
        IndexMetaData[] indices;

        public AllocateDangledRequest() {
        }

        AllocateDangledRequest(DiscoveryNode fromNode, IndexMetaData[] indices) {
            this.fromNode = fromNode;
            this.indices = indices;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            fromNode = DiscoveryNode.readNode(in);
            indices = new IndexMetaData[in.readVInt()];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = IndexMetaData.Builder.readFrom(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            fromNode.writeTo(out);
            out.writeVInt(indices.length);
            for (IndexMetaData indexMetaData : indices) {
                indexMetaData.writeTo(out);
            }
        }
    }

    public static class AllocateDangledResponse extends TransportResponse {

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
            super.readFrom(in);
            ack = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(ack);
        }
    }
}
