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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.cluster.metadata.MetaDataIndexStateService.isIndexVerifiedBeforeClosed;

public class LocalAllocateDangledIndices {

    private static final Logger logger = LogManager.getLogger(LocalAllocateDangledIndices.class);

    public static final String ACTION_NAME = "internal:gateway/local/allocate_dangled";

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final MetaDataIndexUpgradeService metaDataIndexUpgradeService;

    @Inject
    public LocalAllocateDangledIndices(TransportService transportService, ClusterService clusterService,
                                       AllocationService allocationService, MetaDataIndexUpgradeService metaDataIndexUpgradeService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.metaDataIndexUpgradeService = metaDataIndexUpgradeService;
        transportService.registerRequestHandler(ACTION_NAME, ThreadPool.Names.SAME, AllocateDangledRequest::new,
            new AllocateDangledRequestHandler());
    }

    public void allocateDangled(Collection<IndexMetaData> indices, ActionListener<AllocateDangledResponse> listener) {
        ClusterState clusterState = clusterService.state();
        DiscoveryNode masterNode = clusterState.nodes().getMasterNode();
        if (masterNode == null) {
            listener.onFailure(new MasterNotDiscoveredException());
            return;
        }
        AllocateDangledRequest request = new AllocateDangledRequest(clusterService.localNode(),
            indices.toArray(new IndexMetaData[indices.size()]));
        transportService.sendRequest(masterNode, ACTION_NAME, request,
            new ActionListenerResponseHandler<>(listener, AllocateDangledResponse::new, ThreadPool.Names.SAME));
    }

    class AllocateDangledRequestHandler implements TransportRequestHandler<AllocateDangledRequest> {
        @Override
        public void messageReceived(final AllocateDangledRequest request, final TransportChannel channel, Task task) throws Exception {
            String[] indexNames = new String[request.indices.length];
            for (int i = 0; i < request.indices.length; i++) {
                indexNames[i] = request.indices[i].getIndex().getName();
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
                    final Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion()
                        .minimumIndexCompatibilityVersion();
                    boolean importNeeded = false;
                    StringBuilder sb = new StringBuilder();
                    for (IndexMetaData indexMetaData : request.indices) {
                        if (indexMetaData.getCreationVersion().before(minIndexCompatibilityVersion)) {
                            logger.warn("ignoring dangled index [{}] on node [{}]" +
                                " since it's created version [{}] is not supported by at least one node in the cluster minVersion [{}]",
                                indexMetaData.getIndex(), request.fromNode, indexMetaData.getCreationVersion(),
                                minIndexCompatibilityVersion);
                            continue;
                        }
                        if (currentState.nodes().getMinNodeVersion().before(indexMetaData.getCreationVersion())) {
                            logger.warn("ignoring dangled index [{}] on node [{}]" +
                                " since its created version [{}] is later than the oldest versioned node in the cluster [{}]",
                                indexMetaData.getIndex(), request.fromNode, indexMetaData.getCreationVersion(),
                                currentState.getNodes().getMasterNode().getVersion());
                            continue;
                        }
                        if (currentState.metaData().hasIndex(indexMetaData.getIndex().getName())) {
                            continue;
                        }
                        if (currentState.metaData().hasAlias(indexMetaData.getIndex().getName())) {
                            logger.warn("ignoring dangled index [{}] on node [{}] due to an existing alias with the same name",
                                    indexMetaData.getIndex(), request.fromNode);
                            continue;
                        }
                        importNeeded = true;

                        IndexMetaData upgradedIndexMetaData;
                        try {
                            // The dangled index might be from an older version, we need to make sure it's compatible
                            // with the current version and upgrade it if needed.
                            upgradedIndexMetaData = metaDataIndexUpgradeService.upgradeIndexMetaData(indexMetaData,
                                minIndexCompatibilityVersion);
                        } catch (Exception ex) {
                            // upgrade failed - adding index as closed
                            logger.warn(() -> new ParameterizedMessage("found dangled index [{}] on node [{}]. This index cannot be " +
                                "upgraded to the latest version, adding as closed", indexMetaData.getIndex(), request.fromNode), ex);
                            upgradedIndexMetaData = IndexMetaData.builder(indexMetaData).state(IndexMetaData.State.CLOSE)
                                .version(indexMetaData.getVersion() + 1).build();
                        }
                        metaData.put(upgradedIndexMetaData, false);
                        blocks.addBlocks(upgradedIndexMetaData);
                        if (upgradedIndexMetaData.getState() == IndexMetaData.State.OPEN || isIndexVerifiedBeforeClosed(indexMetaData)) {
                            routingTableBuilder.addAsFromDangling(upgradedIndexMetaData);
                        }
                        sb.append("[").append(upgradedIndexMetaData.getIndex()).append("/").append(upgradedIndexMetaData.getState())
                            .append("]");
                    }
                    if (!importNeeded) {
                        return currentState;
                    }
                    logger.info("auto importing dangled indices {} from [{}]", sb.toString(), request.fromNode);

                    RoutingTable routingTable = routingTableBuilder.build();
                    ClusterState updatedState = ClusterState.builder(currentState).metaData(metaData).blocks(blocks)
                        .routingTable(routingTable).build();

                    // now, reroute
                    return allocationService.reroute(
                            ClusterState.builder(updatedState).routingTable(routingTable).build(), "dangling indices allocated");
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(() -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed send response for allocating dangled", inner);
                    }
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    try {
                        channel.sendResponse(new AllocateDangledResponse());
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

        public AllocateDangledRequest(StreamInput in) throws IOException {
            super(in);
            fromNode = new DiscoveryNode(in);
            indices = new IndexMetaData[in.readVInt()];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = IndexMetaData.readFrom(in);
            }
        }

        AllocateDangledRequest(DiscoveryNode fromNode, IndexMetaData[] indices) {
            this.fromNode = fromNode;
            this.indices = indices;
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

        private AllocateDangledResponse(StreamInput in) throws IOException {
            if (in.getVersion().before(Version.V_8_0_0)) {
                in.readBoolean();
            }
        }

        private AllocateDangledResponse() {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().before(Version.V_8_0_0)) {
                out.writeBoolean(true);
            }
        }
    }
}
