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
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TimeoutClusterStateUpdateTask;
import org.elasticsearch.cluster.action.index.NodeIndicesStateUpdatedAction;
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
import org.elasticsearch.common.util.concurrent.CountDown;
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
public class MetaDataIndexStateService extends AbstractComponent {

    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.READ_WRITE);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    private final NodeIndicesStateUpdatedAction indicesStateUpdatedAction;

    @Inject
    public MetaDataIndexStateService(Settings settings, ClusterService clusterService, AllocationService allocationService, NodeIndicesStateUpdatedAction indicesStateUpdatedAction) {
        super(settings);
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.indicesStateUpdatedAction = indicesStateUpdatedAction;
    }

    public void closeIndex(final Request request, final Listener listener) {
        if (request.indices == null || request.indices.length == 0) {
            throw new ElasticSearchIllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices);
        clusterService.submitStateUpdateTask("close-indices " + indicesAsString, Priority.URGENT, new TimeoutClusterStateUpdateTask() {

            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                List<String> indicesToClose = new ArrayList<String>();
                for (String index : request.indices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(index));
                    }

                    if (indexMetaData.state() != IndexMetaData.State.CLOSE) {
                        IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
                        for (IndexShardRoutingTable shard : indexRoutingTable) {
                            if (!shard.primaryAllocatedPostApi()) {
                                throw new IndexPrimaryShardNotAllocatedException(new Index(index));
                            }
                        }
                        indicesToClose.add(index);
                    }
                }

                if (indicesToClose.isEmpty()) {
                    return currentState;
                }

                logger.info("closing indices [{}]", indicesAsString);

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                for (String index : indicesToClose) {
                    mdBuilder.put(IndexMetaData.builder(currentState.metaData().index(index)).state(IndexMetaData.State.CLOSE));
                    blocksBuilder.addIndexBlock(index, INDEX_CLOSED_BLOCK);
                }

                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
                for (String index : indicesToClose) {
                    rtBuilder.remove(index);
                }

                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(rtBuilder).build());

                ClusterState newClusterState = ClusterState.builder(updatedState).routingResult(routingResult).build();

                waitForOtherNodes(newClusterState, listener, request.timeout);

                return newClusterState;
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (oldState == newState) {
                    // we didn't do anything, callback
                    listener.onResponse(new Response(true));
                }
            }
        });
    }

    public void openIndex(final Request request, final Listener listener) {
        if (request.indices == null || request.indices.length == 0) {
            throw new ElasticSearchIllegalArgumentException("Index name is required");
        }

        final String indicesAsString = Arrays.toString(request.indices);
        clusterService.submitStateUpdateTask("open-indices " + indicesAsString, Priority.URGENT, new TimeoutClusterStateUpdateTask() {
            @Override
            public TimeValue timeout() {
                return request.masterTimeout;
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                List<String> indicesToOpen = new ArrayList<String>();
                for (String index : request.indices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(index));
                    }
                    if (indexMetaData.state() != IndexMetaData.State.OPEN) {
                        indicesToOpen.add(index);
                    }
                }

                if (indicesToOpen.isEmpty()) {
                    return currentState;
                }

                logger.info("opening indices [{}]", indicesAsString);

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder()
                        .blocks(currentState.blocks());
                for (String index : indicesToOpen) {
                    mdBuilder.put(IndexMetaData.builder(currentState.metaData().index(index)).state(IndexMetaData.State.OPEN));
                    blocksBuilder.removeIndexBlock(index, INDEX_CLOSED_BLOCK);
                }

                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocksBuilder).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder(updatedState.routingTable());
                for (String index : indicesToOpen) {
                    rtBuilder.addAsRecovery(updatedState.metaData().index(index));
                }

                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(rtBuilder).build());

                ClusterState newClusterState = ClusterState.builder(updatedState).routingResult(routingResult).build();

                waitForOtherNodes(newClusterState, listener, request.timeout);

                return newClusterState;

            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (oldState == newState) {
                    // we didn't do anything, callback
                    listener.onResponse(new Response(true));
                }
            }
        });
    }

    private void waitForOtherNodes(ClusterState updatedState, Listener listener, TimeValue timeout) {
        // wait for responses from other nodes if needed
        int responseCount = updatedState.nodes().size();
        long version = updatedState.version() + 1;
        logger.trace("waiting for [{}] notifications with version [{}]", responseCount, version);
        indicesStateUpdatedAction.add(new CountDownListener(responseCount, listener, version), timeout);
    }

    public static interface Listener {

        void onResponse(Response response);

        void onFailure(Throwable t);
    }

    public static class Request {

        final String[] indices;

        TimeValue timeout = TimeValue.timeValueSeconds(10);
        TimeValue masterTimeout = MasterNodeOperationRequest.DEFAULT_MASTER_NODE_TIMEOUT;

        public Request(String[] indices) {
            this.indices = indices;
        }

        public Request timeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public Request masterTimeout(TimeValue masterTimeout) {
            this.masterTimeout = masterTimeout;
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

    private class CountDownListener implements NodeIndicesStateUpdatedAction.Listener {
        private final CountDown countDown;
        private final Listener listener;
        private final long version;

        public CountDownListener(int count, Listener listener, long version) {
            this.countDown = new CountDown(count);
            this.listener = listener;
            this.version = version;
        }

        @Override
        public void onIndexStateUpdated(NodeIndicesStateUpdatedAction.NodeIndexStateUpdatedResponse response) {
            if (version <= response.version()) {
                logger.trace("Received NodeIndexStateUpdatedResponse with version [{}] from [{}]", response.version(), response.nodeId());
                if (countDown.countDown()) {
                    indicesStateUpdatedAction.remove(this);
                    logger.trace("NodeIndexStateUpdated was acknowledged by all expected nodes, returning");
                    listener.onResponse(new Response(true));
                }
            }
        }

        @Override
        public void onTimeout() {
            if (countDown.fastForward()) {
                indicesStateUpdatedAction.remove(this);
                listener.onResponse(new Response(false));
            }
        }
    }
}
