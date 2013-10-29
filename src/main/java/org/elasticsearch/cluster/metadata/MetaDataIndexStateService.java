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

import org.elasticsearch.action.admin.indices.close.CloseIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexClusterStateUpdateRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException;
import org.elasticsearch.rest.RestStatus;


/**
 * Service responsible for submitting open/close index requests
 */
public class MetaDataIndexStateService extends AbstractComponent {

    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.READ_WRITE);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    @Inject
    public MetaDataIndexStateService(Settings settings, ClusterService clusterService, AllocationService allocationService) {
        super(settings);
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    public void closeIndex(final CloseIndexClusterStateUpdateRequest request, final ClusterStateUpdateListener listener) {
        clusterService.submitStateUpdateTask("close-index [" + request.index() + "]", Priority.URGENT, new AckedClusterStateUpdateTask() {

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }

            @Override
            public void onAckTimeout() {
                listener.onResponse(new ClusterStateUpdateResponse(false));
            }

            @Override
            public TimeValue ackTimeout() {
                return request.ackTimeout();
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetaData indexMetaData = currentState.metaData().index(request.index());
                if (indexMetaData == null) {
                    throw new IndexMissingException(new Index(request.index()));
                }

                if (indexMetaData.state() == IndexMetaData.State.CLOSE) {
                    return currentState;
                }

                IndexRoutingTable indexRoutingTable = currentState.routingTable().index(request.index());
                for (IndexShardRoutingTable shard : indexRoutingTable) {
                    if (!shard.primaryAllocatedPostApi()) {
                        throw new IndexPrimaryShardNotAllocatedException(new Index(request.index()));
                    }
                }

                if (indexMetaData.state() == IndexMetaData.State.CLOSE) {
                    return currentState;
                }

                logger.info("[{}] closing index", request.index());

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData())
                        .put(IndexMetaData.builder(currentState.metaData().index(request.index())).state(IndexMetaData.State.CLOSE));

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks())
                        .addIndexBlock(request.index(), INDEX_CLOSED_BLOCK);

                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocks).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable())
                        .remove(request.index());

                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(rtBuilder).build());
                //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
                return ClusterState.builder(updatedState).routingResult(routingResult).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }
        });
    }

    public void openIndex(final OpenIndexClusterStateUpdateRequest request, final ClusterStateUpdateListener listener) {
        clusterService.submitStateUpdateTask("open-index [" + request.index() + "]", Priority.URGENT, new AckedClusterStateUpdateTask() {

            @Override
            public boolean mustAck(DiscoveryNode discoveryNode) {
                return true;
            }

            @Override
            public void onAllNodesAcked(@Nullable Throwable t) {
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }

            @Override
            public void onAckTimeout() {
                listener.onResponse(new ClusterStateUpdateResponse(false));
            }

            @Override
            public TimeValue ackTimeout() {
                return request.ackTimeout();
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                listener.onFailure(t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetaData indexMetaData = currentState.metaData().index(request.index());
                if (indexMetaData == null) {
                    throw new IndexMissingException(new Index(request.index()));
                }

                if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                    return currentState;
                }

                logger.info("[{}] opening index", request.index());

                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData())
                        .put(IndexMetaData.builder(currentState.metaData().index(request.index())).state(IndexMetaData.State.OPEN));

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks())
                        .removeIndexBlock(request.index(), INDEX_CLOSED_BLOCK);

                ClusterState updatedState = ClusterState.builder(currentState).metaData(mdBuilder).blocks(blocks).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder(updatedState.routingTable())
                        .addAsRecovery(updatedState.metaData().index(request.index()));

                RoutingAllocation.Result routingResult = allocationService.reroute(ClusterState.builder(updatedState).routingTable(rtBuilder).build());
                //no explicit wait for other nodes needed as we use AckedClusterStateUpdateTask
                return ClusterState.builder(updatedState).routingResult(routingResult).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }
        });
    }
}
