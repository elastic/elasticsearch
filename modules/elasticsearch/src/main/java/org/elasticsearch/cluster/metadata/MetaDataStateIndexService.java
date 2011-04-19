/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardsAllocation;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;

import static org.elasticsearch.cluster.ClusterState.*;

/**
 * @author kimchy (shay.banon)
 */
public class MetaDataStateIndexService extends AbstractComponent {

    public static final ClusterBlock INDEX_CLOSED_BLOCK = new ClusterBlock(4, "index closed", false, false, ClusterBlockLevel.READ_WRITE);

    private final ClusterService clusterService;

    private final ShardsAllocation shardsAllocation;

    @Inject public MetaDataStateIndexService(Settings settings, ClusterService clusterService, ShardsAllocation shardsAllocation) {
        super(settings);
        this.clusterService = clusterService;
        this.shardsAllocation = shardsAllocation;
    }

    public void closeIndex(final Request request, final Listener listener) {
        clusterService.submitStateUpdateTask("close-index [" + request.index + "]", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {

                IndexMetaData indexMetaData = currentState.metaData().index(request.index);
                if (indexMetaData == null) {
                    listener.onFailure(new IndexMissingException(new Index(request.index)));
                    return currentState;
                }

                if (indexMetaData.state() == IndexMetaData.State.CLOSE) {
                    listener.onResponse(new Response(true));
                    return currentState;
                }

                logger.info("[{}] closing index", request.index);

                MetaData.Builder mdBuilder = MetaData.builder()
                        .metaData(currentState.metaData())
                        .put(IndexMetaData.newIndexMetaDataBuilder(currentState.metaData().index(request.index)).state(IndexMetaData.State.CLOSE));

                RoutingTable.Builder rtBuilder = RoutingTable.builder()
                        .routingTable(currentState.routingTable())
                        .remove(request.index);

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks())
                        .addIndexBlock(request.index, INDEX_CLOSED_BLOCK);

                return ClusterState.builder().state(currentState).metaData(mdBuilder).routingTable(rtBuilder).blocks(blocks).build();
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                listener.onResponse(new Response(true));
            }
        });
    }

    public void openIndex(final Request request, final Listener listener) {
        clusterService.submitStateUpdateTask("open-index [" + request.index + "]", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {

                IndexMetaData indexMetaData = currentState.metaData().index(request.index);
                if (indexMetaData == null) {
                    listener.onFailure(new IndexMissingException(new Index(request.index)));
                    return currentState;
                }

                if (indexMetaData.state() == IndexMetaData.State.OPEN) {
                    listener.onResponse(new Response(true));
                    return currentState;
                }

                logger.info("[{}] opening index", request.index);

                MetaData.Builder mdBuilder = MetaData.builder()
                        .metaData(currentState.metaData())
                        .put(IndexMetaData.newIndexMetaDataBuilder(currentState.metaData().index(request.index)).state(IndexMetaData.State.OPEN));

                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks())
                        .removeIndexBlock(request.index, INDEX_CLOSED_BLOCK);

                ClusterState updatedState = ClusterState.builder().state(currentState).metaData(mdBuilder).blocks(blocks).build();

                RoutingTable.Builder rtBuilder = RoutingTable.builder().routingTable(updatedState.routingTable());
                IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(request.index)
                        .initializeEmpty(updatedState.metaData().index(request.index), false);
                rtBuilder.add(indexRoutingBuilder);

                RoutingAllocation.Result routingResult = shardsAllocation.reroute(newClusterStateBuilder().state(updatedState).routingTable(rtBuilder).build());

                return ClusterState.builder().state(updatedState).routingResult(routingResult).build();
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                listener.onResponse(new Response(true));
            }
        });
    }

    public static interface Listener {

        void onResponse(Response response);

        void onFailure(Throwable t);
    }

    public static class Request {

        final String index;

        TimeValue timeout = TimeValue.timeValueSeconds(10);

        public Request(String index) {
            this.index = index;
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
