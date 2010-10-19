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
import static org.elasticsearch.cluster.metadata.MetaData.*;

/**
 * @author kimchy (shay.banon)
 */
public class MetaDataDeleteIndexService extends AbstractComponent {

    private final ClusterService clusterService;

    private final ShardsAllocation shardsAllocation;

    @Inject public MetaDataDeleteIndexService(Settings settings, ClusterService clusterService, ShardsAllocation shardsAllocation) {
        super(settings);
        this.clusterService = clusterService;
        this.shardsAllocation = shardsAllocation;
    }

    public void deleteIndex(final Request request, final Listener listener) {
        clusterService.submitStateUpdateTask("delete-index [" + request.index + "]", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                try {
                    RoutingTable routingTable = currentState.routingTable();
                    if (!routingTable.hasIndex(request.index)) {
                        listener.onFailure(new IndexMissingException(new Index(request.index)));
                        return currentState;
                    }

                    logger.info("[{}] deleting index", request.index);

                    RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder();
                    for (IndexRoutingTable indexRoutingTable : currentState.routingTable().indicesRouting().values()) {
                        if (!indexRoutingTable.index().equals(request.index)) {
                            routingTableBuilder.add(indexRoutingTable);
                        }
                    }
                    MetaData newMetaData = newMetaDataBuilder()
                            .metaData(currentState.metaData())
                            .remove(request.index)
                            .build();

                    RoutingAllocation.Result routingResult = shardsAllocation.reroute(
                            newClusterStateBuilder().state(currentState).routingTable(routingTableBuilder).metaData(newMetaData).build());

                    ClusterBlocks blocks = ClusterBlocks.builder().blocks(currentState.blocks()).removeIndexBlocks(request.index).build();

                    return newClusterStateBuilder().state(currentState).routingResult(routingResult).metaData(newMetaData).blocks(blocks).build();
                } catch (Exception e) {
                    listener.onFailure(e);
                    return currentState;
                }
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
