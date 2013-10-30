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

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.ShardStatus;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * RestRecoveryAction provides information about the status of replica recovery
 * in a string format, designed to be used at the command line. An Index can
 * be specified to limit output to a particular index or indices.
 */
public class RestRecoveryAction extends BaseRestHandler {

    @Inject
    protected RestRecoveryAction(Settings settings, Client client, RestController restController) {
        super(settings, client);
        restController.registerHandler(GET, "/_cat/recovery", this);
        restController.registerHandler(GET, "/_cat/recovery/{index}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.filterMetaData(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse clusterStateResponse) {
                IndicesStatusRequest indicesStatusRequest = new IndicesStatusRequest(indices);
                indicesStatusRequest.recovery(true);
                indicesStatusRequest.operationThreading(BroadcastOperationThreading.SINGLE_THREAD);

                client.admin().indices().status(indicesStatusRequest, new ActionListener<IndicesStatusResponse>() {
                    @Override
                    public void onResponse(IndicesStatusResponse indicesStatusResponse) {
                        Map<String, Long> primarySizes = new HashMap<String, Long>();
                        Set<ShardStatus> replicas = new HashSet<ShardStatus>();

                        // Loop through all the shards in the index status, keeping
                        // track of the primary shard size with a Map and the
                        // recovering shards in a Set of ShardStatus objects
                        for (ShardStatus shardStatus : indicesStatusResponse.getShards()) {
                            if (shardStatus.getShardRouting().primary()) {
                                primarySizes.put(shardStatus.getShardRouting().getIndex() + shardStatus.getShardRouting().getId(),
                                        shardStatus.getStoreSize().bytes());
                            } else if (shardStatus.getState() == IndexShardState.RECOVERING) {
                                replicas.add(shardStatus);
                            }
                        }

                        try {
                            channel.sendResponse(RestTable.buildResponse(buildRecoveryTable(clusterStateResponse, primarySizes, replicas), request, channel));
                        } catch (Throwable e) {
                            try {
                                channel.sendResponse(new XContentThrowableRestResponse(request, e));
                            } catch (IOException e2) {
                                logger.error("Unable to send recovery status response", e2);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(new XContentThrowableRestResponse(request, e));
                        } catch (IOException e1) {
                            logger.error("Failed to send failure response", e1);
                        }
                    }
                });
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });

    }

    /**
     * buildRecoveryTable will build a table of recovery information suitable
     * for displaying at the command line.
     * @param state Current cluster state.
     * @param primarySizes A Map of {@code index + shardId} strings to store size for all primary shards.
     * @param recoveringReplicas A Set of {@link ShardStatus} objects for each recovering replica to be displayed.
     * @return A table containing index, shardId, node, target size, recovered size and percentage for each recovering replica
     */
    public static Table buildRecoveryTable(ClusterStateResponse state, Map<String, Long> primarySizes, Set<ShardStatus> recoveringReplicas) {
        Table t = new Table();
        t.startHeaders().addCell("index")
                .addCell("shard")
                .addCell("target", "text-align:right;")
                .addCell("recovered", "text-align:right;")
                .addCell("%", "text-align:right;")
                .addCell("ip")
                .addCell("node")
                .endHeaders();
        for (ShardStatus status : recoveringReplicas) {
            DiscoveryNode node = state.getState().nodes().get(status.getShardRouting().currentNodeId());

            String index = status.getShardRouting().getIndex();
            int id = status.getShardId();
            long replicaSize = status.getStoreSize().bytes();
            Long primarySize = primarySizes.get(index + id);
            t.startRow();
            t.addCell(index);
            t.addCell(id);
            t.addCell(primarySize);
            t.addCell(replicaSize);
            t.addCell(primarySize == null ? null : String.format(Locale.ROOT, "%1.1f%%", 100.0 * (float)replicaSize / primarySize));
            t.addCell(node == null ? null : ((InetSocketTransportAddress) node.address()).address().getAddress().getHostAddress());
            t.addCell(node == null ? null : node.name());
            t.endRow();
        }
        return t;
    }
}
