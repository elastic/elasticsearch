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

package org.elasticsearch.rest.action.recovery;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Locale;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.admin.indices.status.ShardStatus;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.collect.Tuple;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestRequest.Method.GET;


/**
 *  REST handler for recovery status. Provides cluster-wide information on all recovering shards/replicas.
 */
public class RestRecoveryAction extends BaseRestHandler {

    @Inject
    public RestRecoveryAction(Settings settings, Client client, RestController restController) {
        super(settings, client);
        restController.registerHandler(GET, "/_recovery", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse clusterStateResponse) {
                IndicesStatusRequest indicesStatusRequest = new IndicesStatusRequest();
                indicesStatusRequest.recovery(true);
                indicesStatusRequest.operationThreading(BroadcastOperationThreading.SINGLE_THREAD);

                client.admin().indices().status(indicesStatusRequest, new ActionListener<IndicesStatusResponse>() {
                    @Override
                    public void onResponse(IndicesStatusResponse indicesStatusResponse) {

                        // Map each index to its shards, keeping track of the size of the primary shard
                        Map<String, Tuple<Long, Set<ShardStatus>>> indexShardMap = new HashMap<String, Tuple<Long, Set<ShardStatus>>>();

                        for (ShardStatus shardStatus : indicesStatusResponse.getShards()) {
                            if (shardStatus.getShardRouting().primary()) {
                                indexShardMap.put(shardStatus.getShardRouting().getIndex(),
                                    new Tuple<Long, Set<ShardStatus>>(shardStatus.getStoreSize().bytes(), new HashSet<ShardStatus>()));
                            }
                        }
                        for (ShardStatus shardStatus : indicesStatusResponse.getShards()) {
                            if (shardStatus.getState() == IndexShardState.RECOVERING) {
                                Tuple<Long, Set<ShardStatus>> replicas = indexShardMap.get(shardStatus.getShardRouting().getIndex());
                                replicas.v2().add(shardStatus);
                            }
                        }

                        try {
                            RecoveryInfo recoveryInfo = new RecoveryInfo(clusterStateResponse, indicesStatusResponse, indexShardMap);
                            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                            if (recoveryInfo.hasRecoveries()) {
                                builder.startObject();
                                recoveryInfo.toXContent(builder, request);
                                builder.endObject();
                            }
                            channel.sendResponse(new XContentRestResponse(request, OK, builder));
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

    private static class RecoveryInfo implements ToXContent {

        ClusterStateResponse state;
        Map<String, Tuple<Long, Set<ShardStatus>>> indexShardMap;
        IndicesStatusResponse indicesStatusResponse;

        public RecoveryInfo(ClusterStateResponse state, IndicesStatusResponse indicesStatusResponse,
                            Map<String, Tuple<Long, Set<ShardStatus>>> indexShardMap) {
            this.indicesStatusResponse = indicesStatusResponse;
            this.state = state;
            this.indexShardMap = indexShardMap;
        }

        public boolean hasRecoveries() {
            for (String indexName : indexShardMap.keySet()) {
                if (indexShardMap.get(indexName).v2().size() > 0) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

            for (String indexName : indexShardMap.keySet()) {
                // Skip index if it doesn't have any recovering shards
                Tuple<Long, Set<ShardStatus>> shards = indexShardMap.get(indexName);
                if (shards == null || shards.v2() == null || shards.v2().size() == 0) {
                    continue;
                }

                builder.field(indexName);
                builder.startArray();
                long primarySize = shards.v1();

                for (ShardStatus shardStatus : shards.v2()) {
                    builder.startObject(Integer.toString(shardStatus.getShardId()));
                    long replicaSize = shardStatus.getStoreSize().bytes();
                    DiscoveryNode node =
                            state.getState().nodes().get(shardStatus.getShardRouting().currentNodeId());

                    builder.field("hostname", node.getHostName());
                    builder.field("ip",  node.getHostAddress());
                    builder.field("node", node.name());
                    builder.field("primary", shardStatus.getShardRouting().primary());
                    builder.field("total_bytes", primarySize);
                    builder.field("bytes_recovered", replicaSize);
                    builder.field("percent_recovered", String.format(Locale.ROOT, "%1.1f%%", 100.0 * ((float) replicaSize / primarySize) ));

                    // Indicates a restore from a repository snapshot
                    if (shardStatus.getShardRouting().restoreSource() != null) {
                        builder.field("snapshot_recovery_source");
                        shardStatus.getShardRouting().restoreSource().toXContent(builder, params);
                    } else {
                        indicesStatusResponse.shardRecoveryStatusToXContent(builder, params, shardStatus);
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            return builder;
        }
    }
}
