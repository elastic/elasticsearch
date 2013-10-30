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

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;


public class RestAllocationAction extends BaseRestHandler{
    @Inject
    public RestAllocationAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_cat/allocation", this);
        controller.registerHandler(GET, "/_cat/allocation/{nodes}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] nodes = Strings.splitStringByCommaToArray(request.param("nodes"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.filterMetaData(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        client.admin().cluster().state(clusterStateRequest, new ActionListener<ClusterStateResponse>() {
            @Override
            public void onResponse(final ClusterStateResponse state) {
                NodesStatsRequest statsRequest = new NodesStatsRequest(nodes);
                statsRequest.clear().fs(true);

                client.admin().cluster().nodesStats(statsRequest, new ActionListener<NodesStatsResponse>() {
                    @Override
                    public void onResponse(NodesStatsResponse stats) {
                        try {
                            Table tab = buildTable(state, stats);
                            channel.sendResponse(RestTable.buildResponse(tab, request, channel));
                        } catch (Throwable e) {
                            onFailure(e);
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

    private Table buildTable(final ClusterStateResponse state, final NodesStatsResponse stats) {
        final ObjectIntOpenHashMap<String> allocs = new ObjectIntOpenHashMap<String>();

        for (ShardRouting shard : state.getState().routingTable().allShards()) {
            String nodeId = "UNASSIGNED";

            if (shard.assignedToNode()) {
                nodeId = shard.currentNodeId();
            }

            allocs.addTo(nodeId, 1);
        }

        final Table table = new Table();
        table.startHeaders();
        table.addCell("shards", "text-align:right;");
        table.addCell("diskUsed", "text-align:right;");
        table.addCell("diskAvail", "text-align:right;");
        table.addCell("diskRatio", "text-align:right;");
        table.addCell("ip");
        table.addCell("node");
        table.endHeaders();

        for (NodeStats nodeStats : stats.getNodes()) {
            DiscoveryNode node = nodeStats.getNode();

            long used = -1;
            long avail = -1;

            Iterator<FsStats.Info> diskIter = nodeStats.getFs().iterator();
            while (diskIter.hasNext()) {
                FsStats.Info disk = diskIter.next();
                used += disk.getTotal().bytes() - disk.getAvailable().bytes();
                avail += disk.getAvailable().bytes();
            }

            String nodeId = node.id();

            int shardCount = -1;
            if (allocs.containsKey(nodeId)) {
                shardCount = allocs.lget();
            }

            float ratio = -1;

            if (used >=0 && avail > 0) {
                ratio = used / (float) avail;
            }

            table.startRow();
            table.addCell(shardCount < 0 ? null : shardCount);
            table.addCell(used < 0 ? null : new ByteSizeValue(used));
            table.addCell(avail < 0 ? null : new ByteSizeValue(avail));
            table.addCell(ratio < 0 ? null : String.format(Locale.ROOT, "%.1f%%", ratio*100.0));
            table.addCell(node == null ? null : ((InetSocketTransportAddress) node.address()).address().getAddress().getHostAddress());
            table.addCell(node == null ? "UNASSIGNED" : node.name());
            table.endRow();
        }

        if (allocs.containsKey("UNASSIGNED")) {
            table.startRow();
            table.addCell(allocs.lget());
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell("UNASSIGNED");
            table.endRow();
        }

        return table;
    }

}
