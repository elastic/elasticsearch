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

package org.elasticsearch.rest.action.cat;

import com.carrotsearch.hppc.ObjectIntScatterMap;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;


public class RestAllocationAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "/_cat/allocation"),
            new Route(GET, "/_cat/allocation/{nodes}")));
    }

    @Override
    public String getName() {
        return "cat_allocation_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/allocation\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final String[] nodes = Strings.splitStringByCommaToArray(request.param("nodes", "data:true"));
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().routingTable(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse state) {
                NodesStatsRequest statsRequest = new NodesStatsRequest(nodes);
                statsRequest.clear().fs(true).indices(new CommonStatsFlags(CommonStatsFlags.Flag.Store));

                client.admin().cluster().nodesStats(statsRequest, new RestResponseListener<NodesStatsResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(NodesStatsResponse stats) throws Exception {
                        Table tab = buildTable(request, state, stats);
                        return RestTable.buildResponse(tab, channel);
                    }
                });
            }
        });

    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("shards", "alias:s;text-align:right;desc:number of shards on node");
        table.addCell("disk.indices", "alias:di,diskIndices;text-align:right;desc:disk used by ES indices");
        table.addCell("disk.used", "alias:du,diskUsed;text-align:right;desc:disk used (total, not just ES)");
        table.addCell("disk.avail", "alias:da,diskAvail;text-align:right;desc:disk available");
        table.addCell("disk.total", "alias:dt,diskTotal;text-align:right;desc:total capacity of all volumes");
        table.addCell("disk.percent", "alias:dp,diskPercent;text-align:right;desc:percent disk used");
        table.addCell("host", "alias:h;desc:host of node");
        table.addCell("ip", "desc:ip of node");
        table.addCell("node", "alias:n;desc:name of node");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, final ClusterStateResponse state, final NodesStatsResponse stats) {
        final ObjectIntScatterMap<String> allocs = new ObjectIntScatterMap<>();

        for (ShardRouting shard : state.getState().routingTable().allShards()) {
            String nodeId = "UNASSIGNED";

            if (shard.assignedToNode()) {
                nodeId = shard.currentNodeId();
            }

            allocs.addTo(nodeId, 1);
        }

        Table table = getTableWithHeader(request);

        for (NodeStats nodeStats : stats.getNodes()) {
            DiscoveryNode node = nodeStats.getNode();

            int shardCount = allocs.getOrDefault(node.getId(), 0);

            ByteSizeValue total = nodeStats.getFs().getTotal().getTotal();
            ByteSizeValue avail = nodeStats.getFs().getTotal().getAvailable();
            //if we don't know how much we use (non data nodes), it means 0
            long used = 0;
            short diskPercent = -1;
            if (total.getBytes() > 0) {
                used = total.getBytes() - avail.getBytes();
                if (used >= 0 && avail.getBytes() >= 0) {
                    diskPercent = (short) (used * 100 / (used + avail.getBytes()));
                }
            }

            table.startRow();
            table.addCell(shardCount);
            table.addCell(nodeStats.getIndices().getStore().getSize());
            table.addCell(used < 0 ? null : new ByteSizeValue(used));
            table.addCell(avail.getBytes() < 0 ? null : avail);
            table.addCell(total.getBytes() < 0 ? null : total);
            table.addCell(diskPercent < 0 ? null : diskPercent);
            table.addCell(node.getHostName());
            table.addCell(node.getHostAddress());
            table.addCell(node.getName());
            table.endRow();
        }

        final String UNASSIGNED = "UNASSIGNED";
        if (allocs.containsKey(UNASSIGNED)) {
            table.startRow();
            table.addCell(allocs.get(UNASSIGNED));
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(UNASSIGNED);
            table.endRow();
        }

        return table;
    }

}
