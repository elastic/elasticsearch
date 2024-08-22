/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationStats;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestAllocationAction extends AbstractCatAction {

    private static final String UNASSIGNED = "UNASSIGNED";

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/allocation"), new Route(GET, "/_cat/allocation/{nodes}"));
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
        clusterStateRequest.masterNodeTimeout(getMasterNodeTimeout(request));

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse state) {
                NodesStatsRequest statsRequest = new NodesStatsRequest(nodes);
                statsRequest.setIncludeShardsStats(false);
                statsRequest.clear()
                    .addMetric(NodesStatsRequestParameters.Metric.FS)
                    .addMetric(NodesStatsRequestParameters.Metric.ALLOCATIONS)
                    .indices(new CommonStatsFlags(CommonStatsFlags.Flag.Store));

                client.admin().cluster().nodesStats(statsRequest, new RestResponseListener<>(channel) {
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
        table.addCell("shards.undesired", "text-align:right;desc:number of shards that are scheduled to be moved elsewhere in the cluster");
        table.addCell("write_load.forecast", "alias:wlf,writeLoadForecast;text-align:right;desc:sum of index write load forecasts");
        table.addCell("disk.indices.forecast", "alias:dif,diskIndicesForecast;text-align:right;desc:sum of shard size forecasts");
        table.addCell("disk.indices", "alias:di,diskIndices;text-align:right;desc:disk used by ES indices");
        table.addCell("disk.used", "alias:du,diskUsed;text-align:right;desc:disk used (total, not just ES)");
        table.addCell("disk.avail", "alias:da,diskAvail;text-align:right;desc:disk available");
        table.addCell("disk.total", "alias:dt,diskTotal;text-align:right;desc:total capacity of all volumes");
        table.addCell("disk.percent", "alias:dp,diskPercent;text-align:right;desc:percent disk used");
        table.addCell("host", "alias:h;desc:host of node");
        table.addCell("ip", "desc:ip of node");
        table.addCell("node", "alias:n;desc:name of node");
        table.addCell("node.role", "alias:r,role,nodeRole;desc:node roles");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, final ClusterStateResponse state, final NodesStatsResponse stats) {
        final Map<String, Integer> shardCounts = new HashMap<>();

        for (ShardRouting shard : state.getState().routingTable().allShardsIterator()) {
            String nodeId = shard.assignedToNode() ? shard.currentNodeId() : UNASSIGNED;
            shardCounts.merge(nodeId, 1, Integer::sum);
        }
        Table table = getTableWithHeader(request);

        for (NodeStats nodeStats : stats.getNodes()) {
            DiscoveryNode node = nodeStats.getNode();

            ByteSizeValue total = nodeStats.getFs().getTotal().getTotal();
            ByteSizeValue avail = nodeStats.getFs().getTotal().getAvailable();
            // if we don't know how much we use (non data nodes), it means 0
            long used = 0;
            short diskPercent = -1;
            if (total.getBytes() > 0) {
                used = total.getBytes() - avail.getBytes();
                if (used >= 0 && avail.getBytes() >= 0) {
                    diskPercent = (short) (used * 100 / (used + avail.getBytes()));
                }
            }
            NodeAllocationStats nodeAllocationStats = nodeStats.getNodeAllocationStats();

            table.startRow();
            table.addCell(shardCounts.getOrDefault(node.getId(), 0));
            table.addCell(nodeAllocationStats != null ? nodeAllocationStats.undesiredShards() : null);
            table.addCell(nodeAllocationStats != null ? nodeAllocationStats.forecastedIngestLoad() : null);
            table.addCell(nodeAllocationStats != null ? ByteSizeValue.ofBytes(nodeAllocationStats.forecastedDiskUsage()) : null);
            table.addCell(nodeStats.getIndices().getStore().size());
            table.addCell(used < 0 ? null : ByteSizeValue.ofBytes(used));
            table.addCell(avail.getBytes() < 0 ? null : avail);
            table.addCell(total.getBytes() < 0 ? null : total);
            table.addCell(diskPercent < 0 ? null : diskPercent);
            table.addCell(node.getHostName());
            table.addCell(node.getHostAddress());
            table.addCell(node.getName());
            table.addCell(node.getRoleAbbreviationString());
            table.endRow();
        }

        if (shardCounts.containsKey(UNASSIGNED)) {
            table.startRow();
            table.addCell(shardCounts.get(UNASSIGNED));
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(null);
            table.addCell(UNASSIGNED);
            table.addCell(null);
            table.endRow();
        }

        return table;
    }
}
