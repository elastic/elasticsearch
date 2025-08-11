/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Table;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;
import java.util.Locale;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestPluginsAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/plugins"));
    }

    @Override
    public String getName() {
        return "cat_plugins_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/plugins\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final boolean includeBootstrapPlugins = request.paramAsBoolean("include_bootstrap", false);
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().nodes(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestActionListener<ClusterStateResponse>(channel) {
            @Override
            public void processResponse(final ClusterStateResponse clusterStateResponse) throws Exception {
                NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
                nodesInfoRequest.clear().addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
                client.admin().cluster().nodesInfo(nodesInfoRequest, new RestResponseListener<NodesInfoResponse>(channel) {
                    @Override
                    public RestResponse buildResponse(final NodesInfoResponse nodesInfoResponse) throws Exception {
                        return RestTable.buildResponse(
                            buildTable(request, clusterStateResponse, nodesInfoResponse, includeBootstrapPlugins),
                            channel
                        );
                    }
                });
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("id", "default:false;desc:unique node id");
        table.addCell("name", "alias:n;desc:node name");
        table.addCell("component", "alias:c;desc:component");
        table.addCell("version", "alias:v;desc:component version");
        table.addCell("description", "alias:d;default:false;desc:plugin details");
        table.addCell("type", "alias:t;default:false;desc:plugin type");
        table.endHeaders();
        return table;
    }

    Table buildTable(RestRequest req, ClusterStateResponse state, NodesInfoResponse nodesInfo, boolean includeBootstrapPlugins) {
        DiscoveryNodes nodes = state.getState().nodes();
        Table table = getTableWithHeader(req);

        for (DiscoveryNode node : nodes) {
            NodeInfo info = nodesInfo.getNodesMap().get(node.getId());
            if (info == null) {
                continue;
            }
            PluginsAndModules plugins = info.getInfo(PluginsAndModules.class);
            if (plugins == null) {
                continue;
            }
            for (PluginDescriptor pluginDescriptor : plugins.getPluginInfos()) {
                if (pluginDescriptor.getType() == PluginType.BOOTSTRAP && includeBootstrapPlugins == false) {
                    continue;
                }
                table.startRow();
                table.addCell(node.getId());
                table.addCell(node.getName());
                table.addCell(pluginDescriptor.getName());
                table.addCell(pluginDescriptor.getVersion());
                table.addCell(pluginDescriptor.getDescription());
                table.addCell(pluginDescriptor.getType().toString().toLowerCase(Locale.ROOT));
                table.endRow();
            }
        }

        return table;
    }
}
