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
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Cat API class to display information about the size of fielddata fields per node
 */
@ServerlessScope(Scope.INTERNAL)
public class RestFielddataAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/fielddata"), new Route(GET, "/_cat/fielddata/{fields}"));
    }

    @Override
    public String getName() {
        return "cat_fielddata_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("data:true");
        nodesStatsRequest.setIncludeShardsStats(false);
        nodesStatsRequest.clear();
        nodesStatsRequest.indices(true);
        String[] fields = request.paramAsStringArray("fields", null);
        nodesStatsRequest.indices().fieldDataFields(fields == null ? new String[] { "*" } : fields);

        return channel -> client.admin().cluster().nodesStats(nodesStatsRequest, new RestResponseListener<NodesStatsResponse>(channel) {
            @Override
            public RestResponse buildResponse(NodesStatsResponse nodeStatses) throws Exception {
                return RestTable.buildResponse(buildTable(request, nodeStatses), channel);
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/fielddata\n");
        sb.append("/_cat/fielddata/{fields}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders()
            .addCell("id", "desc:node id")
            .addCell("host", "alias:h;desc:host name")
            .addCell("ip", "desc:ip address")
            .addCell("node", "alias:n;desc:node name")
            .addCell("field", "alias:f;desc:field name")
            .addCell("size", "text-align:right;alias:s;desc:field data usage")
            .endHeaders();
        return table;
    }

    private Table buildTable(final RestRequest request, final NodesStatsResponse nodeStatses) {
        Table table = getTableWithHeader(request);

        for (NodeStats nodeStats : nodeStatses.getNodes()) {
            if (nodeStats.getIndices().getFieldData().getFields() != null) {
                for (var field : nodeStats.getIndices().getFieldData().getFields()) {
                    table.startRow();
                    table.addCell(nodeStats.getNode().getId());
                    table.addCell(nodeStats.getNode().getHostName());
                    table.addCell(nodeStats.getNode().getHostAddress());
                    table.addCell(nodeStats.getNode().getName());
                    table.addCell(field.getKey());
                    table.addCell(ByteSizeValue.ofBytes(field.getValue()));
                    table.endRow();
                }
            }
        }

        return table;
    }
}
