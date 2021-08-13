/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestTemplatesAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_cat/templates"),
            new Route(GET, "/_cat/templates/{name}"));
    }

    @Override
    public String getName() {
        return "cat_templates_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/templates\n");
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, NodeClient client) {
        final String matchPattern = request.hasParam("name") ? request.param("name") : null;
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().metadata(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestResponseListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse clusterStateResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, clusterStateResponse, matchPattern), channel);
            }
        });
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("name", "alias:n;desc:template name");
        table.addCell("index_patterns", "alias:t;desc:template index patterns");
        table.addCell("order", "alias:o,p;desc:template application order/priority number");
        table.addCell("version", "alias:v;desc:version");
        table.addCell("composed_of", "alias:c;desc:component templates comprising index template");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, ClusterStateResponse clusterStateResponse, String patternString) {
        Table table = getTableWithHeader(request);
        Metadata metadata = clusterStateResponse.getState().metadata();
        for (ObjectObjectCursor<String, IndexTemplateMetadata> entry : metadata.templates()) {
            IndexTemplateMetadata indexData = entry.value;
            if (patternString == null || Regex.simpleMatch(patternString, indexData.name())) {
                table.startRow();
                table.addCell(indexData.name());
                table.addCell("[" + String.join(", ", indexData.patterns()) + "]");
                table.addCell(indexData.getOrder());
                table.addCell(indexData.getVersion());
                table.addCell("");
                table.endRow();
            }
        }

        for (Map.Entry<String, ComposableIndexTemplate> entry : metadata.templatesV2().entrySet()) {
            String name = entry.getKey();
            ComposableIndexTemplate template = entry.getValue();
            if (patternString == null || Regex.simpleMatch(patternString, name)) {
                table.startRow();
                table.addCell(name);
                table.addCell("[" + String.join(", ", template.indexPatterns()) + "]");
                table.addCell(template.priorityOrZero());
                table.addCell(template.version());
                table.addCell("[" + String.join(", ", template.composedOf()) + "]");
                table.endRow();
            }
        }
        return table;
    }
}
