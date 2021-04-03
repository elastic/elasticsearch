/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * cat API class for handling get componentTemplate.
 */
public class RestCatComponentTemplateAction extends AbstractCatAction {
    @Override
    public String getName() {
        return "cat_component_template_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_cat/component_templates"),
            new Route(GET, "/_cat/component_templates/{name}"));
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/component_templates");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("name", "alias:n;desc:component template name");
        table.addCell("template", "alias:t;desc:component template");
        table.addCell("version", "alias:v;desc:version");
        table.addCell("meta_data", "alias:m;desc:component templates comprising metadata");
        table.endHeaders();
        return table;
    }

    @Override
    protected RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {
        final String matchPattern = request.hasParam("name") ? request.param("name") : null;
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().metadata(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", clusterStateRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().state(clusterStateRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse clusterStateResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, clusterStateResponse, matchPattern), channel);
            }
        });
    }

    public Table buildTable(RestRequest request, ClusterStateResponse clusterStateResponse, String patternString) {
        Table table = getTableWithHeader(request);
        Metadata metadata = clusterStateResponse.getState().metadata();

        for (Map.Entry<String, ComponentTemplate> entry : metadata.componentTemplates().entrySet()) {
            String name = entry.getKey();
            ComponentTemplate template = entry.getValue();
            if (patternString == null || Regex.simpleMatch(patternString, name)) {
                table.startRow();
                table.addCell(name);
                table.addCell("[" + String.join(", ", template.toString()) + "]");
                table.addCell(template.version());
                table.addCell(template.metadata() == null ? "-":
                    "[" + String.join(", ", template.metadata().toString()) + "]");
                table.endRow();
            }
        }
        return table;
    }
}
