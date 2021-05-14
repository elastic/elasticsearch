/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.template.post.SimulateTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSimulateTemplateAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_index_template/_simulate"),
            new Route(POST, "/_index_template/_simulate/{name}"));
    }

    @Override
    public String getName() {
        return "simulate_template_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SimulateTemplateAction.Request simulateRequest = new SimulateTemplateAction.Request();
        simulateRequest.templateName(request.param("name"));
        if (request.hasContent()) {
            PutComposableIndexTemplateAction.Request indexTemplateRequest =
                new PutComposableIndexTemplateAction.Request("simulating_template");
            indexTemplateRequest.indexTemplate(ComposableIndexTemplate.parse(request.contentParser()));
            indexTemplateRequest.create(request.paramAsBoolean("create", false));
            indexTemplateRequest.cause(request.param("cause", "api"));

            simulateRequest.indexTemplateRequest(indexTemplateRequest);
        }
        simulateRequest.masterNodeTimeout(request.paramAsTime("master_timeout", simulateRequest.masterNodeTimeout()));

        return channel -> client.execute(SimulateTemplateAction.INSTANCE, simulateRequest, new RestToXContentListener<>(channel));
    }
}
