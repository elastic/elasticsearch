/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.template.post.SimulateTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestSimulateTemplateAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_index_template/_simulate"), new Route(POST, "/_index_template/_simulate/{name}"));
    }

    @Override
    public String getName() {
        return "simulate_template_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        SimulateTemplateAction.Request simulateRequest = new SimulateTemplateAction.Request(
            getMasterNodeTimeout(request),
            request.param("name")
        );
        simulateRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        if (request.hasContent()) {
            TransportPutComposableIndexTemplateAction.Request indexTemplateRequest = new TransportPutComposableIndexTemplateAction.Request(
                "simulating_template"
            );
            try (var parser = request.contentParser()) {
                indexTemplateRequest.indexTemplate(ComposableIndexTemplate.parse(parser));
            }
            indexTemplateRequest.create(request.paramAsBoolean("create", false));
            indexTemplateRequest.cause(request.param("cause", "api"));

            simulateRequest.indexTemplateRequest(indexTemplateRequest);
        }

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            SimulateTemplateAction.INSTANCE,
            simulateRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
