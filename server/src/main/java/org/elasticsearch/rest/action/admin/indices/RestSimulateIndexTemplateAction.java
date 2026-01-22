/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.post.SimulateIndexTemplateRequest;
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
public class RestSimulateIndexTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_index_template/_simulate_index/{name}"));
    }

    @Override
    public String getName() {
        return "simulate_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        SimulateIndexTemplateRequest simulateIndexTemplateRequest = new SimulateIndexTemplateRequest(
            getMasterNodeTimeout(request),
            request.param("name")
        );
        simulateIndexTemplateRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        if (request.hasContent()) {
            TransportPutComposableIndexTemplateAction.Request indexTemplateRequest = new TransportPutComposableIndexTemplateAction.Request(
                "simulating_template"
            );
            try (var parser = request.contentParser()) {
                indexTemplateRequest.indexTemplate(ComposableIndexTemplate.parse(parser));
            }
            indexTemplateRequest.create(request.paramAsBoolean("create", false));
            indexTemplateRequest.cause(request.param("cause", "api"));

            simulateIndexTemplateRequest.indexTemplateRequest(indexTemplateRequest);
        }

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            SimulateIndexTemplateAction.INSTANCE,
            simulateIndexTemplateRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
