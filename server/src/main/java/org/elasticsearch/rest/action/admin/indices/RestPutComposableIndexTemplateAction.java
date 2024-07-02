/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestPutComposableIndexTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_index_template/{name}"), new Route(PUT, "/_index_template/{name}"));
    }

    @Override
    public String getName() {
        return "put_composable_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        TransportPutComposableIndexTemplateAction.Request putRequest = new TransportPutComposableIndexTemplateAction.Request(
            request.param("name")
        );
        putRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        putRequest.create(request.paramAsBoolean("create", false));
        putRequest.cause(request.param("cause", "api"));
        try (var parser = request.contentParser()) {
            putRequest.indexTemplate(ComposableIndexTemplate.parse(parser));
        }

        return channel -> client.execute(TransportPutComposableIndexTemplateAction.TYPE, putRequest, new RestToXContentListener<>(channel));
    }
}
