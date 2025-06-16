/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestPutComponentTemplateAction extends BaseRestHandler {

    public static final String SUPPORTS_FAILURE_STORE_LIFECYCLE = "data_stream_options.failure_store.lifecycle";
    public static final String SUPPORTS_FAILURE_STORE = "data_stream_options.failure_store";
    private static final Set<String> capabilities = Set.of(SUPPORTS_FAILURE_STORE, SUPPORTS_FAILURE_STORE_LIFECYCLE);

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_component_template/{name}"), new Route(PUT, "/_component_template/{name}"));
    }

    @Override
    public String getName() {
        return "put_component_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        PutComponentTemplateAction.Request putRequest = new PutComponentTemplateAction.Request(request.param("name"));
        putRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        putRequest.create(request.paramAsBoolean("create", false));
        putRequest.cause(request.param("cause", "api"));
        try (var parser = request.contentParser()) {
            putRequest.componentTemplate(ComponentTemplate.parse(parser));
        }

        return channel -> client.execute(PutComponentTemplateAction.INSTANCE, putRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return capabilities;
    }
}
