/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

@ServerlessScope(Scope.PUBLIC)
public class RestGetComponentTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_component_template"),
            new Route(GET, "/_component_template/{name}"),
            new Route(HEAD, "/_component_template/{name}")
        );
    }

    @Override
    public String getName() {
        return "get_component_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        final GetComponentTemplateAction.Request getRequest = new GetComponentTemplateAction.Request(request.param("name"));
        if (DataStreamLifecycle.isEnabled()) {
            getRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        }
        getRequest.local(request.paramAsBoolean("local", getRequest.local()));
        getRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getRequest.masterNodeTimeout()));

        final boolean implicitAll = getRequest.name() == null;

        return channel -> client.execute(GetComponentTemplateAction.INSTANCE, getRequest, new RestToXContentListener<>(channel) {
            @Override
            protected RestStatus getStatus(final GetComponentTemplateAction.Response response) {
                final boolean templateExists = response.getComponentTemplates().isEmpty() == false;
                return (templateExists || implicitAll) ? OK : NOT_FOUND;
            }
        });
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

}
