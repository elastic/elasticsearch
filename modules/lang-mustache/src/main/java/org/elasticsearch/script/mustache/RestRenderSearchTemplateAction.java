/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestRenderSearchTemplateAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_render/template"),
            new Route(POST, "/_render/template"),
            new Route(GET, "/_render/template/{id}"),
            new Route(POST, "/_render/template/{id}")
        );
    }

    @Override
    public String getName() {
        return "render_search_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Creates the render template request
        SearchTemplateRequest renderRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            renderRequest = SearchTemplateRequest.fromXContent(parser);
        }
        renderRequest.setSimulate(true);

        String id = request.param("id");
        if (id != null) {
            renderRequest.setScriptType(ScriptType.STORED);
            renderRequest.setScript(id);
        }

        return channel -> client.execute(SearchTemplateAction.INSTANCE, renderRequest, new RestToXContentListener<>(channel));
    }
}
