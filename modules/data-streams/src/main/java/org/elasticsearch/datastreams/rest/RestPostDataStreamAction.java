/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.rest;

import org.elasticsearch.action.datastreams.PostDataStreamAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestPostDataStreamAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "post_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_data_stream/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ComposableIndexTemplate indexTemplate;
        if (request.hasContent()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                indexTemplate = ComposableIndexTemplate.parseNoIndexPatterns(parser);
            }
        } else {
            indexTemplate = null;
        }
        PostDataStreamAction.Request putDataStreamRequest = new PostDataStreamAction.Request(
            RestUtils.getMasterNodeTimeout(request),
            RestUtils.getAckTimeout(request),
            request.param("name"),
            indexTemplate
        );
        return channel -> client.execute(PostDataStreamAction.INSTANCE, putDataStreamRequest, new RestToXContentListener<>(channel));
    }
}
