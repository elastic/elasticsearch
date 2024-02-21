/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.lifecycle.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.datastreams.lifecycle.action.PutDataStreamGlobalRetentionAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Updates the default_retention and the max_retention of the data stream global retention configuration. It
 * does not accept an empty payload.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestPutDataStreamGlobalRetentionAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "put_data_stream_global_retention_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_data_stream/_global_retention"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        try (XContentParser parser = restRequest.contentParser()) {
            PutDataStreamGlobalRetentionAction.Request request = PutDataStreamGlobalRetentionAction.Request.parseRequest(parser);
            request.dryRun(restRequest.paramAsBoolean("dry_run", false));
            return channel -> client.execute(
                PutDataStreamGlobalRetentionAction.INSTANCE,
                request,
                new RestChunkedToXContentListener<>(channel)
            );
        }
    }
}
