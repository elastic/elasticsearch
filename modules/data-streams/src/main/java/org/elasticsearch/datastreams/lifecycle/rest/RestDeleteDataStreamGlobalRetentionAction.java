/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams.lifecycle.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.datastreams.lifecycle.action.DeleteDataStreamGlobalRetentionAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

@ServerlessScope(Scope.PUBLIC)
public class RestDeleteDataStreamGlobalRetentionAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "delete_data_stream_global_retention_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_data_stream/_global_retention"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteDataStreamGlobalRetentionAction.Request request = new DeleteDataStreamGlobalRetentionAction.Request();
        request.dryRun(restRequest.paramAsBoolean("dry_run", false));
        return channel -> client.execute(
            DeleteDataStreamGlobalRetentionAction.INSTANCE,
            request,
            new RestChunkedToXContentListener<>(channel)
        );
    }
}
