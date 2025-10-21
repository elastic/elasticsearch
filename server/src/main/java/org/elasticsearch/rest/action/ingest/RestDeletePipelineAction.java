/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.DeletePipelineTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestDeletePipelineAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_ingest/pipeline/{id}"));
    }

    @Override
    public String getName() {
        return "ingest_delete_pipeline_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final var request = new DeletePipelineRequest(
            getMasterNodeTimeout(restRequest),
            getAckTimeout(restRequest),
            restRequest.param("id")
        );
        return channel -> client.execute(DeletePipelineTransportAction.TYPE, request, new RestToXContentListener<>(channel));
    }
}
