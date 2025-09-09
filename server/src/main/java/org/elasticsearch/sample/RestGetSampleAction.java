/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sample;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.project.ProjectIdResolver;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetSampleAction extends BaseRestHandler {
    private final ProjectIdResolver projectIdResolver;

    public RestGetSampleAction(ProjectIdResolver projectIdResolver) {
        this.projectIdResolver = projectIdResolver;
    }

    @Override
    public String getName() {
        return "get_sample";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_sample/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        GetSampleAction.Request getSampleRequest = new GetSampleAction.Request(
            projectIdResolver.getProjectId(),
            new String[] { request.param("name") }
        );
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            GetSampleAction.INSTANCE,
            getSampleRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }
}
