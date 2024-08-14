/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.multiproject.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestPutProjectAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "/_project"));
    }

    @Override
    public String getName() {
        return "put_project";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final PutProjectAction.Request putProjectRequest;
        try (var parser = restRequest.contentParser()) {
            putProjectRequest = PutProjectAction.Request.parseRequest(
                (ProjectId projectId) -> new PutProjectAction.Request(
                    getMasterNodeTimeout(restRequest),
                    getAckTimeout(restRequest),
                    projectId
                ),
                parser
            );
        }
        return channel -> client.execute(PutProjectAction.INSTANCE, putProjectRequest, new RestToXContentListener<>(channel));
    }
}
