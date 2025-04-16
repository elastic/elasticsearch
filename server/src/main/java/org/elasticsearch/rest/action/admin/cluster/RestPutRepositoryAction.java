/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

/**
 * Registers repositories
 */
@ServerlessScope(Scope.INTERNAL)
public class RestPutRepositoryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}"), new Route(PUT, "/_snapshot/{repository}"));
    }

    @Override
    public String getName() {
        return "put_repository_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var putRepositoryRequest = new PutRepositoryRequest(
            getMasterNodeTimeout(request),
            getAckTimeout(request),
            request.param("repository")
        );
        try (XContentParser parser = request.contentParser()) {
            putRepositoryRequest.source(parser.mapOrdered());
        }
        putRepositoryRequest.verify(request.paramAsBoolean("verify", true));
        return channel -> client.admin().cluster().putRepository(putRepositoryRequest, new RestToXContentListener<>(channel));
    }
}
