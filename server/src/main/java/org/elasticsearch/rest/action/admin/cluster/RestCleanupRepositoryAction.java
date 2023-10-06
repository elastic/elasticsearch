/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Cleans up a repository
 */
@ServerlessScope(Scope.INTERNAL)
public class RestCleanupRepositoryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_cleanup"));
    }

    @Override
    public String getName() {
        return "cleanup_repository_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String name = request.param("repository");
        CleanupRepositoryRequest cleanupRepositoryRequest = new CleanupRepositoryRequest(name);
        cleanupRepositoryRequest.timeout(request.paramAsTime("timeout", cleanupRepositoryRequest.timeout()));
        cleanupRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", cleanupRepositoryRequest.masterNodeTimeout()));
        return channel -> client.admin().cluster().cleanupRepository(cleanupRepositoryRequest, new RestToXContentListener<>(channel));
    }
}
