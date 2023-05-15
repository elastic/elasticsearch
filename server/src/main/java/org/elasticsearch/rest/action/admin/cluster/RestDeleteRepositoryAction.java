/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.repositories.RepositoryConflictException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

/**
 * Unregisters a repository
 */
public class RestDeleteRepositoryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_snapshot/{repository}"));
    }

    @Override
    public String getName() {
        return "delete_repository_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String name = request.param("repository");
        DeleteRepositoryRequest deleteRepositoryRequest = new DeleteRepositoryRequest(name);
        deleteRepositoryRequest.timeout(request.paramAsTime("timeout", deleteRepositoryRequest.timeout()));
        deleteRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteRepositoryRequest.masterNodeTimeout()));
        return channel -> client.admin()
            .cluster()
            .deleteRepository(
                deleteRepositoryRequest,
                new RestToXContentListener<AcknowledgedResponse>(channel).delegateResponse((delegate, err) -> {
                    if (request.getRestApiVersion().equals(RestApiVersion.V_7) && err instanceof RepositoryConflictException) {
                        delegate.onFailure(new IllegalStateException(((RepositoryConflictException) err).getBackwardCompatibleMessage()));
                    } else {
                        delegate.onFailure(err);
                    }
                })
            );
    }
}
