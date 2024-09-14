/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.repositories.RepositoryConflictException;
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

/**
 * Unregisters a repository
 */
@ServerlessScope(Scope.INTERNAL)
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
        final var deleteRepositoryRequest = new DeleteRepositoryRequest(getMasterNodeTimeout(request), getAckTimeout(request), name);
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
