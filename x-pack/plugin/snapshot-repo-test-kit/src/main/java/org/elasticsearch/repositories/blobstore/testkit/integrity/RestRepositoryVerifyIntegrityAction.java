/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.INTERNAL)
public class RestRepositoryVerifyIntegrityAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_verify_integrity"));
    }

    @Override
    public String getName() {
        return "repository_verify_integrity";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final var requestParams = new RepositoryVerifyIntegrityParams(request);
        return channel -> {
            final var responseStream = new RepositoryVerifyIntegrityResponseStream(channel);
            new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
                TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE,
                new TransportRepositoryVerifyIntegrityCoordinationAction.Request(requestParams, responseStream),
                responseStream.getCompletionListener()
            );
        };
    }
}
