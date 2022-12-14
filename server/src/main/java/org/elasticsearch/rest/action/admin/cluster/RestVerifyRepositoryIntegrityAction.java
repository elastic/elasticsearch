/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.repositories.integrity.VerifyRepositoryIntegrityAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestVerifyRepositoryIntegrityAction extends BaseRestHandler {
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_verify_integrity"));
    }

    @Override
    public String getName() {
        return "verify_repository_integrity_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final var verifyRequest = new VerifyRepositoryIntegrityAction.Request(
            request.param("repository"),
            request.paramAsInt("threadpool_concurrency", 0),
            request.paramAsInt("snapshot_verification_concurrency", 5),
            request.paramAsInt("index_verification_concurrency", 5),
            request.paramAsInt("index_snapshot_verification_concurrency", 5),
            request.paramAsInt("max_failures", 10000),
            request.paramAsBoolean("permit_missing_snapshot_details", false)
        );
        verifyRequest.masterNodeTimeout(request.paramAsTime("master_timeout", verifyRequest.masterNodeTimeout()));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .execute(VerifyRepositoryIntegrityAction.INSTANCE, verifyRequest, new RestChunkedToXContentListener<>(channel));
    }
}
