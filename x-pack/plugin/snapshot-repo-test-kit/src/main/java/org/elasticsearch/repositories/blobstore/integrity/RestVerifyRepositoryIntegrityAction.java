/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.integrity;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

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
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final var verifyRequest = new VerifyRepositoryIntegrityAction.Request(
            request.param("repository"),
            request.param("results_index", ""),
            request.paramAsInt("metadata_threads", 0),
            request.paramAsInt("data_threads", 0),
            request.paramAsInt("snapshot_verification_concurrency", 0),
            request.paramAsInt("index_verification_concurrency", 0),
            request.paramAsInt("index_snapshot_verification_concurrency", 0),
            request.paramAsBoolean("verify_blob_contents", false),
            request.paramAsSize("max_verify_bytes_per_sec", ByteSizeValue.ofMb(10)),
            request.paramAsBoolean("wait_for_completion", false)
        );
        verifyRequest.masterNodeTimeout(request.paramAsTime("master_timeout", verifyRequest.masterNodeTimeout()));
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            VerifyRepositoryIntegrityAction.INSTANCE,
            verifyRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
