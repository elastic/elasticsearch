/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.repositories.blobstore.testkit.RepositorySpeedTestAction;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRepositorySpeedTestAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_speed_test"));
    }

    @Override
    public String getName() {
        return "repository_speed_test";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        RepositorySpeedTestAction.Request verifyRepositoryRequest = new RepositorySpeedTestAction.Request(request.param("repository"));

        verifyRepositoryRequest.blobCount(request.paramAsInt("blob_count", verifyRepositoryRequest.getBlobCount()));
        verifyRepositoryRequest.concurrency(request.paramAsInt("concurrency", verifyRepositoryRequest.getConcurrency()));
        verifyRepositoryRequest.seed(request.paramAsLong("seed", verifyRepositoryRequest.getSeed()));
        verifyRepositoryRequest.maxBlobSize(request.paramAsSize("max_blob_size", verifyRepositoryRequest.getMaxBlobSize()));
        verifyRepositoryRequest.timeout(request.paramAsTime("timeout", verifyRepositoryRequest.getTimeout()));
        verifyRepositoryRequest.masterNodeTimeout(request.paramAsTime("master_timeout", verifyRepositoryRequest.masterNodeTimeout()));

        RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
        return channel -> cancelClient.execute(
            RepositorySpeedTestAction.INSTANCE,
            verifyRepositoryRequest,
            new RestStatusToXContentListener<>(channel)
        );
    }
}
