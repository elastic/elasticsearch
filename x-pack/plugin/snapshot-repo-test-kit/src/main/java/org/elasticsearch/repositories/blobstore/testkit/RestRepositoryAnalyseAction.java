/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRepositoryAnalyseAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_analyse"));
    }

    @Override
    public String getName() {
        return "repository_analyse";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        RepositoryAnalyseAction.Request analyseRepositoryRequest = new RepositoryAnalyseAction.Request(request.param("repository"));

        analyseRepositoryRequest.blobCount(request.paramAsInt("blob_count", analyseRepositoryRequest.getBlobCount()));
        analyseRepositoryRequest.concurrency(request.paramAsInt("concurrency", analyseRepositoryRequest.getConcurrency()));
        analyseRepositoryRequest.readNodeCount(request.paramAsInt("read_node_count", analyseRepositoryRequest.getReadNodeCount()));
        analyseRepositoryRequest.earlyReadNodeCount(
            request.paramAsInt("early_read_node_count", analyseRepositoryRequest.getEarlyReadNodeCount())
        );
        analyseRepositoryRequest.seed(request.paramAsLong("seed", analyseRepositoryRequest.getSeed()));
        analyseRepositoryRequest.rareActionProbability(
            request.paramAsDouble("rare_action_probability", analyseRepositoryRequest.getRareActionProbability())
        );
        analyseRepositoryRequest.maxBlobSize(request.paramAsSize("max_blob_size", analyseRepositoryRequest.getMaxBlobSize()));
        analyseRepositoryRequest.maxTotalDataSize(
            request.paramAsSize("max_total_data_size", analyseRepositoryRequest.getMaxTotalDataSize())
        );
        analyseRepositoryRequest.timeout(request.paramAsTime("timeout", analyseRepositoryRequest.getTimeout()));
        analyseRepositoryRequest.detailed(request.paramAsBoolean("detailed", analyseRepositoryRequest.getDetailed()));

        RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
        return channel -> cancelClient.execute(
            RepositoryAnalyseAction.INSTANCE,
            analyseRepositoryRequest,
            new RestStatusToXContentListener<>(channel)
        );
    }
}
