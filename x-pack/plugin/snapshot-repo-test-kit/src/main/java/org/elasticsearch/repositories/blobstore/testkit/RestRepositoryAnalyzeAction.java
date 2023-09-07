/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.INTERNAL)
public class RestRepositoryAnalyzeAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_analyze"));
    }

    @Override
    public String getName() {
        return "repository_analyze";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        RepositoryAnalyzeAction.Request analyzeRepositoryRequest = new RepositoryAnalyzeAction.Request(request.param("repository"));

        analyzeRepositoryRequest.blobCount(request.paramAsInt("blob_count", analyzeRepositoryRequest.getBlobCount()));
        analyzeRepositoryRequest.concurrency(request.paramAsInt("concurrency", analyzeRepositoryRequest.getConcurrency()));
        analyzeRepositoryRequest.readNodeCount(request.paramAsInt("read_node_count", analyzeRepositoryRequest.getReadNodeCount()));
        analyzeRepositoryRequest.earlyReadNodeCount(
            request.paramAsInt("early_read_node_count", analyzeRepositoryRequest.getEarlyReadNodeCount())
        );
        analyzeRepositoryRequest.seed(request.paramAsLong("seed", analyzeRepositoryRequest.getSeed()));
        analyzeRepositoryRequest.rareActionProbability(
            request.paramAsDouble("rare_action_probability", analyzeRepositoryRequest.getRareActionProbability())
        );
        analyzeRepositoryRequest.maxBlobSize(request.paramAsSize("max_blob_size", analyzeRepositoryRequest.getMaxBlobSize()));
        analyzeRepositoryRequest.maxTotalDataSize(
            request.paramAsSize("max_total_data_size", analyzeRepositoryRequest.getMaxTotalDataSize())
        );
        analyzeRepositoryRequest.timeout(request.paramAsTime("timeout", analyzeRepositoryRequest.getTimeout()));
        analyzeRepositoryRequest.detailed(request.paramAsBoolean("detailed", analyzeRepositoryRequest.getDetailed()));
        analyzeRepositoryRequest.abortWritePermitted(
            request.paramAsBoolean("rarely_abort_writes", analyzeRepositoryRequest.isAbortWritePermitted())
        );

        RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
        return channel -> cancelClient.execute(
            RepositoryAnalyzeAction.INSTANCE,
            analyzeRepositoryRequest,
            new RestStatusToXContentListener<>(channel) {
                @Override
                public RestResponse buildResponse(RepositoryAnalyzeAction.Response response, XContentBuilder builder) throws Exception {
                    builder.humanReadable(request.paramAsBoolean("human", true));
                    return super.buildResponse(response, builder);
                }
            }
        );
    }
}
