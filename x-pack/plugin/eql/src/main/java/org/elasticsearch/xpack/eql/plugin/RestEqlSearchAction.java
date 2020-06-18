/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestEqlSearchAction extends BaseRestHandler {
    private static final String SEARCH_PATH = "/{index}/_eql/search";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, SEARCH_PATH),
            new Route(POST, SEARCH_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
        throws IOException {

        EqlSearchRequest eqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            eqlRequest = EqlSearchRequest.fromXContent(parser);
            eqlRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
            eqlRequest.indicesOptions(IndicesOptions.fromRequest(request, eqlRequest.indicesOptions()));
            if (request.hasParam("wait_for_completion_timeout")) {
                eqlRequest.waitForCompletionTimeout(
                    request.paramAsTime("wait_for_completion_timeout", eqlRequest.waitForCompletionTimeout()));
            }
            if (request.hasParam("keep_alive")) {
                eqlRequest.keepAlive(request.paramAsTime("keep_alive", eqlRequest.keepAlive()));
            }
            eqlRequest.keepOnCompletion(request.paramAsBoolean("keep_on_completion", eqlRequest.keepOnCompletion()));
        }

        return channel -> {
            RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancellableClient.execute(EqlSearchAction.INSTANCE, eqlRequest, new RestResponseListener<>(channel) {
                @Override
                public RestResponse buildResponse(EqlSearchResponse response) throws Exception {
                    XContentBuilder builder = channel.newBuilder(request.getXContentType(), XContentType.JSON, true);
                    response.toXContent(builder, request);
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            });
        };
    }

    @Override
    public String getName() {
        return "eql_search";
    }
}
