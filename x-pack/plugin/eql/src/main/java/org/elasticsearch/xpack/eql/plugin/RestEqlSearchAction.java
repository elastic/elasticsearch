/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestEqlSearchAction extends BaseRestHandler {
    private static final String SEARCH_PATH = "/{index}/_eql/search";

    public RestEqlSearchAction(RestController controller) {
        controller.registerHandler(GET, SEARCH_PATH, this);
        controller.registerHandler(POST, SEARCH_PATH, this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
        throws IOException {

        EqlSearchRequest eqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            eqlRequest = EqlSearchRequest.fromXContent(parser);
            eqlRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
        }

        return channel -> client.execute(EqlSearchAction.INSTANCE, eqlRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(EqlSearchResponse response) throws Exception {
                XContentBuilder builder = channel.newBuilder(request.getXContentType(), XContentType.JSON, true);
                response.toXContent(builder, request);
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    public String getName() {
        return "eql_search";
    }
}
