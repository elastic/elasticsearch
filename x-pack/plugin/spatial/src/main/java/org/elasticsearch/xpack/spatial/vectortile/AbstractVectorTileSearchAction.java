/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Base class for rest actions that performs a search and translates it into
 * a protobuf response
 */
public abstract class AbstractVectorTileSearchAction extends BaseRestHandler {

    private static final String INDEX_PARAM = "index";
    private static final String FIELD_PARAM = "field";
    private static final String Z_PARAM = "z";
    private static final String X_PARAM = "x";
    private static final String Y_PARAM = "y";

    @FunctionalInterface
    protected interface ResponseBuilder {
        void buildResponse(SearchResponse searchResponse, OutputStream outputStream) throws IOException;
    }

    protected abstract ResponseBuilder doParseRequest(
        RestRequest restRequest, String field, int z, int x, int y, SearchRequestBuilder searchRequestBuilder) throws IOException;

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final String index = restRequest.param(INDEX_PARAM);
        final String field = restRequest.param(FIELD_PARAM);
        final int z = Integer.parseInt(restRequest.param(Z_PARAM));
        final int x = Integer.parseInt(restRequest.param(X_PARAM));
        final int y = Integer.parseInt(restRequest.param(Y_PARAM));

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(Strings.splitStringByCommaToArray(index));

        ResponseBuilder responseBuilder = doParseRequest(restRequest, field, z, x, y, searchRequestBuilder);

        // TODO: how do we handle cancellations?
        return channel -> searchRequestBuilder.execute(new RestResponseListener<>(channel) {

            @Override
            public RestResponse buildResponse(SearchResponse searchResponse) throws Exception {
                try (BytesStream bytesOut = Streams.flushOnCloseStream(channel.bytesOutput())) {
                    responseBuilder.buildResponse(searchResponse, bytesOut);
                    return new BytesRestResponse(RestStatus.OK, "application/x-protobuf", bytesOut.bytes());
                }
            }
        });
    }

}
