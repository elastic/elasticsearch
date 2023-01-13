/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestEsqlQueryAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestEsqlQueryAction.class);

    @Override
    public String getName() {
        return "esql_query";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(Route.builder(POST, "/_esql").build());
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        EsqlQueryRequest esqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            esqlRequest = EsqlQueryRequest.fromXContent(parser);
        }
        return channel -> client.execute(EsqlQueryAction.INSTANCE, esqlRequest, new ActionListener<>() {
            @Override
            public void onResponse(EsqlQueryResponse esqlQueryResponse) {
                try {
                    XContentBuilder builder = channel.newBuilder(request.getXContentType(), null, true);
                    esqlQueryResponse.toXContent(builder, request);
                    channel.sendResponse(new RestResponse(RestStatus.OK, builder));
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(new RestResponse(channel, e));
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.error("failed to send failure response", inner);
                }
            }
        });
    }
}
