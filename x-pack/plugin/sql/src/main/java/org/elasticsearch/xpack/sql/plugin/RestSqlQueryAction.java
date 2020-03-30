/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSqlQueryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, Protocol.SQL_QUERY_REST_ENDPOINT),
            new Route(POST, Protocol.SQL_QUERY_REST_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
            throws IOException {
        SqlQueryRequest sqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            sqlRequest = SqlQueryRequest.fromXContent(parser);
        }

        /*
         * Since we support {@link TextFormat} <strong>and</strong>
         * {@link XContent} outputs we can't use {@link RestToXContentListener}
         * like everything else. We want to stick as closely as possible to
         * Elasticsearch's defaults though, while still layering in ways to
         * control the output more easily.
         *
         * First we find the string that the user used to specify the response
         * format. If there is a {@code format} parameter we use that. If there
         * isn't but there is a {@code Accept} header then we use that. If there
         * isn't then we use the {@code Content-Type} header which is required.
         */
        String accept = null;

        if (Mode.isDedicatedClient(sqlRequest.requestInfo().mode())
                && (sqlRequest.binaryCommunication() == null || sqlRequest.binaryCommunication())) {
            // enforce CBOR response for drivers and CLI (unless instructed differently through the config param)
            accept = XContentType.CBOR.name();
        } else {
            accept = request.param("format");
        }
        if (accept == null) {
            accept = request.header("Accept");
            if ("*/*".equals(accept)) {
                // */* means "I don't care" which we should treat like not specifying the header
                accept = null;
            }
        }
        if (accept == null) {
            accept = request.header("Content-Type");
        }
        assert accept != null : "The Content-Type header is required";

        /*
         * Second, we pick the actual content type to use by first parsing the
         * string from the previous step as an {@linkplain XContent} value. If
         * that doesn't parse we parse it as a {@linkplain TextFormat} value. If
         * that doesn't parse it'll throw an {@link IllegalArgumentException}
         * which we turn into a 400 error.
         */
        XContentType xContentType = accept == null ? XContentType.JSON : XContentType.fromMediaTypeOrFormat(accept);
        TextFormat textFormat = xContentType == null ? TextFormat.fromMediaTypeOrFormat(accept) : null;

        if (xContentType == null && sqlRequest.columnar()) {
            throw new IllegalArgumentException("Invalid use of [columnar] argument: cannot be used in combination with "
                    + "txt, csv or tsv formats");
        }

        long startNanos = System.nanoTime();
        return channel -> client.execute(SqlQueryAction.INSTANCE, sqlRequest, new RestResponseListener<SqlQueryResponse>(channel) {
            @Override
            public RestResponse buildResponse(SqlQueryResponse response) throws Exception {
                RestResponse restResponse;

                // XContent branch
                if (xContentType != null) {
                    XContentBuilder builder = channel.newBuilder(request.getXContentType(), xContentType, true);
                    response.toXContent(builder, request);
                    restResponse = new BytesRestResponse(RestStatus.OK, builder);
                }
                // TextFormat
                else {
                    final String data = textFormat.format(request, response);

                    restResponse = new BytesRestResponse(RestStatus.OK, textFormat.contentType(request),
                        data.getBytes(StandardCharsets.UTF_8));

                    if (response.hasCursor()) {
                        restResponse.addHeader("Cursor", response.cursor());
                    }
                }

                restResponse.addHeader("Took-nanos", Long.toString(System.nanoTime() - startNanos));
                return restResponse;
            }
        });
    }

    @Override
    public String getName() {
        return "sql_query";
    }
}
