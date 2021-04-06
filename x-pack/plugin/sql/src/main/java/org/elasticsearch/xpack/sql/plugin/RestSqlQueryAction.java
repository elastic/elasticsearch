/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.MediaType;
import org.elasticsearch.common.xcontent.MediaTypeRegistry;
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
import org.elasticsearch.xpack.sql.proto.Protocol;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.sql.proto.Protocol.URL_PARAM_DELIMITER;

public class RestSqlQueryAction extends BaseRestHandler {

    private final SqlMediaTypeParser sqlMediaTypeParser = new SqlMediaTypeParser();

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, Protocol.SQL_QUERY_REST_ENDPOINT),
            new Route(POST, Protocol.SQL_QUERY_REST_ENDPOINT));
    }

    public MediaTypeRegistry<? extends MediaType> validAcceptMediaTypes() {
        return SqlMediaTypeParser.MEDIA_TYPE_REGISTRY;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
            throws IOException {
        SqlQueryRequest sqlRequest;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            sqlRequest = SqlQueryRequest.fromXContent(parser);
        }

        MediaType responseMediaType = sqlMediaTypeParser.getResponseMediaType(request, sqlRequest);
        if (responseMediaType == null) {
            String msg = String.format(Locale.ROOT, "Invalid response content type: Accept=[%s], Content-Type=[%s], format=[%s]",
                request.header("Accept"), request.header("Content-Type"), request.param("format"));
            throw new IllegalArgumentException(msg);
        }

        /*
         * Special handling for the "delimiter" parameter which should only be
         * checked for being present or not in the case of CSV format. We cannot
         * override {@link BaseRestHandler#responseParams()} because this
         * parameter should only be checked for CSV, not always.
         */
        if ((responseMediaType instanceof XContentType || ((TextFormat) responseMediaType) != TextFormat.CSV)
            && request.hasParam(URL_PARAM_DELIMITER)) {
            throw new IllegalArgumentException(unrecognized(request, Collections.singleton(URL_PARAM_DELIMITER), emptySet(), "parameter"));
        }

        long startNanos = System.nanoTime();
        return channel -> client.execute(SqlQueryAction.INSTANCE, sqlRequest, new RestResponseListener<SqlQueryResponse>(channel) {
            @Override
            public RestResponse buildResponse(SqlQueryResponse response) throws Exception {
                RestResponse restResponse;

                // XContent branch
                if (responseMediaType instanceof XContentType) {
                    XContentType type = (XContentType) responseMediaType;
                    XContentBuilder builder = channel.newBuilder(request.getXContentType(), type, true);
                    response.toXContent(builder, request);
                    restResponse = new BytesRestResponse(RestStatus.OK, builder);
                } else { // TextFormat
                    TextFormat type = (TextFormat) responseMediaType;
                    final String data = type.format(request, response);

                    restResponse = new BytesRestResponse(RestStatus.OK, type.contentType(request), data.getBytes(StandardCharsets.UTF_8));

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
    protected Set<String> responseParams() {
        return Collections.singleton(URL_PARAM_DELIMITER);
    }

    @Override
    public String getName() {
        return "sql_query";
    }
}
