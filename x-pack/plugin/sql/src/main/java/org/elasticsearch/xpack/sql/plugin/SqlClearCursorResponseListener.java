/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.action.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.action.SqlClearCursorResponse;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.sql.proto.CoreProtocol.HEADER_NAME_TOOK_NANOS;

public class SqlClearCursorResponseListener extends RestResponseListener<SqlClearCursorResponse> {
    private final long startNanos = System.nanoTime();
    RestRequest request;
    private final MediaType mediaType;

    protected SqlClearCursorResponseListener(RestChannel channel, RestRequest request, SqlClearCursorRequest sqlRequest) {
        super(channel);
        this.request = request;
        this.mediaType = SqlMediaTypeParser.getResponseMediaType(request, sqlRequest);
    }

    @Override
    public RestResponse buildResponse(SqlClearCursorResponse response) throws Exception {
        BytesRestResponse restResponse;
        if (mediaType instanceof XContentType xContentType) {
            XContentBuilder builder = channel.newBuilder(request.getXContentType(), xContentType, true);
            response.toXContent(builder, request);
            restResponse = new BytesRestResponse(RestStatus.OK, builder);
        } else {
            // TextFormat
            TextFormat type = (TextFormat) mediaType;
            final String data = "";
            restResponse = new BytesRestResponse(RestStatus.OK, type.contentType(request), data.getBytes(StandardCharsets.UTF_8));
        }
        restResponse.addHeader(HEADER_NAME_TOOK_NANOS, Long.toString(System.nanoTime() - startNanos));
        return restResponse;
    }

}
