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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.action.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.action.SqlClearCursorResponse;
import org.elasticsearch.xpack.sql.proto.Mode;

import static org.elasticsearch.xpack.sql.proto.CoreProtocol.HEADER_NAME_TOOK_NANOS;

public class SqlClearCursorResponseListener extends RestResponseListener<SqlClearCursorResponse> {
    private final long startNanos = System.nanoTime();
    private final RestRequest request;
    private final XContentType responseType;

    protected SqlClearCursorResponseListener(RestChannel channel, RestRequest request, SqlClearCursorRequest sqlRequest) {
        super(channel);
        this.request = request;
        Boolean binaryRequest = sqlRequest.binaryCommunication();
        if (Boolean.TRUE.equals(binaryRequest) || (binaryRequest == null && Mode.isDriver(sqlRequest.mode()))) {
            responseType = XContentType.CBOR;
        } else {
            responseType = XContentType.JSON;
        }
    }

    @Override
    public RestResponse buildResponse(SqlClearCursorResponse response) throws Exception {
        XContentBuilder builder = channel.newBuilder(request.getXContentType(), responseType, true);
        response.toXContent(builder, request);
        BytesRestResponse restResponse = new BytesRestResponse(RestStatus.OK, builder);
        restResponse.addHeader(HEADER_NAME_TOOK_NANOS, Long.toString(System.nanoTime() - startNanos));
        return restResponse;
    }

}
