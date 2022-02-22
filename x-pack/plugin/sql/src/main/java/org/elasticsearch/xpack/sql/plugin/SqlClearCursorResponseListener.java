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

public class SqlClearCursorResponseListener extends RestResponseListener<SqlClearCursorResponse> {

    RestRequest request;
    private final MediaType mediaType;

    protected SqlClearCursorResponseListener(RestChannel channel, RestRequest request, SqlClearCursorRequest sqlRequest) {
        super(channel);
        this.request = request;
        this.mediaType = SqlMediaTypeParser.getResponseMediaType(request, sqlRequest);
    }

    @Override
    public RestResponse buildResponse(SqlClearCursorResponse response) throws Exception {
        assert mediaType instanceof XContentType;
        XContentBuilder builder = channel.newBuilder(request.getXContentType(), (XContentType) mediaType, true);
        response.toXContent(builder, request);
        return new BytesRestResponse(RestStatus.OK, builder);
    }

}
