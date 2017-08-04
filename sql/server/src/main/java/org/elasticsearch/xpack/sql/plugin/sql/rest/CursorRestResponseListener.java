/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.rest;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;
import org.elasticsearch.xpack.sql.util.ThrowableBiConsumer;
import org.elasticsearch.xpack.sql.util.ThrowableConsumer;

import java.util.Map;

import static org.elasticsearch.rest.RestStatus.OK;

class CursorRestResponseListener extends RestBuilderListener<SqlResponse> {

    CursorRestResponseListener(RestChannel channel) {
        super(channel);
    }

    @Override
    public RestResponse buildResponse(SqlResponse response, XContentBuilder builder) throws Exception {
        return new BytesRestResponse(OK, createResponse(response, builder));
    }

    private static XContentBuilder createResponse(SqlResponse response, XContentBuilder builder) throws Exception {
        builder.startObject();
        // header
        builder.field("size", response.size());
        // NOCOMMIT: that should be a list since order is important
        builder.startObject("columns");

        ThrowableBiConsumer<String, String> buildSchema = (f, t) -> builder.startObject(f).field("type", t).endObject();
        response.columns().forEach(buildSchema);

        builder.endObject();

        // payload
        builder.startArray("rows");
        
        ThrowableBiConsumer<String, Object> eachColumn = builder::field;
        // NOCOMMIT: that should be a list since order is important
        ThrowableConsumer<Map<String, Object>> eachRow = r -> { builder.startObject(); r.forEach(eachColumn); builder.endObject(); };

        response.rows().forEach(eachRow);
        
        builder.endArray();
        builder.endObject();

        builder.close();
        return builder;
    }
}