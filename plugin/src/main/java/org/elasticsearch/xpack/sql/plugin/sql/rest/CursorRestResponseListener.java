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
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.session.RowView;
import org.elasticsearch.xpack.sql.type.Schema.Entry;
import org.elasticsearch.xpack.sql.util.ThrowableBiConsumer;
import org.elasticsearch.xpack.sql.util.ThrowableConsumer;

import static org.elasticsearch.rest.RestStatus.OK;

class CursorRestResponseListener extends RestBuilderListener<SqlResponse> {

    CursorRestResponseListener(RestChannel channel) {
        super(channel);
    }

    @Override
    public RestResponse buildResponse(SqlResponse response, XContentBuilder builder) throws Exception {
        return new BytesRestResponse(OK, createResponse(response.rowSetCursor(), builder));
    }

    static XContentBuilder createResponse(RowSetCursor cursor, XContentBuilder builder) throws Exception {
        builder.startObject();
        // header
        builder.field("size", cursor.size());
        builder.startObject("columns");

        ThrowableConsumer<Entry> buildSchema = e -> builder.startObject(e.name()).field("type", e.type().esName()).endObject();
        cursor.schema().forEach(buildSchema);

        builder.endObject();

        // payload
        builder.startArray("rows");
        
        ThrowableBiConsumer<Object, Entry> eachColumn = (v, e) -> builder.field(e.name(), v);
        ThrowableConsumer<RowView> eachRow = r -> { builder.startObject(); r.forEachColumn(eachColumn); builder.endObject(); };

        cursor.forEachRow(eachRow);
        
        builder.endArray();
        builder.endObject();

        builder.close();
        return builder;
    }
}