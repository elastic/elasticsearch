/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * Response to the request to clean all SQL resources associated with the cursor
 */
public class SqlClearCursorResponse extends ActionResponse implements StatusToXContentObject {

    private boolean succeeded;

    public SqlClearCursorResponse(boolean succeeded) {
        this.succeeded = succeeded;
    }

    SqlClearCursorResponse(StreamInput in) throws IOException {
        super(in);
        succeeded = in.readBoolean();
    }

    /**
     * @return Whether the attempt to clear a cursor was successful.
     */
    public boolean isSucceeded() {
        return succeeded;
    }

    public SqlClearCursorResponse setSucceeded(boolean succeeded) {
        this.succeeded = succeeded;
        return this;
    }

    @Override
    public RestStatus status() {
        return succeeded ? NOT_FOUND : OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("succeeded", succeeded);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(succeeded);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlClearCursorResponse response = (SqlClearCursorResponse) o;
        return succeeded == response.succeeded;
    }

    @Override
    public int hashCode() {
        return Objects.hash(succeeded);
    }
}
