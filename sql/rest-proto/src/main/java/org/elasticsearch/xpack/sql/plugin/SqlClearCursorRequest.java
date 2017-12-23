/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to clean all SQL resources associated with the cursor
 */
public class SqlClearCursorRequest extends ActionRequest implements ToXContentObject {

    public static final ObjectParser<SqlClearCursorRequest, Void> PARSER =
            new ObjectParser<>(SqlClearCursorAction.NAME, SqlClearCursorRequest::new);

    public static final ParseField CURSOR = new ParseField("cursor");

    static {
        PARSER.declareString(SqlClearCursorRequest::setCursor, CURSOR);
    }

    private String cursor;

    public SqlClearCursorRequest() {

    }

    public SqlClearCursorRequest(String cursor) {
        this.cursor = cursor;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (getCursor() == null) {
            validationException = addValidationError("cursor is required", validationException);
        }
        return validationException;
    }

    public String getCursor() {
        return cursor;
    }

    public SqlClearCursorRequest setCursor(String cursor) {
        this.cursor = cursor;
        return this;
    }

    @Override
    public String getDescription() {
        return "SQL Clean cursor [" + getCursor() + "]";
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cursor = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(cursor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SqlClearCursorRequest request = (SqlClearCursorRequest) o;
        return Objects.equals(cursor, request.cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("cursor", cursor);
        builder.endObject();
        return builder;
    }
}