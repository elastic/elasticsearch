/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.CLIENT_ID;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.CURSOR;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.MODE;
import static org.elasticsearch.xpack.sql.action.AbstractSqlQueryRequest.VERSION;

/**
 * Request to clean all SQL resources associated with the cursor
 */
public class SqlClearCursorRequest extends AbstractSqlRequest {

    private static final ConstructingObjectParser<SqlClearCursorRequest, Void> PARSER =
        // here the position in "objects" is the same as the fields parser declarations below
        new ConstructingObjectParser<>(SqlClearCursorAction.NAME, objects -> {
            RequestInfo requestInfo = new RequestInfo(Mode.fromString((String) objects[1]), (String) objects[2]);
            return new SqlClearCursorRequest(requestInfo, (String) objects[0]);
        });

    static {
        // "cursor" is required constructor parameter
        PARSER.declareString(constructorArg(), CURSOR);
        PARSER.declareString(optionalConstructorArg(), MODE);
        PARSER.declareString(optionalConstructorArg(), CLIENT_ID);
        PARSER.declareString(optionalConstructorArg(), VERSION);
    }

    private String cursor;

    public SqlClearCursorRequest() {}

    public SqlClearCursorRequest(RequestInfo requestInfo, String cursor) {
        super(requestInfo);
        this.cursor = cursor;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
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

    public SqlClearCursorRequest(StreamInput in) throws IOException {
        super(in);
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
        if (super.equals(o) == false) return false;
        SqlClearCursorRequest that = (SqlClearCursorRequest) o;
        return Objects.equals(cursor, that.cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cursor);
    }

    public static SqlClearCursorRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
