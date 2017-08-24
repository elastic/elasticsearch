/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class SqlRequest extends AbstractSqlRequest {

    public static final ObjectParser<SqlRequest, Void> PARSER = objectParser(SqlRequest::new);

    public static final ParseField CURSOR = new ParseField("cursor");

    static {
        PARSER.declareString((request, nextPage) -> request.cursor(Cursor.decodeFromString(nextPage)), CURSOR);
    }

    private Cursor cursor = Cursor.EMPTY;

    public SqlRequest() {}

    public SqlRequest(String query, DateTimeZone timeZone, int fetchSize, Cursor cursor) {
        super(query, timeZone, fetchSize);
        this.cursor = cursor;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if ((false == Strings.hasText(query())) && cursor == Cursor.EMPTY) {
            validationException = addValidationError("one of [query] or [cursor] is required", validationException);
        }
        return validationException;
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results.
     */
    public Cursor cursor() {
        return cursor;
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results.
     */
    public SqlRequest cursor(Cursor cursor) {
        if (cursor == null) {
            throw new IllegalArgumentException("cursor may not be null.");
        }
        this.cursor = cursor;
        return this;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        cursor = in.readNamedWriteable(Cursor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cursor);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(cursor, ((SqlRequest) obj).cursor);
    }

    @Override
    public String getDescription() {
        return "SQL [" + query() + "]";
    }
}