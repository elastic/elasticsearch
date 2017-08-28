/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
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

public class SqlRequest extends ActionRequest implements CompositeIndicesRequest {
    public static final ParseField CURSOR = new ParseField("cursor");
    public static final ObjectParser<SqlRequest, Void> PARSER = new ObjectParser<>("sql/query", SqlRequest::new);
    static {
        PARSER.declareString(SqlRequest::query, new ParseField("query"));
        PARSER.declareString((request, zoneId) -> request.timeZone(DateTimeZone.forID(zoneId)), new ParseField("time_zone"));
        PARSER.declareInt(SqlRequest::fetchSize, new ParseField("fetch_size"));
        PARSER.declareString((request, nextPage) -> request.cursor(Cursor.decodeFromString(nextPage)), CURSOR);
    }

    public static final DateTimeZone DEFAULT_TIME_ZONE = DateTimeZone.UTC;
    public static final int DEFAULT_FETCH_SIZE = 1000;

    private String query = "";
    private DateTimeZone timeZone = DEFAULT_TIME_ZONE;
    private Cursor cursor = Cursor.EMPTY;
    private int fetchSize = DEFAULT_FETCH_SIZE;

    public SqlRequest() {}

    public SqlRequest(String query, DateTimeZone timeZone, Cursor nextPageInfo) {
        this.query = query;
        this.timeZone = timeZone;
        this.cursor = nextPageInfo;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if ((false == Strings.hasText(query)) && cursor == Cursor.EMPTY) {
            validationException = addValidationError("one of [query] or [cursor] is required", validationException);
        }
        return validationException;
    }

    public String query() {
        return query;
    }

    public SqlRequest query(String query) {
        if (query == null) {
            throw new IllegalArgumentException("query may not be null.");
        }
        this.query = query;
        return this;
    }

    public DateTimeZone timeZone() {
        return timeZone;
    }

    public SqlRequest timeZone(DateTimeZone timeZone) {
        if (query == null) {
            throw new IllegalArgumentException("time zone may not be null.");
        }
        this.timeZone = timeZone;
        return this;
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

    /**
     * Hint about how many results to fetch at once.
     */
    public int fetchSize() {
        return fetchSize;
    }

    /**
     * Hint about how many results to fetch at once.
     */
    public SqlRequest fetchSize(int fetchSize) {
        if (fetchSize <= 0) {
            throw new IllegalArgumentException("fetch_size must be more than 0");
        }
        this.fetchSize = fetchSize;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        query = in.readString();
        timeZone = DateTimeZone.forID(in.readString());
        cursor = in.readNamedWriteable(Cursor.class);
        fetchSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(query);
        out.writeString(timeZone.getID());
        out.writeNamedWriteable(cursor);
        out.writeVInt(fetchSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, timeZone, cursor);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SqlRequest other = (SqlRequest) obj;
        return Objects.equals(query, other.query) 
                && Objects.equals(timeZone, other.timeZone)
                && Objects.equals(cursor, other.cursor)
                && fetchSize == other.fetchSize;
    }

    @Override
    public String getDescription() {
        return "SQL [" + query + "]";
    }
}