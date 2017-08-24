/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;



import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryInitRequest;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class AbstractSqlRequest extends ActionRequest implements CompositeIndicesRequest {

    public static final DateTimeZone DEFAULT_TIME_ZONE = DateTimeZone.UTC;
    public static final int DEFAULT_FETCH_SIZE = AbstractQueryInitRequest.DEFAULT_FETCH_SIZE;
    private String query = "";
    private DateTimeZone timeZone = DEFAULT_TIME_ZONE;
    private int fetchSize = DEFAULT_FETCH_SIZE;

    public AbstractSqlRequest() {
        super();
    }

    public AbstractSqlRequest(String query, DateTimeZone timeZone, int fetchSize) {
        this.query = query;
        this.timeZone = timeZone;
        this.fetchSize = fetchSize;
    }

    public static <R extends AbstractSqlRequest> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
        ObjectParser<R, Void> parser = new ObjectParser<R, Void>("sql/query", supplier);

        parser.declareString(AbstractSqlRequest::query, new ParseField("query"));
        parser.declareString((request, zoneId) -> request.timeZone(DateTimeZone.forID(zoneId)), new ParseField("time_zone"));
        parser.declareInt(AbstractSqlRequest::fetchSize, new ParseField("fetch_size"));

        return parser;
    }

    public String query() {
        return query;
    }

    public AbstractSqlRequest query(String query) {
        if (query == null) {
            throw new IllegalArgumentException("query may not be null.");
        }
        this.query = query;
        return this;
    }

    public DateTimeZone timeZone() {
        return timeZone;
    }

    public AbstractSqlRequest timeZone(DateTimeZone timeZone) {
        if (query == null) {
            throw new IllegalArgumentException("time zone may not be null.");
        }
        this.timeZone = timeZone;
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
    public AbstractSqlRequest fetchSize(int fetchSize) {
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
        fetchSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(query);
        out.writeString(timeZone.getID());
        out.writeVInt(fetchSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, timeZone, fetchSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
    
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
    
        AbstractSqlRequest other = (AbstractSqlRequest) obj;
        return Objects.equals(query, other.query) 
                && Objects.equals(timeZone, other.timeZone)
                && fetchSize == other.fetchSize;
    }
}