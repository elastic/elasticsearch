/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;



import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryInitRequest;
import org.elasticsearch.xpack.sql.protocol.shared.TimeoutInfo;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class AbstractSqlRequest extends ActionRequest implements CompositeIndicesRequest {

    public static final DateTimeZone DEFAULT_TIME_ZONE = DateTimeZone.UTC;
    public static final int DEFAULT_FETCH_SIZE = AbstractQueryInitRequest.DEFAULT_FETCH_SIZE;
    public static final TimeValue DEFAULT_REQUEST_TIMEOUT = TimeValue.timeValueMillis(TimeoutInfo.DEFAULT_REQUEST_TIMEOUT);
    public static final TimeValue DEFAULT_PAGE_TIMEOUT = TimeValue.timeValueMillis(TimeoutInfo.DEFAULT_PAGE_TIMEOUT);

    private String query = "";
    private DateTimeZone timeZone = DEFAULT_TIME_ZONE;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private TimeValue requestTimeout = DEFAULT_REQUEST_TIMEOUT; 
    private TimeValue pageTimeout = DEFAULT_PAGE_TIMEOUT;
    @Nullable
    private QueryBuilder filter = null;

    public AbstractSqlRequest() {
        super();
    }

    public AbstractSqlRequest(String query, QueryBuilder filter, DateTimeZone timeZone, int fetchSize, TimeValue requestTimeout, TimeValue pageTimeout) {
        this.query = query;
        this.timeZone = timeZone;
        this.fetchSize = fetchSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
    }

    public static <R extends AbstractSqlRequest> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
        ObjectParser<R, Void> parser = new ObjectParser<R, Void>("sql/query", supplier);

        parser.declareString(AbstractSqlRequest::query, new ParseField("query"));
        parser.declareString((request, zoneId) -> request.timeZone(DateTimeZone.forID(zoneId)), new ParseField("time_zone"));
        parser.declareInt(AbstractSqlRequest::fetchSize, new ParseField("fetch_size"));
        parser.declareString(
                (request, timeout) -> request.requestTimeout(TimeValue.parseTimeValue(timeout, DEFAULT_REQUEST_TIMEOUT, "request_timeout")),
                new ParseField("request_timeout"));
        parser.declareString(
                (request, timeout) -> request.pageTimeout(TimeValue.parseTimeValue(timeout, DEFAULT_PAGE_TIMEOUT, "page_timeout")),
                new ParseField("page_timeout"));
        parser.declareObject(AbstractSqlRequest::filter,
                (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), new ParseField("filter"));

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
            throw new IllegalArgumentException("fetch_size must be more than 0.");
        }
        this.fetchSize = fetchSize;
        return this;
    }

    public TimeValue requestTimeout() {
        return requestTimeout;
    }

    public AbstractSqlRequest requestTimeout(TimeValue requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }


    public TimeValue pageTimeout() {
        return pageTimeout;
    }

    public AbstractSqlRequest pageTimeout(TimeValue pageTimeout) {
        this.pageTimeout = pageTimeout;
        return this;
    }

    /**
     * An optional Query DSL defined query that can added as a filter on the top of the SQL query
     */
    public AbstractSqlRequest filter(QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    /**
     * An optional Query DSL defined query that can added as a filter on the top of the SQL query
     */
    public QueryBuilder filter() {
        return filter;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        query = in.readString();
        timeZone = DateTimeZone.forID(in.readString());
        fetchSize = in.readVInt();
        requestTimeout = new TimeValue(in);
        pageTimeout = new TimeValue(in);
        filter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(query);
        out.writeString(timeZone.getID());
        out.writeVInt(fetchSize);
        requestTimeout.writeTo(out);
        pageTimeout.writeTo(out);
        out.writeOptionalNamedWriteable(filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, timeZone, fetchSize, requestTimeout, pageTimeout, filter);
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
                && fetchSize == other.fetchSize
                && Objects.equals(requestTimeout, other.requestTimeout)
                && Objects.equals(pageTimeout, other.pageTimeout)
                && Objects.equals(filter, other.filter);
    }
}