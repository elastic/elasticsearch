/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Base class for requests that contain sql queries (Query and Translate)
 */
public abstract class AbstractSqlQueryRequest extends AbstractSqlRequest implements CompositeIndicesRequest, ToXContentFragment {
    public static final DateTimeZone DEFAULT_TIME_ZONE = DateTimeZone.UTC;

    /**
     * Global choice for the default fetch size.
     */
    public static final int DEFAULT_FETCH_SIZE = 1000;
    public static final TimeValue DEFAULT_REQUEST_TIMEOUT = TimeValue.timeValueSeconds(90);
    public static final TimeValue DEFAULT_PAGE_TIMEOUT = TimeValue.timeValueSeconds(45);

    private String query = "";
    private DateTimeZone timeZone = DEFAULT_TIME_ZONE;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private TimeValue requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private TimeValue pageTimeout = DEFAULT_PAGE_TIMEOUT;
    @Nullable
    private QueryBuilder filter = null;

    public AbstractSqlQueryRequest() {
        super();
    }

    public AbstractSqlQueryRequest(Mode mode, String query, QueryBuilder filter, DateTimeZone timeZone, int fetchSize,
                                   TimeValue requestTimeout, TimeValue pageTimeout) {
        super(mode);
        this.query = query;
        this.timeZone = timeZone;
        this.fetchSize = fetchSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
        this.filter = filter;
    }

    protected static <R extends AbstractSqlQueryRequest> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
        // TODO: convert this into ConstructingObjectParser
        ObjectParser<R, Void> parser = new ObjectParser<>("sql/query", true, supplier);
        parser.declareString(AbstractSqlQueryRequest::query, new ParseField("query"));
        parser.declareString((request, zoneId) -> request.timeZone(DateTimeZone.forID(zoneId)), new ParseField("time_zone"));
        parser.declareInt(AbstractSqlQueryRequest::fetchSize, new ParseField("fetch_size"));
        parser.declareString(
                (request, timeout) -> request.requestTimeout(TimeValue.parseTimeValue(timeout, DEFAULT_REQUEST_TIMEOUT, "request_timeout")),
                new ParseField("request_timeout"));
        parser.declareString(
                (request, timeout) -> request.pageTimeout(TimeValue.parseTimeValue(timeout, DEFAULT_PAGE_TIMEOUT, "page_timeout")),
                new ParseField("page_timeout"));
        parser.declareObject(AbstractSqlQueryRequest::filter,
                (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), new ParseField("filter"));

        return parser;
    }

    public String query() {
        return query;
    }

    public AbstractSqlQueryRequest query(String query) {
        if (query == null) {
            throw new IllegalArgumentException("query may not be null.");
        }
        this.query = query;
        return this;
    }

    public DateTimeZone timeZone() {
        return timeZone;
    }

    public AbstractSqlQueryRequest timeZone(DateTimeZone timeZone) {
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
    public AbstractSqlQueryRequest fetchSize(int fetchSize) {
        if (fetchSize <= 0) {
            throw new IllegalArgumentException("fetch_size must be more than 0.");
        }
        this.fetchSize = fetchSize;
        return this;
    }

    public TimeValue requestTimeout() {
        return requestTimeout;
    }

    public AbstractSqlQueryRequest requestTimeout(TimeValue requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }


    public TimeValue pageTimeout() {
        return pageTimeout;
    }

    public AbstractSqlQueryRequest pageTimeout(TimeValue pageTimeout) {
        this.pageTimeout = pageTimeout;
        return this;
    }

    /**
     * An optional Query DSL defined query that can added as a filter on the top of the SQL query
     */
    public AbstractSqlQueryRequest filter(QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    /**
     * An optional Query DSL defined query that can added as a filter on the top of the SQL query
     */
    public QueryBuilder filter() {
        return filter;
    }

    public AbstractSqlQueryRequest(StreamInput in) throws IOException {
        super(in);
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AbstractSqlQueryRequest that = (AbstractSqlQueryRequest) o;
        return fetchSize == that.fetchSize &&
                Objects.equals(query, that.query) &&
                Objects.equals(timeZone, that.timeZone) &&
                Objects.equals(requestTimeout, that.requestTimeout) &&
                Objects.equals(pageTimeout, that.pageTimeout) &&
                Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, timeZone, fetchSize, requestTimeout, pageTimeout, filter);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (query != null) {
            builder.field("query", query);
        }
        if (timeZone != null) {
            builder.field("time_zone", timeZone.getID());
        }
        if (fetchSize != DEFAULT_FETCH_SIZE) {
            builder.field("fetch_size", fetchSize);
        }
        if (requestTimeout != DEFAULT_REQUEST_TIMEOUT) {
            builder.field("request_timeout", requestTimeout.getStringRep());
        }
        if (pageTimeout != DEFAULT_PAGE_TIMEOUT) {
            builder.field("page_timeout", pageTimeout.getStringRep());
        }
        if (filter != null) {
            builder.field("filter");
            filter.toXContent(builder, params);
        }
        return builder;
    }
}