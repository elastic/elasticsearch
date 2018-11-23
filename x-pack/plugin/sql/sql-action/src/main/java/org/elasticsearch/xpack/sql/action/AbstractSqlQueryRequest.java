/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * Base class for requests that contain sql queries (Query and Translate)
 */
public abstract class AbstractSqlQueryRequest extends AbstractSqlRequest implements CompositeIndicesRequest, ToXContentFragment {

    private String query = "";
    private TimeZone timeZone = Protocol.TIME_ZONE;
    private int fetchSize = Protocol.FETCH_SIZE;
    private TimeValue requestTimeout = Protocol.REQUEST_TIMEOUT;
    private TimeValue pageTimeout = Protocol.PAGE_TIMEOUT;
    @Nullable
    private QueryBuilder filter = null;
    private List<SqlTypedParamValue> params = Collections.emptyList();

    public AbstractSqlQueryRequest() {
        super();
    }

    public AbstractSqlQueryRequest(String query, List<SqlTypedParamValue> params, QueryBuilder filter, TimeZone timeZone,
                                   int fetchSize, TimeValue requestTimeout, TimeValue pageTimeout, RequestInfo requestInfo) {
        super(requestInfo);
        this.query = query;
        this.params = params;
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
        parser.declareObjectArray(AbstractSqlQueryRequest::params, (p, c) -> SqlTypedParamValue.fromXContent(p), new ParseField("params"));
        parser.declareString((request, zoneId) -> request.timeZone(TimeZone.getTimeZone(zoneId)), new ParseField("time_zone"));
        parser.declareInt(AbstractSqlQueryRequest::fetchSize, new ParseField("fetch_size"));
        parser.declareString((request, timeout) -> request.requestTimeout(TimeValue.parseTimeValue(timeout, Protocol.REQUEST_TIMEOUT,
            "request_timeout")), new ParseField("request_timeout"));
        parser.declareString(
                (request, timeout) -> request.pageTimeout(TimeValue.parseTimeValue(timeout, Protocol.PAGE_TIMEOUT, "page_timeout")),
                new ParseField("page_timeout"));
        parser.declareObject(AbstractSqlQueryRequest::filter,
                (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), new ParseField("filter"));
        return parser;
    }

    /**
     * Text of SQL query
     */
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

    /**
     * An optional list of parameters if the SQL query is parametrized
     */
    public List<SqlTypedParamValue> params() {
        return params;
    }

    public AbstractSqlQueryRequest params(List<SqlTypedParamValue> params) {
        if (params == null) {
            throw new IllegalArgumentException("params may not be null.");
        }
        this.params = params;
        return this;
    }

    /**
     * The client's time zone
     */
    public TimeZone timeZone() {
        return timeZone;
    }

    public AbstractSqlQueryRequest timeZone(TimeZone timeZone) {
        if (timeZone == null) {
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

    /**
     * The timeout specified on the search request
     */
    public TimeValue requestTimeout() {
        return requestTimeout;
    }

    public AbstractSqlQueryRequest requestTimeout(TimeValue requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    /**
     * The scroll timeout
     */
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
        params = in.readList(AbstractSqlQueryRequest::readSqlTypedParamValue);
        timeZone = TimeZone.getTimeZone(in.readString());
        fetchSize = in.readVInt();
        requestTimeout = in.readTimeValue();
        pageTimeout = in.readTimeValue();
        filter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    public static void writeSqlTypedParamValue(StreamOutput out, SqlTypedParamValue value) throws IOException {
        out.writeString(value.type);
        out.writeGenericValue(value.value);
    }

    public static SqlTypedParamValue readSqlTypedParamValue(StreamInput in) throws IOException {
        return new SqlTypedParamValue(in.readString(), in.readGenericValue());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(query);
        out.writeVInt(params.size());
        for (SqlTypedParamValue param: params) {
            writeSqlTypedParamValue(out, param);
        }
        out.writeString(timeZone.getID());
        out.writeVInt(fetchSize);
        out.writeTimeValue(requestTimeout);
        out.writeTimeValue(pageTimeout);
        out.writeOptionalNamedWriteable(filter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        AbstractSqlQueryRequest that = (AbstractSqlQueryRequest) o;
        return fetchSize == that.fetchSize &&
                Objects.equals(query, that.query) &&
                Objects.equals(params, that.params) &&
                Objects.equals(timeZone, that.timeZone) &&
                Objects.equals(requestTimeout, that.requestTimeout) &&
                Objects.equals(pageTimeout, that.pageTimeout) &&
                Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, timeZone, fetchSize, requestTimeout, pageTimeout, filter);
    }
}
