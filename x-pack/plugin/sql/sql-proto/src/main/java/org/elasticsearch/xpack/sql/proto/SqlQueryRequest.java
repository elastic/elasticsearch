/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Sql query request for JDBC/CLI client
 */
public class SqlQueryRequest extends AbstractSqlRequest {
    @Nullable
    private final String cursor;
    private final String query;
    private final ZoneId zoneId;
    private final int fetchSize;
    private final TimeValue requestTimeout;
    private final TimeValue pageTimeout;
    @Nullable
    private final ToXContent filter;
    private final List<SqlTypedParamValue> params;


    public SqlQueryRequest(String query, List<SqlTypedParamValue> params, ZoneId zoneId, int fetchSize,
                           TimeValue requestTimeout, TimeValue pageTimeout, ToXContent filter, String cursor, RequestInfo requestInfo) {
        super(requestInfo);
        this.query = query;
        this.params = params;
        this.zoneId = zoneId;
        this.fetchSize = fetchSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
        this.filter = filter;
        this.cursor = cursor;
    }

    public SqlQueryRequest(String query, List<SqlTypedParamValue> params, ToXContent filter, ZoneId zoneId,
                           int fetchSize, TimeValue requestTimeout, TimeValue pageTimeout, RequestInfo requestInfo) {
        this(query, params, zoneId, fetchSize, requestTimeout, pageTimeout, filter, null, requestInfo);
    }

    public SqlQueryRequest(String cursor, TimeValue requestTimeout, TimeValue pageTimeout, RequestInfo requestInfo) {
        this("", Collections.emptyList(), Protocol.TIME_ZONE, Protocol.FETCH_SIZE, requestTimeout, pageTimeout,
             null, cursor, requestInfo);
    }

    /**
     * The key that must be sent back to SQL to access the next page of
     * results.
     */
    public String cursor() {
        return cursor;
    }

    /**
     * Text of SQL query
     */
    public String query() {
        return query;
    }

    /**
     * An optional list of parameters if the SQL query is parametrized
     */
    public List<SqlTypedParamValue> params() {
        return params;
    }

    /**
     * The client's time zone
     */
    public ZoneId zoneId() {
        return zoneId;
    }


    /**
     * Hint about how many results to fetch at once.
     */
    public int fetchSize() {
        return fetchSize;
    }

    /**
     * The timeout specified on the search request
     */
    public TimeValue requestTimeout() {
        return requestTimeout;
    }

    /**
     * The scroll timeout
     */
    public TimeValue pageTimeout() {
        return pageTimeout;
    }

    /**
     * An optional Query DSL defined query that can added as a filter on the top of the SQL query
     */
    public ToXContent filter() {
        return filter;
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
        SqlQueryRequest that = (SqlQueryRequest) o;
        return fetchSize == that.fetchSize &&
            Objects.equals(query, that.query) &&
            Objects.equals(params, that.params) &&
            Objects.equals(zoneId, that.zoneId) &&
            Objects.equals(requestTimeout, that.requestTimeout) &&
            Objects.equals(pageTimeout, that.pageTimeout) &&
            Objects.equals(filter, that.filter) &&
            Objects.equals(cursor, that.cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, zoneId, fetchSize, requestTimeout, pageTimeout, filter, cursor);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (query != null) {
            builder.field("query", query);
        }
        builder.field("mode", mode().toString());
        if (clientId() != null) {
            builder.field("client_id", clientId());
        }
        if (this.params.isEmpty() == false) {
            builder.startArray("params");
            for (SqlTypedParamValue val : this.params) {
                val.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (zoneId != null) {
            builder.field("time_zone", zoneId.getId());
        }
        if (fetchSize != Protocol.FETCH_SIZE) {
            builder.field("fetch_size", fetchSize);
        }
        if (requestTimeout != Protocol.REQUEST_TIMEOUT) {
            builder.field("request_timeout", requestTimeout.getStringRep());
        }
        if (pageTimeout != Protocol.PAGE_TIMEOUT) {
            builder.field("page_timeout", pageTimeout.getStringRep());
        }
        if (filter != null) {
            builder.field("filter");
            filter.toXContent(builder, params);
        }
        if (cursor != null) {
            builder.field("cursor", cursor);
        }
        return builder;
    }
}