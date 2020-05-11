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

import static org.elasticsearch.xpack.sql.proto.Protocol.BINARY_FORMAT_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.CLIENT_ID_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.COLUMNAR_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.CURSOR_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.FETCH_SIZE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.FIELD_MULTI_VALUE_LENIENCY_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.FILTER_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.INDEX_INCLUDE_FROZEN_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.MODE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.PAGE_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.PARAMS_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.QUERY_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.REQUEST_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.TIME_ZONE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.VERSION_NAME;

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
    private final Boolean columnar;
    private final List<SqlTypedParamValue> params;
    private final boolean fieldMultiValueLeniency;
    private final boolean indexIncludeFrozen;
    private final Boolean binaryCommunication;

    public SqlQueryRequest(String query, List<SqlTypedParamValue> params, ZoneId zoneId, int fetchSize,
                           TimeValue requestTimeout, TimeValue pageTimeout, ToXContent filter, Boolean columnar,
                           String cursor, RequestInfo requestInfo, boolean fieldMultiValueLeniency, boolean indexIncludeFrozen,
                           Boolean binaryCommunication) {
        super(requestInfo);
        this.query = query;
        this.params = params;
        this.zoneId = zoneId;
        this.fetchSize = fetchSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
        this.filter = filter;
        this.columnar = columnar;
        this.cursor = cursor;
        this.fieldMultiValueLeniency = fieldMultiValueLeniency;
        this.indexIncludeFrozen = indexIncludeFrozen;
        this.binaryCommunication = binaryCommunication;
    }

    public SqlQueryRequest(String cursor, TimeValue requestTimeout, TimeValue pageTimeout, RequestInfo requestInfo,
                           boolean binaryCommunication) {
        this("", Collections.emptyList(), Protocol.TIME_ZONE, Protocol.FETCH_SIZE, requestTimeout, pageTimeout,
                null, false, cursor, requestInfo, Protocol.FIELD_MULTI_VALUE_LENIENCY, Protocol.INDEX_INCLUDE_FROZEN, binaryCommunication);
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
    
    /**
     * Optional setting for returning the result values in a columnar fashion (as opposed to rows of values).
     * Each column will have all its values in a list. Defaults to false.
     */
    public Boolean columnar() {
        return columnar;
    }

    public boolean fieldMultiValueLeniency() {
        return fieldMultiValueLeniency;
    }
    
    public boolean indexIncludeFrozen() {
        return indexIncludeFrozen;
    }
    
    public Boolean binaryCommunication() {
        return binaryCommunication;
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
        return fetchSize == that.fetchSize
                && Objects.equals(query, that.query)
                && Objects.equals(params, that.params)
                && Objects.equals(zoneId, that.zoneId)
                && Objects.equals(requestTimeout, that.requestTimeout)
                && Objects.equals(pageTimeout, that.pageTimeout)
                && Objects.equals(filter, that.filter)
                && Objects.equals(columnar,  that.columnar)
                && Objects.equals(cursor, that.cursor)
                && fieldMultiValueLeniency == that.fieldMultiValueLeniency
                && indexIncludeFrozen == that.indexIncludeFrozen
                && Objects.equals(binaryCommunication,  that.binaryCommunication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, zoneId, fetchSize, requestTimeout, pageTimeout,
                filter, columnar, cursor, fieldMultiValueLeniency, indexIncludeFrozen, binaryCommunication);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (query != null) {
            builder.field(QUERY_NAME, query);
        }
        builder.field(MODE_NAME, mode().toString());
        if (clientId() != null) {
            builder.field(CLIENT_ID_NAME, clientId());
        }
        if (version() != null) {
            builder.field(VERSION_NAME, version().toString());
        }
        if (this.params != null && this.params.isEmpty() == false) {
            builder.startArray(PARAMS_NAME);
            for (SqlTypedParamValue val : this.params) {
                val.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (zoneId != null) {
            builder.field(TIME_ZONE_NAME, zoneId.getId());
        }
        if (fetchSize != Protocol.FETCH_SIZE) {
            builder.field(FETCH_SIZE_NAME, fetchSize);
        }
        if (requestTimeout != Protocol.REQUEST_TIMEOUT) {
            builder.field(REQUEST_TIMEOUT_NAME, requestTimeout.getStringRep());
        }
        if (pageTimeout != Protocol.PAGE_TIMEOUT) {
            builder.field(PAGE_TIMEOUT_NAME, pageTimeout.getStringRep());
        }
        if (filter != null) {
            builder.field(FILTER_NAME);
            filter.toXContent(builder, params);
        }
        if (columnar != null) {
            builder.field(COLUMNAR_NAME, columnar);
        }
        if (fieldMultiValueLeniency) {
            builder.field(FIELD_MULTI_VALUE_LENIENCY_NAME, fieldMultiValueLeniency);
        }
        if (indexIncludeFrozen) {
            builder.field(INDEX_INCLUDE_FROZEN_NAME, indexIncludeFrozen);
        }
        if (binaryCommunication != null) {
            builder.field(BINARY_FORMAT_NAME, binaryCommunication);
        }
        if (cursor != null) {
            builder.field(CURSOR_NAME, cursor);
        }
        return builder;
    }
}