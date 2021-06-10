/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.sql.proto.Protocol.BINARY_FORMAT_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.CLIENT_ID_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.COLUMNAR_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.CURSOR_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.FETCH_SIZE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.FIELD_MULTI_VALUE_LENIENCY_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.FILTER_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.INDEX_INCLUDE_FROZEN_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.KEEP_ALIVE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.KEEP_ON_COMPLETION_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.MODE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.PAGE_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.PARAMS_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.QUERY_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.REQUEST_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.RUNTIME_MAPPINGS_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.TIME_ZONE_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.VERSION_NAME;
import static org.elasticsearch.xpack.sql.proto.Protocol.WAIT_FOR_COMPLETION_TIMEOUT_NAME;

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
    @Nullable
    private final Map<String, Object> runtimeMappings;
    // Async settings
    private final TimeValue waitForCompletionTimeout;
    private final boolean keepOnCompletion;
    private final TimeValue keepAlive;

    public SqlQueryRequest(String query, List<SqlTypedParamValue> params, ZoneId zoneId, int fetchSize,
                           TimeValue requestTimeout, TimeValue pageTimeout, ToXContent filter, Boolean columnar,
                           String cursor, RequestInfo requestInfo, boolean fieldMultiValueLeniency, boolean indexIncludeFrozen,
                           Boolean binaryCommunication, Map<String, Object> runtimeMappings, TimeValue waitForCompletionTimeout,
                           boolean keepOnCompletion, TimeValue keepAlive) {
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
        this.runtimeMappings = runtimeMappings;
        this.waitForCompletionTimeout = waitForCompletionTimeout;
        this.keepOnCompletion = keepOnCompletion;
        this.keepAlive = keepAlive;
    }

    public SqlQueryRequest(String query, List<SqlTypedParamValue> params, ZoneId zoneId, int fetchSize,
                           TimeValue requestTimeout, TimeValue pageTimeout, ToXContent filter, Boolean columnar,
                           String cursor, RequestInfo requestInfo, boolean fieldMultiValueLeniency, boolean indexIncludeFrozen,
                           Boolean binaryCommunication, Map<String, Object> runtimeMappings) {
        this(query, params, zoneId, fetchSize, requestTimeout, pageTimeout, filter, columnar, cursor, requestInfo, fieldMultiValueLeniency,
            indexIncludeFrozen, binaryCommunication, runtimeMappings, Protocol.DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT,
            Protocol.DEFAULT_KEEP_ON_COMPLETION, Protocol.DEFAULT_KEEP_ALIVE);
    }
    public SqlQueryRequest(String cursor, TimeValue requestTimeout, TimeValue pageTimeout, RequestInfo requestInfo,
                           boolean binaryCommunication) {
        this("", emptyList(), Protocol.TIME_ZONE, Protocol.FETCH_SIZE, requestTimeout, pageTimeout, null, false,
                cursor, requestInfo, Protocol.FIELD_MULTI_VALUE_LENIENCY, Protocol.INDEX_INCLUDE_FROZEN, binaryCommunication, emptyMap());
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

    public Map<String, Object> runtimeMappings() {
        return runtimeMappings;
    }

    public TimeValue waitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    public boolean keepOnCompletion() {
        return keepOnCompletion;
    }

    public TimeValue keepAlive() {
        return keepAlive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
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
                && Objects.equals(binaryCommunication,  that.binaryCommunication)
                && Objects.equals(runtimeMappings, that.runtimeMappings)
                && Objects.equals(waitForCompletionTimeout, that.waitForCompletionTimeout)
                && keepOnCompletion == that.keepOnCompletion
                && Objects.equals(keepAlive, that.keepAlive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, zoneId, fetchSize, requestTimeout, pageTimeout,
                filter, columnar, cursor, fieldMultiValueLeniency, indexIncludeFrozen, binaryCommunication, runtimeMappings,
                waitForCompletionTimeout, keepOnCompletion, keepAlive);
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
        if (runtimeMappings.isEmpty() == false) {
            builder.field(RUNTIME_MAPPINGS_NAME, runtimeMappings);
        }
        if (waitForCompletionTimeout != null) {
            builder.field(WAIT_FOR_COMPLETION_TIMEOUT_NAME, waitForCompletionTimeout.getStringRep());
        }
        if (keepOnCompletion) {
            builder.field(KEEP_ON_COMPLETION_NAME, keepOnCompletion);
        }
        if (keepAlive != null) {
            builder.field(KEEP_ALIVE_NAME, keepAlive.getStringRep());
        }
        return builder;
    }
}
