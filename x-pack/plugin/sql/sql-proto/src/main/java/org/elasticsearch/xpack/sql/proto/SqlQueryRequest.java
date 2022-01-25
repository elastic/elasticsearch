/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.xpack.sql.proto.core.Nullable;
import org.elasticsearch.xpack.sql.proto.core.TimeValue;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * Sql query request for JDBC/CLI client
 */
public class SqlQueryRequest extends AbstractSqlRequest {
    @Nullable
    private final String cursor;
    private final String query;
    private final ZoneId zoneId;
    private final String catalog;
    private final int fetchSize;
    private final TimeValue requestTimeout;
    private final TimeValue pageTimeout;
    private final Boolean columnar;
    private final List<SqlTypedParamValue> params;
    private final boolean fieldMultiValueLeniency;
    private final boolean indexIncludeFrozen;
    private final Boolean binaryCommunication;
    // Async settings
    private final TimeValue waitForCompletionTimeout;
    private final boolean keepOnCompletion;
    private final TimeValue keepAlive;

    public SqlQueryRequest(
        String query,
        List<SqlTypedParamValue> params,
        ZoneId zoneId,
        String catalog,
        int fetchSize,
        TimeValue requestTimeout,
        TimeValue pageTimeout,
        Boolean columnar,
        String cursor,
        RequestInfo requestInfo,
        boolean fieldMultiValueLeniency,
        boolean indexIncludeFrozen,
        Boolean binaryCommunication,
        TimeValue waitForCompletionTimeout,
        boolean keepOnCompletion,
        TimeValue keepAlive
    ) {
        super(requestInfo);
        this.query = query;
        this.params = params;
        this.zoneId = zoneId;
        this.catalog = catalog;
        this.fetchSize = fetchSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
        this.columnar = columnar;
        this.cursor = cursor;
        this.fieldMultiValueLeniency = fieldMultiValueLeniency;
        this.indexIncludeFrozen = indexIncludeFrozen;
        this.binaryCommunication = binaryCommunication;
        this.waitForCompletionTimeout = waitForCompletionTimeout;
        this.keepOnCompletion = keepOnCompletion;
        this.keepAlive = keepAlive;
    }

    public SqlQueryRequest(
        String query,
        List<SqlTypedParamValue> params,
        ZoneId zoneId,
        String catalog,
        int fetchSize,
        TimeValue requestTimeout,
        TimeValue pageTimeout,
        Boolean columnar,
        String cursor,
        RequestInfo requestInfo,
        boolean fieldMultiValueLeniency,
        boolean indexIncludeFrozen,
        Boolean binaryCommunication
    ) {
        this(
            query,
            params,
            zoneId,
            catalog,
            fetchSize,
            requestTimeout,
            pageTimeout,
            columnar,
            cursor,
            requestInfo,
            fieldMultiValueLeniency,
            indexIncludeFrozen,
            binaryCommunication,
            CoreProtocol.DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT,
            CoreProtocol.DEFAULT_KEEP_ON_COMPLETION,
            CoreProtocol.DEFAULT_KEEP_ALIVE
        );
    }

    public SqlQueryRequest(
        String cursor,
        TimeValue requestTimeout,
        TimeValue pageTimeout,
        RequestInfo requestInfo,
        boolean binaryCommunication
    ) {
        this(
            "",
            emptyList(),
            CoreProtocol.TIME_ZONE,
            null,
            CoreProtocol.FETCH_SIZE,
            requestTimeout,
            pageTimeout,
            false,
            cursor,
            requestInfo,
            CoreProtocol.FIELD_MULTI_VALUE_LENIENCY,
            CoreProtocol.INDEX_INCLUDE_FROZEN,
            binaryCommunication
        );
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

    public String catalog() {
        return catalog;
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
            && Objects.equals(catalog, that.catalog)
            && Objects.equals(requestTimeout, that.requestTimeout)
            && Objects.equals(pageTimeout, that.pageTimeout)
            && Objects.equals(columnar, that.columnar)
            && Objects.equals(cursor, that.cursor)
            && fieldMultiValueLeniency == that.fieldMultiValueLeniency
            && indexIncludeFrozen == that.indexIncludeFrozen
            && Objects.equals(binaryCommunication, that.binaryCommunication)
            && Objects.equals(waitForCompletionTimeout, that.waitForCompletionTimeout)
            && keepOnCompletion == that.keepOnCompletion
            && Objects.equals(keepAlive, that.keepAlive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            query,
            zoneId,
            catalog,
            fetchSize,
            requestTimeout,
            pageTimeout,
            columnar,
            cursor,
            fieldMultiValueLeniency,
            indexIncludeFrozen,
            binaryCommunication,
            waitForCompletionTimeout,
            keepOnCompletion,
            keepAlive
        );
    }
}
