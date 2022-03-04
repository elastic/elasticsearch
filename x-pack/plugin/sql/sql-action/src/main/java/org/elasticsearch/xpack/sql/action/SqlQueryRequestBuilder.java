/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * The builder to build sql request
 */
public class SqlQueryRequestBuilder extends ActionRequestBuilder<SqlQueryRequest, SqlQueryResponse> {

    public SqlQueryRequestBuilder(ElasticsearchClient client, SqlQueryAction action) {
        this(
            client,
            action,
            "",
            emptyList(),
            null,
            emptyMap(),
            Protocol.TIME_ZONE,
            null,
            Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT,
            Protocol.PAGE_TIMEOUT,
            false,
            "",
            new RequestInfo(Mode.PLAIN),
            Protocol.FIELD_MULTI_VALUE_LENIENCY,
            Protocol.INDEX_INCLUDE_FROZEN,
            Protocol.DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT,
            Protocol.DEFAULT_KEEP_ON_COMPLETION,
            Protocol.DEFAULT_KEEP_ALIVE
        );
    }

    public SqlQueryRequestBuilder(
        ElasticsearchClient client,
        SqlQueryAction action,
        String query,
        List<SqlTypedParamValue> params,
        QueryBuilder filter,
        Map<String, Object> runtimeMappings,
        ZoneId zoneId,
        String catalog,
        int fetchSize,
        TimeValue requestTimeout,
        TimeValue pageTimeout,
        boolean columnar,
        String nextPageInfo,
        RequestInfo requestInfo,
        boolean multiValueFieldLeniency,
        boolean indexIncludeFrozen,
        TimeValue waitForCompletionTimeout,
        boolean keepOnCompletion,
        TimeValue keepAlive
    ) {
        super(
            client,
            action,
            new SqlQueryRequest(
                query,
                params,
                filter,
                runtimeMappings,
                zoneId,
                catalog,
                fetchSize,
                requestTimeout,
                pageTimeout,
                columnar,
                nextPageInfo,
                requestInfo,
                multiValueFieldLeniency,
                indexIncludeFrozen,
                waitForCompletionTimeout,
                keepOnCompletion,
                keepAlive
            )
        );
    }

    public SqlQueryRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public SqlQueryRequestBuilder mode(String mode) {
        request.mode(mode);
        return this;
    }

    public SqlQueryRequestBuilder mode(Mode mode) {
        request.mode(mode);
        return this;
    }

    public SqlQueryRequestBuilder version(String version) {
        request.version(version);
        return this;
    }

    public SqlQueryRequestBuilder cursor(String cursor) {
        request.cursor(cursor);
        return this;
    }

    public SqlQueryRequestBuilder filter(QueryBuilder filter) {
        request.filter(filter);
        return this;
    }

    public SqlQueryRequestBuilder runtimeMappings(Map<String, Object> runtimeMappings) {
        request.runtimeMappings(runtimeMappings);
        return this;
    }

    public SqlQueryRequestBuilder zoneId(ZoneId zoneId) {
        request.zoneId(zoneId);
        return this;
    }

    public SqlQueryRequestBuilder catalog(String catalog) {
        request.catalog(catalog);
        return this;
    }

    public SqlQueryRequestBuilder requestTimeout(TimeValue timeout) {
        request.requestTimeout(timeout);
        return this;
    }

    public SqlQueryRequestBuilder pageTimeout(TimeValue timeout) {
        request.pageTimeout(timeout);
        return this;
    }

    public SqlQueryRequestBuilder columnar(boolean columnar) {
        request.columnar(columnar);
        return this;
    }

    public SqlQueryRequestBuilder fetchSize(int fetchSize) {
        request.fetchSize(fetchSize);
        return this;
    }

    public SqlQueryRequestBuilder multiValueFieldLeniency(boolean lenient) {
        request.fieldMultiValueLeniency(lenient);
        return this;
    }

    public SqlQueryRequestBuilder waitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        request.waitForCompletionTimeout(waitForCompletionTimeout);
        return this;
    }

    public SqlQueryRequestBuilder keepOnCompletion(boolean keepOnCompletion) {
        request.keepOnCompletion(keepOnCompletion);
        return this;
    }

    public SqlQueryRequestBuilder keepAlive(TimeValue keepAlive) {
        request.keepAlive(keepAlive);
        return this;
    }
}
