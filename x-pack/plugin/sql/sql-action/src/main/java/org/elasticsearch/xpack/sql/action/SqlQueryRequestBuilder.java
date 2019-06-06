/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

/**
 * The builder to build sql request
 */
public class SqlQueryRequestBuilder extends ActionRequestBuilder<SqlQueryRequest, SqlQueryResponse> {

    public SqlQueryRequestBuilder(ElasticsearchClient client, SqlQueryAction action) {
        this(client, action, "", Collections.emptyList(), null, Protocol.TIME_ZONE, Protocol.FETCH_SIZE, Protocol.REQUEST_TIMEOUT,
            Protocol.PAGE_TIMEOUT, false, "", new RequestInfo(Mode.PLAIN), Protocol.FIELD_MULTI_VALUE_LENIENCY, 
            Protocol.INDEX_INCLUDE_FROZEN);
    }

    public SqlQueryRequestBuilder(ElasticsearchClient client, SqlQueryAction action, String query, List<SqlTypedParamValue> params,
            QueryBuilder filter, ZoneId zoneId, int fetchSize, TimeValue requestTimeout,
            TimeValue pageTimeout, boolean columnar, String nextPageInfo, RequestInfo requestInfo,
            boolean multiValueFieldLeniency, boolean indexIncludeFrozen) {
        super(client, action, new SqlQueryRequest(query, params, filter, zoneId, fetchSize, requestTimeout, pageTimeout, columnar,
                nextPageInfo, requestInfo, multiValueFieldLeniency, indexIncludeFrozen));
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

    public SqlQueryRequestBuilder cursor(String cursor) {
        request.cursor(cursor);
        return this;
    }

    public SqlQueryRequestBuilder filter(QueryBuilder filter) {
        request.filter(filter);
        return this;
    }

    public SqlQueryRequestBuilder zoneId(ZoneId zoneId) {
        request.zoneId(zoneId);
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
}