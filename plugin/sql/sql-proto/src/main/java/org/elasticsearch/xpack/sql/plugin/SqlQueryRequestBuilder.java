/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.plugin.AbstractSqlRequest.Mode;

import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.plugin.AbstractSqlQueryRequest.DEFAULT_FETCH_SIZE;
import static org.elasticsearch.xpack.sql.plugin.AbstractSqlQueryRequest.DEFAULT_PAGE_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.AbstractSqlQueryRequest.DEFAULT_REQUEST_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.AbstractSqlQueryRequest.DEFAULT_TIME_ZONE;

/**
 * The builder to build sql request
 */
public class SqlQueryRequestBuilder extends ActionRequestBuilder<SqlQueryRequest, SqlQueryResponse, SqlQueryRequestBuilder> {

    public SqlQueryRequestBuilder(ElasticsearchClient client, SqlQueryAction action) {
        this(client, action, "", Collections.emptyList(), null, DEFAULT_TIME_ZONE, DEFAULT_FETCH_SIZE, DEFAULT_REQUEST_TIMEOUT,
                DEFAULT_PAGE_TIMEOUT, "", Mode.PLAIN);
    }

    public SqlQueryRequestBuilder(ElasticsearchClient client, SqlQueryAction action, String query, List<SqlTypedParamValue> params,
                                  QueryBuilder filter, TimeZone timeZone, int fetchSize, TimeValue requestTimeout,
                                  TimeValue pageTimeout, String nextPageInfo, Mode mode) {
        super(client, action, new SqlQueryRequest(mode, query, params, filter, timeZone, fetchSize, requestTimeout, pageTimeout,
                nextPageInfo));
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

    public SqlQueryRequestBuilder timeZone(TimeZone timeZone) {
        request.timeZone(timeZone);
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

    public SqlQueryRequestBuilder fetchSize(int fetchSize) {
        request.fetchSize(fetchSize);
        return this;
    }
}
