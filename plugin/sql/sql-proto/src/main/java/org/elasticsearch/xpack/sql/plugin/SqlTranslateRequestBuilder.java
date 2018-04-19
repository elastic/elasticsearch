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

import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static org.elasticsearch.xpack.sql.plugin.AbstractSqlQueryRequest.DEFAULT_FETCH_SIZE;
import static org.elasticsearch.xpack.sql.plugin.AbstractSqlQueryRequest.DEFAULT_PAGE_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.AbstractSqlQueryRequest.DEFAULT_REQUEST_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.AbstractSqlQueryRequest.DEFAULT_TIME_ZONE;

/**
 * Builder for the request for the sql action for translating SQL queries into ES requests
 */
public class SqlTranslateRequestBuilder extends ActionRequestBuilder<SqlTranslateRequest, SqlTranslateResponse,
        SqlTranslateRequestBuilder> {
    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action) {
        this(client, action, AbstractSqlRequest.Mode.PLAIN, null, null, Collections.emptyList(), DEFAULT_TIME_ZONE, DEFAULT_FETCH_SIZE,
                DEFAULT_REQUEST_TIMEOUT, DEFAULT_PAGE_TIMEOUT);
    }

    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action, AbstractSqlRequest.Mode mode, String query,
                                      QueryBuilder filter, List<SqlTypedParamValue> params, TimeZone timeZone, int fetchSize,
                                      TimeValue requestTimeout, TimeValue pageTimeout) {
        super(client, action, new SqlTranslateRequest(mode, query, params, filter, timeZone, fetchSize, requestTimeout, pageTimeout));
    }

    public SqlTranslateRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public SqlTranslateRequestBuilder timeZone(TimeZone timeZone) {
        request.timeZone(timeZone);
        return this;
    }
}
