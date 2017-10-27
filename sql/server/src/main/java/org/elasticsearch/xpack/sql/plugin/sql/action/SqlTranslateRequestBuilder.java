/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTimeZone;

import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_FETCH_SIZE;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_PAGE_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_REQUEST_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_TIME_ZONE;

public class SqlTranslateRequestBuilder
        extends ActionRequestBuilder<SqlTranslateRequest, SqlTranslateResponse, SqlTranslateRequestBuilder> {

    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action) {
        this(client, action, null, DEFAULT_TIME_ZONE, DEFAULT_FETCH_SIZE, DEFAULT_REQUEST_TIMEOUT, DEFAULT_PAGE_TIMEOUT);
    }

    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action, String query, DateTimeZone timeZone,
                                      int fetchSize, TimeValue requestTimeout, TimeValue pageTimeout) {
        super(client, action, new SqlTranslateRequest(query, timeZone, fetchSize, requestTimeout, pageTimeout));
    }

    public SqlTranslateRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public SqlTranslateRequestBuilder timeZone(DateTimeZone timeZone) {
        request.timeZone(timeZone);
        return this;
    }
}
