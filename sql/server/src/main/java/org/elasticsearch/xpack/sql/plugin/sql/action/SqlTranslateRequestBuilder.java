/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.joda.time.DateTimeZone;

import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_FETCH_SIZE;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_TIME_ZONE;

public class SqlTranslateRequestBuilder
        extends ActionRequestBuilder<SqlTranslateRequest, SqlTranslateResponse, SqlTranslateRequestBuilder> {

    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action) {
        this(client, action, null, DEFAULT_TIME_ZONE, DEFAULT_FETCH_SIZE);
    }

    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action, String query, DateTimeZone timeZone, int fetchSize) {
        super(client, action, new SqlTranslateRequest(query, timeZone, fetchSize));
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
