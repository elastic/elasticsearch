/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.joda.time.DateTimeZone;

import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_FETCH_SIZE;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_PAGE_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_REQUEST_TIMEOUT;
import static org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest.DEFAULT_TIME_ZONE;

public class SqlRequestBuilder extends ActionRequestBuilder<SqlRequest, SqlResponse, SqlRequestBuilder> {

    public SqlRequestBuilder(ElasticsearchClient client, SqlAction action) {
        this(client, action, "", null, DEFAULT_TIME_ZONE, DEFAULT_FETCH_SIZE, DEFAULT_REQUEST_TIMEOUT, DEFAULT_PAGE_TIMEOUT, Cursor.EMPTY);
    }

    public SqlRequestBuilder(ElasticsearchClient client, SqlAction action, String query, QueryBuilder filter, DateTimeZone timeZone,
                             int fetchSize, TimeValue requestTimeout, TimeValue pageTimeout, Cursor nextPageInfo) {
        super(client, action, new SqlRequest(query, filter, timeZone, fetchSize, requestTimeout, pageTimeout, nextPageInfo));
    }

    public SqlRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public SqlRequestBuilder filter(QueryBuilder filter) {
        request.filter(filter);
        return this;
    }

    public SqlRequestBuilder nextPageKey(Cursor nextPageInfo) {
        request.cursor(nextPageInfo);
        return this;
    }

    public SqlRequestBuilder timeZone(DateTimeZone timeZone) {
        request.timeZone(timeZone);
        return this;
    }

    public SqlRequestBuilder requestTimeout(TimeValue timeout) {
        request.requestTimeout(timeout);
        return this;
    }

    public SqlRequestBuilder pageTimeout(TimeValue timeout) {
        request.pageTimeout(timeout);
        return this;
    }
}
