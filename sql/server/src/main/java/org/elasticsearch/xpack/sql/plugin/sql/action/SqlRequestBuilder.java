/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.joda.time.DateTimeZone;

import static org.elasticsearch.xpack.sql.plugin.sql.action.SqlRequest.DEFAULT_TIME_ZONE;

public class SqlRequestBuilder extends ActionRequestBuilder<SqlRequest, SqlResponse, SqlRequestBuilder> {

    public SqlRequestBuilder(ElasticsearchClient client, SqlAction action) {
        this(client, action, "", DEFAULT_TIME_ZONE, Cursor.EMPTY);
    }

    public SqlRequestBuilder(ElasticsearchClient client, SqlAction action, String query, DateTimeZone timeZone, Cursor nextPageInfo) {
        super(client, action, new SqlRequest(query, timeZone, nextPageInfo));
    }

    public SqlRequestBuilder query(String query) {
        request.query(query);
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

}
