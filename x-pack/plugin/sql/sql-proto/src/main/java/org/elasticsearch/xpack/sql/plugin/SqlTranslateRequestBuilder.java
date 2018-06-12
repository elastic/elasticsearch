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
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

/**
 * Builder for the request for the sql action for translating SQL queries into ES requests
 */
public class SqlTranslateRequestBuilder extends ActionRequestBuilder<SqlTranslateRequest, SqlTranslateResponse> {
    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action) {
        this(client, action, Mode.PLAIN, null, null, Collections.emptyList(), Protocol.TIME_ZONE, Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT);
    }

    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action, Mode mode, String query,
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
