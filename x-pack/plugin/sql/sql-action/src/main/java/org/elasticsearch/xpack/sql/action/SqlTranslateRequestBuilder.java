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
 * Builder for the request for the sql action for translating SQL queries into ES requests
 */
public class SqlTranslateRequestBuilder extends ActionRequestBuilder<SqlTranslateRequest, SqlTranslateResponse> {
    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action) {
        this(client, action, null, null, Collections.emptyList(), Protocol.TIME_ZONE, Protocol.FETCH_SIZE, Protocol.REQUEST_TIMEOUT,
            Protocol.PAGE_TIMEOUT, new RequestInfo(Mode.PLAIN));
    }

    public SqlTranslateRequestBuilder(ElasticsearchClient client, SqlTranslateAction action, String query, QueryBuilder filter,
            List<SqlTypedParamValue> params, ZoneId zoneId, int fetchSize, TimeValue requestTimeout,
                                      TimeValue pageTimeout, RequestInfo requestInfo) {
        super(client, action,
                new SqlTranslateRequest(query, params, filter, zoneId, fetchSize, requestTimeout, pageTimeout, requestInfo));
    }

    public SqlTranslateRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public SqlTranslateRequestBuilder zoneId(ZoneId zoneId) {
        request.zoneId(zoneId);
        return this;
    }
}
