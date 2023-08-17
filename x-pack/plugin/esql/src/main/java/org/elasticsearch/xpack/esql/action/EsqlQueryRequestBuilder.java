/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.time.ZoneId;

public class EsqlQueryRequestBuilder extends ActionRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> {

    public EsqlQueryRequestBuilder(ElasticsearchClient client, EsqlQueryAction action, EsqlQueryRequest request) {
        super(client, action, request);
    }

    public EsqlQueryRequestBuilder(ElasticsearchClient client, EsqlQueryAction action) {
        this(client, action, new EsqlQueryRequest());
    }

    public EsqlQueryRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public EsqlQueryRequestBuilder columnar(boolean columnar) {
        request.columnar(columnar);
        return this;
    }

    public EsqlQueryRequestBuilder timeZone(ZoneId zoneId) {
        request.zoneId(zoneId);
        return this;
    }

    public EsqlQueryRequestBuilder filter(QueryBuilder filter) {
        request.filter(filter);
        return this;
    }

    public EsqlQueryRequestBuilder pragmas(QueryPragmas pragmas) {
        request.pragmas(pragmas);
        return this;
    }
}
