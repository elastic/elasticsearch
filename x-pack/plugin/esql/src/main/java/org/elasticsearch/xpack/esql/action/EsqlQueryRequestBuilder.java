/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.esql.action.internal.SharedSecrets;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

public class EsqlQueryRequestBuilder extends org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder<
    EsqlQueryRequest,
    EsqlQueryResponse> {

    public static EsqlQueryRequestBuilder newAsyncEsqlQueryRequestBuilder(ElasticsearchClient client) {
        return new EsqlQueryRequestBuilder(client, EsqlQueryRequest.asyncEsqlQueryRequest());
    }

    public static EsqlQueryRequestBuilder newSyncEsqlQueryRequestBuilder(ElasticsearchClient client) {
        return new EsqlQueryRequestBuilder(client, EsqlQueryRequest.syncEsqlQueryRequest());
    }

    private EsqlQueryRequestBuilder(ElasticsearchClient client, EsqlQueryRequest request) {
        super(client, EsqlQueryAction.INSTANCE, request);
    }

    @Override
    public EsqlQueryRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public EsqlQueryRequestBuilder columnar(boolean columnar) {
        request.columnar(columnar);
        return this;
    }

    @Override
    public EsqlQueryRequestBuilder filter(QueryBuilder filter) {
        request.filter(filter);
        return this;
    }

    public EsqlQueryRequestBuilder pragmas(QueryPragmas pragmas) {
        request.pragmas(pragmas);
        return this;
    }

    public EsqlQueryRequestBuilder waitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        request.waitForCompletionTimeout(waitForCompletionTimeout);
        return this;
    }

    public EsqlQueryRequestBuilder keepAlive(TimeValue keepAlive) {
        request.keepAlive(keepAlive);
        return this;
    }

    public EsqlQueryRequestBuilder keepOnCompletion(boolean keepOnCompletion) {
        request.keepOnCompletion(keepOnCompletion);
        return this;
    }

    @Override
    public EsqlQueryRequestBuilder allowPartialResults(boolean allowPartialResults) {
        request.allowPartialResults(allowPartialResults);
        return this;
    }

    static { // plumb access from x-pack core
        SharedSecrets.setEsqlQueryRequestBuilderAccess(EsqlQueryRequestBuilder::newSyncEsqlQueryRequestBuilder);
    }
}
