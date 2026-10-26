/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;

import java.util.List;
import java.util.Map;

/** Runs ES|QL queries for datafeed validation probes. */
interface EsqlQueryRunner {

    EsqlQueryRunner DEFAULT = EsqlQueryRunner::executeAsyncDefault;

    void executeAsync(
        Client client,
        String query,
        @Nullable QueryBuilder filter,
        Map<String, String> headers,
        @Nullable String projectRouting,
        List<EsqlQueryParam> params,
        ActionListener<EsqlQueryResponse> listener
    );

    private static void executeAsyncDefault(
        Client client,
        String query,
        @Nullable QueryBuilder filter,
        Map<String, String> headers,
        @Nullable String projectRouting,
        List<EsqlQueryParam> params,
        ActionListener<EsqlQueryResponse> listener
    ) {
        EsqlQueryExecutor.executeAsync(
            client,
            EsqlQueryExecutor.newRequest(client, query, filter, params, projectRouting),
            headers,
            listener
        );
    }
}
