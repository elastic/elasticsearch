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
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/** Builds and runs ES|QL requests on behalf of the datafeed validator and extractor, with stored datafeed headers. */
final class EsqlQueryExecutor {

    private EsqlQueryExecutor() {}

    @SuppressWarnings("unchecked")
    static EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> newRequest(
        Client client,
        String query,
        @Nullable QueryBuilder filter,
        List<EsqlQueryParam> params,
        @Nullable String projectRouting
    ) {
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = (EsqlQueryRequestBuilder<
            EsqlQueryRequest,
            EsqlQueryResponse>) EsqlQueryRequestBuilder.newRequestBuilder(client).query(query).allowPartialResults(false);
        if (filter != null) {
            builder.filter(filter);
        }
        if (projectRouting != null) {
            builder.projectRouting(projectRouting);
        }
        if (params.isEmpty() == false) {
            builder.params(params);
        }
        return builder;
    }

    @SuppressWarnings("unchecked")
    static void executeAsync(
        Client client,
        EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request,
        Map<String, String> headers,
        ActionListener<EsqlQueryResponse> listener
    ) {
        ClientHelper.executeWithHeadersAsync(
            client.threadPool().getThreadContext(),
            headers,
            ML_ORIGIN,
            (EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse>) request,
            listener,
            (b, l) -> b.execute(l)
        );
    }

    /** The caller owns the returned response and must close it. */
    static EsqlQueryResponse executeSync(
        Client client,
        EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request,
        Map<String, String> headers
    ) {
        return ClientHelper.executeWithHeaders(headers, ML_ORIGIN, client, () -> request.execute().actionGet());
    }
}
