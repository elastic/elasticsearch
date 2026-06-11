/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.concurrent.Executor;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

/**
 * Read-authorizes and rewrites {@code FROM <dataset>} targets. The companion of {@link DatasetRewriter} that owns the
 * security round-trip: the concrete dataset names the query would read (per-relation resolution of wildcards,
 * exclusions and date math) are first pushed through {@link EsqlResolveDatasetAction} — an
 * {@code IndicesRequest.Replaceable} with {@code resolveDatasets(true)}, so the security filter enforces a read on
 * each dataset name, the DLS/FLS interceptor rejects restricted datasets, and the dataset-datasource interceptor
 * enforces {@code global.data_source: read} on the parent datasource — and only the authorized names are then
 * rewritten into external relations. Mirrors how {@code ViewResolver} routes view names through
 * {@code EsqlResolveViewAction}.
 *
 * <p>When no FROM pattern can match a registered dataset (in particular whenever no datasets exist — the feature-flag
 * off path), the listener completes synchronously with the plan untouched and no request is sent.
 */
public class DatasetResolver {

    private final Client client;
    private final Executor executor;

    public DatasetResolver(Client client, Executor executor) {
        this.client = client;
        this.executor = executor;
    }

    /**
     * Replaces every authorized {@code FROM <dataset>} target in {@code parsed} via
     * {@link DatasetRewriter#rewrite}, completing {@code listener} with the (possibly untouched) plan.
     * Authorization failures from the resolve action — datasource denial, DLS/FLS rejection — propagate
     * to the listener as-is.
     */
    public void replaceDatasets(
        LogicalPlan parsed,
        ProjectMetadata projectMetadata,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ActionListener<LogicalPlan> listener
    ) {
        List<String> candidates = DatasetRewriter.candidateDatasets(parsed, projectMetadata, indexNameExpressionResolver);
        if (candidates.isEmpty()) {
            listener.onResponse(parsed);
            return;
        }
        var request = new EsqlResolveDatasetAction.Request(
            REST_MASTER_TIMEOUT_DEFAULT,
            candidates.toArray(String[]::new),
            DatasetRewriter.datasetToDataSourceMap(projectMetadata)
        );
        client.execute(
            EsqlResolveDatasetAction.TYPE,
            request,
            new ThreadedActionListener<>(
                executor,
                listener.map(response -> DatasetRewriter.rewrite(parsed, projectMetadata, indexNameExpressionResolver, response.datasets()))
            )
        );
    }
}
