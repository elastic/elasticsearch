/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlSession.PreAnalysisResult;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.List;
import java.util.function.BiFunction;

/**
 * The single seam the session asks for schemas, instead of reaching for a different resolver per kind of index
 * abstraction. It composes one provider per kind — indices (the field-caps fetch), views (plan expansion),
 * datasets and external sources (the dataset rewrite plus external-source resolve) — and forwards each call to
 * the resolver that owns that kind's fetch, behaviour-identical to the direct calls it replaces. The provider
 * boundary is where the shared enumerate and authorize stages will later be lifted so every kind goes through one
 * front; for now the providers are thin and each kind keeps its native output shape.
 */
public final class SchemaService {

    private final IndexSchemaProvider indexProvider;
    private final ViewSchemaProvider viewProvider;
    private final DatasetSchemaProvider datasetProvider;

    public SchemaService(
        IndexResolver indexResolver,
        ViewResolver viewResolver,
        ExternalSourceResolver externalSourceResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteClusterService remoteClusterService,
        CrossProjectModeDecider crossProjectModeDecider,
        IndicesExpressionGrouper indicesExpressionGrouper,
        PlanTelemetry planTelemetry,
        Verifier verifier
    ) {
        this.indexProvider = new IndexSchemaProvider(
            indexResolver,
            remoteClusterService,
            crossProjectModeDecider,
            indicesExpressionGrouper,
            planTelemetry,
            verifier
        );
        this.viewProvider = new ViewSchemaProvider(viewResolver);
        this.datasetProvider = new DatasetSchemaProvider(externalSourceResolver, indexNameExpressionResolver);
    }

    /** Expand any view in {@code plan} into its underlying query. A plan rewrite, not a flat schema fetch. */
    public void replaceViews(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        ActionListener<ViewResolver.ViewResolutionResult> listener
    ) {
        viewProvider.replaceViews(plan, projectRouting, parser, listener);
    }

    /** Rewrite {@code FROM <dataset>} targets into external relations so analysis treats them like {@code EXTERNAL}. */
    public LogicalPlan rewriteDatasets(LogicalPlan parsed, ProjectMetadata projectMetadata) {
        return datasetProvider.rewriteDatasets(parsed, projectMetadata);
    }

    /** Resolve the schema of external sources (Iceberg tables / Parquet files) referenced by the query. */
    public void resolveExternalSources(LogicalPlan plan, List<String> icebergPaths, ActionListener<ExternalSourceResolution> listener) {
        datasetProvider.resolveExternalSources(plan, icebergPaths, listener);
    }

    /**
     * Resolve the main index patterns of the query: run the field-caps fetch for each pattern, init cross-cluster
     * state, and accumulate the resolution (plus the minimum transport version) into {@code result}.
     */
    public void resolveMainIndices(SchemaContext ctx, PreAnalysisResult result, ActionListener<PreAnalysisResult> listener) {
        indexProvider.resolveMainIndices(ctx, result, listener);
    }

    /** Resolve every lookup index referenced by the query, validating each resolves to a single lookup-mode index. */
    public void resolveLookupIndices(SchemaContext ctx, PreAnalysisResult result, ActionListener<PreAnalysisResult> listener) {
        indexProvider.resolveLookupIndices(ctx, result, listener);
    }
}
