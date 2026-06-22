/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.List;
import java.util.Map;
import java.util.Set;
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
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this.indexProvider = new IndexSchemaProvider(indexResolver);
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
    public void resolveExternalSources(
        List<String> paths,
        Map<String, Map<String, Object>> pathConfigs,
        @Nullable Map<String, List<PartitionFilterHintExtractor.PartitionFilterHint>> filterHints,
        ActionListener<ExternalSourceResolution> listener
    ) {
        datasetProvider.resolveExternalSources(paths, pathConfigs, filterHints, listener);
    }

    public void resolveMainIndicesVersioned(
        String indexPattern,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions,
        TransportVersion minimumVersion,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        boolean trackUnmappedFieldIndices,
        IndicesExpressionGrouper indicesExpressionGrouper,
        ActionListener<Versioned<IndexResolution>> listener
    ) {
        indexProvider.resolveMainIndicesVersioned(
            indexPattern,
            fieldNames,
            requestFilter,
            includeAllDimensions,
            minimumVersion,
            useAggregateMetricDoubleWhenNotSupported,
            useDenseVectorWhenNotSupported,
            hasTimeSeriesAggregation,
            trackUnmappedFieldIndices,
            indicesExpressionGrouper,
            listener
        );
    }

    public void resolveFlatIndicesVersioned(
        boolean lenient,
        String indexPattern,
        String projectRouting,
        Set<String> fieldNames,
        QueryBuilder requestFilter,
        boolean includeAllDimensions,
        TransportVersion minimumVersion,
        boolean useAggregateMetricDoubleWhenNotSupported,
        boolean useDenseVectorWhenNotSupported,
        boolean hasTimeSeriesAggregation,
        boolean trackUnmappedFieldIndices,
        ActionListener<Versioned<IndexResolution>> listener
    ) {
        indexProvider.resolveFlatIndicesVersioned(
            lenient,
            indexPattern,
            projectRouting,
            fieldNames,
            requestFilter,
            includeAllDimensions,
            minimumVersion,
            useAggregateMetricDoubleWhenNotSupported,
            useDenseVectorWhenNotSupported,
            hasTimeSeriesAggregation,
            trackUnmappedFieldIndices,
            listener
        );
    }

    public void resolveLookupIndices(
        String indexPattern,
        Set<String> fieldNames,
        TransportVersion minimumVersion,
        ActionListener<IndexResolution> listener
    ) {
        indexProvider.resolveLookupIndices(indexPattern, fieldNames, minimumVersion, listener);
    }
}
