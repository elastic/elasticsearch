/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
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
    private final List<AbstractionSchemaProvider> providers;

    public SchemaService(
        IndexResolver indexResolver,
        ViewResolver viewResolver,
        ExternalSourceResolver externalSourceResolver,
        RemoteClusterService remoteClusterService,
        CrossProjectModeDecider crossProjectModeDecider,
        IndicesExpressionGrouper indicesExpressionGrouper,
        PlanTelemetry planTelemetry,
        Verifier verifier,
        Client client,
        Executor executor
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
        this.datasetProvider = new DatasetSchemaProvider(externalSourceResolver, client, executor, crossProjectModeDecider);
        this.providers = List.of(indexProvider, viewProvider, datasetProvider);
    }

    /**
     * Test-only constructor: inject the dispatch providers directly so the routing and grouping logic can be exercised
     * with lightweight fakes, without standing up every real resolver. The kind-specific accessors are unset.
     */
    SchemaService(List<AbstractionSchemaProvider> providers) {
        this.indexProvider = null;
        this.viewProvider = null;
        this.datasetProvider = null;
        this.providers = providers;
    }

    /**
     * The singular schema-resolution entry: resolve {@code names} of any index-abstraction kind. Each name is routed
     * to the provider that {@link AbstractionSchemaProvider#handles its kind} and resolved into a {@link ResolvedSchema};
     * the caller never branches on what kind a name is.
     */
    public void resolveSchema(
        SchemaContext ctx,
        ProjectMetadata projectMetadata,
        List<String> names,
        ActionListener<List<ResolvedSchema>> listener
    ) {
        var lookup = projectMetadata.getIndicesLookup();
        Map<AbstractionSchemaProvider, List<String>> byProvider = new LinkedHashMap<>();
        for (String name : names) {
            IndexAbstraction abstraction = lookup.get(name);
            // A name absent from the lookup is a not-yet-existing concrete index (e.g. a write-on-create target) or a
            // remote-cluster pattern the local lookup can't see — route it to the index provider, whose resolution
            // (and the security layer ahead of it) handles the missing/remote case, exactly as the legacy path did.
            IndexAbstraction.Type type = abstraction == null ? IndexAbstraction.Type.CONCRETE_INDEX : abstraction.getType();
            byProvider.computeIfAbsent(providerFor(type), p -> new ArrayList<>()).add(name);
        }
        if (byProvider.isEmpty()) {
            listener.onResponse(List.of());
            return;
        }
        var grouped = new GroupedActionListener<List<ResolvedSchema>>(
            byProvider.size(),
            listener.map(perProvider -> perProvider.stream().flatMap(List::stream).toList())
        );
        byProvider.forEach((provider, providerNames) -> provider.resolveSchema(ctx, projectMetadata, providerNames, grouped));
    }

    // package-private for SchemaServiceTests
    AbstractionSchemaProvider providerFor(IndexAbstraction.Type type) {
        for (AbstractionSchemaProvider provider : providers) {
            if (provider.handles().contains(type)) {
                return provider;
            }
        }
        throw new IllegalStateException("no schema provider handles index abstraction type [" + type + "]");
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

    /**
     * Authorize and rewrite {@code FROM <dataset>} targets into external relations so analysis treats them like the
     * inline {@code EXTERNAL} command. Only datasets the caller can {@code read} are rewritten.
     *
     * <p>The authorized datasets' configs are produced through the singular {@link #resolveSchema} dispatch — this is
     * the first production call site of the unified schema-discovery front. Each authorized dataset name routes to the
     * dataset provider's {@code resolveSchema} arm, which returns a {@link ResolvedSchema.Dataset} carrying the merged
     * external-source config; the dataset rewrite consumes that config instead of recomputing it inline. The merged
     * config is byte-identical to the inline path, so routing it through the umbrella is behaviour-identical.
     */
    public void resolveDatasets(LogicalPlan parsed, ProjectMetadata projectMetadata, ActionListener<LogicalPlan> listener) {
        datasetProvider.resolveDatasets(parsed, projectMetadata, this::resolveDatasetConfigs, listener);
    }

    /**
     * Resolve each authorized dataset name to its merged external-source config through the singular
     * {@link #resolveSchema} dispatch. The dispatch routes each name to the dataset provider's schema arm; we pull the
     * {@link ResolvedSchema.Dataset} config out of each result and key it by name for the rewrite.
     */
    private void resolveDatasetConfigs(
        ProjectMetadata projectMetadata,
        List<String> names,
        ActionListener<Map<String, Map<String, Object>>> listener
    ) {
        SchemaContext ctx = null; // the dataset schema arm reads no SchemaContext fields
        resolveSchema(ctx, projectMetadata, names, listener.map(resolved -> {
            Map<String, Map<String, Object>> configs = new LinkedHashMap<>();
            for (ResolvedSchema schema : resolved) {
                if (schema instanceof ResolvedSchema.Dataset dataset) {
                    configs.put(dataset.name(), dataset.config());
                } else {
                    throw new IllegalStateException(
                        "expected a dataset schema for [" + schema.name() + "] but got [" + schema.getClass().getSimpleName() + "]"
                    );
                }
            }
            return configs;
        }));
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
