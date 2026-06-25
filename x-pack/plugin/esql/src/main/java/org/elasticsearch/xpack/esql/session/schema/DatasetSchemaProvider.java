/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.datasources.DatasetResolver;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Schema for datasets and external sources. A {@code FROM <dataset>} target is authorized for {@code read} and then
 * rewritten into an external relation; its schema is read from the source metadata by {@link ExternalSourceResolver}.
 * Authorization goes through {@link EsqlResolveDatasetAction} — a security-filtered {@code indices:data/read/...} action
 * that authorizes the dataset names exactly as an index read is authorized — so only datasets the caller can read are
 * rewritten. The per-relation dispatch and rewrite are owned by {@link DatasetResolver}; the schema-by-name path and
 * the external-source resolve are kept as separate entry points because they run at different points in the pipeline.
 */
final class DatasetSchemaProvider implements AbstractionSchemaProvider {

    private final ExternalSourceResolver externalSourceResolver;
    private final DatasetResolver datasetResolver;

    DatasetSchemaProvider(
        ExternalSourceResolver externalSourceResolver,
        Client client,
        Executor executor,
        CrossProjectModeDecider crossProjectModeDecider
    ) {
        this.externalSourceResolver = externalSourceResolver;
        this.datasetResolver = new DatasetResolver(client, executor, crossProjectModeDecider);
    }

    @Override
    public EnumSet<IndexAbstraction.Type> handles() {
        return EnumSet.of(IndexAbstraction.Type.DATASET);
    }

    @Override
    public void resolveSchema(
        SchemaContext ctx,
        ProjectMetadata projectMetadata,
        List<String> names,
        ActionListener<List<ResolvedSchema>> listener
    ) {
        List<ResolvedSchema> resolved = new ArrayList<>(names.size());
        for (String name : names) {
            resolved.add(new ResolvedSchema.Dataset(name, DatasetRewriter.datasetConfig(projectMetadata, name)));
        }
        listener.onResponse(resolved);
    }

    /**
     * Authorize the dataset names referenced by {@code parsed} for {@code read}, then rewrite the authorized ones into
     * external relations. Delegates to {@link DatasetResolver}, which dispatches one {@link EsqlResolveDatasetAction}
     * per FROM relation (the per-relation round-trip that makes exclusion semantics correct), then runs the synchronous
     * rewrite. When no FROM pattern could match a registered dataset, the plan is returned unchanged with no round-trip.
     * An explicitly-named dataset the caller cannot read surfaces as {@code Unknown index} (400), the same error a
     * missing index gives — existence-hiding, not a 403.
     */
    void resolveDatasets(
        LogicalPlan parsed,
        ProjectMetadata projectMetadata,
        DatasetResolver.DatasetConfigResolver configResolver,
        ActionListener<LogicalPlan> listener
    ) {
        datasetResolver.replaceDatasets(parsed, projectMetadata, configResolver, listener);
    }

    /**
     * Resolve the schema of the external sources (Iceberg tables / Parquet files) referenced by {@code icebergPaths},
     * pulling each source's per-path config and partition-filter hints out of the plan. Callers short-circuit on an
     * empty path list; the resolver itself also returns {@link ExternalSourceResolution#EMPTY} for one.
     */
    void resolveExternalSources(LogicalPlan plan, List<String> icebergPaths, ActionListener<ExternalSourceResolution> listener) {
        Map<String, Map<String, Object>> pathConfigs = extractExternalConfigs(plan);
        var filterHints = PartitionFilterHintExtractor.extract(plan);
        externalSourceResolver.resolve(icebergPaths, pathConfigs, filterHints.isEmpty() ? null : filterHints, listener);
    }

    /**
     * Map from table path to config across all {@link UnresolvedExternalRelation} nodes. Every {@code tablePath} is a
     * non-null {@link Literal} post-parsing; a non-Literal here is a precondition violation and throws rather than
     * silently dropping the entry.
     */
    static Map<String, Map<String, Object>> extractExternalConfigs(LogicalPlan plan) {
        Map<String, Map<String, Object>> pathConfigs = new HashMap<>();
        plan.forEachUp(UnresolvedExternalRelation.class, p -> {
            Expression tablePath = p.tablePath();
            if (tablePath instanceof Literal literal && literal.value() != null) {
                // If two UnresolvedExternalRelation nodes share the same literal path the later config overwrites the
                // earlier one. Today the dataset-rewrite pipeline produces at most one UER per concrete path, so this
                // is benign; if plan shapes evolve to allow per-call config divergence at the same path this needs to
                // become a merge or a verifier check.
                pathConfigs.put(BytesRefs.toString(literal.value()), p.config());
            } else {
                throw new IllegalStateException(
                    "UnresolvedExternalRelation tablePath is not a non-null Literal: ["
                        + (tablePath == null ? "null" : tablePath.sourceText())
                        + "]"
                );
            }
        });
        return pathConfigs;
    }
}
