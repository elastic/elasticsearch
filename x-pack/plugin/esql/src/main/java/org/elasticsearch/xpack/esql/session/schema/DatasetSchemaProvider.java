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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema for datasets and external sources. A {@code FROM <dataset>} target is first rewritten into an external
 * relation by {@link DatasetRewriter} (a pre-analysis plan rewrite); its schema is then read from the source
 * metadata by {@link ExternalSourceResolver}. The two steps run at different points in the pipeline, so the
 * provider keeps them as separate entry points rather than one call.
 */
final class DatasetSchemaProvider {

    private final ExternalSourceResolver externalSourceResolver;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    DatasetSchemaProvider(ExternalSourceResolver externalSourceResolver, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.externalSourceResolver = externalSourceResolver;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    LogicalPlan rewriteDatasets(LogicalPlan parsed, ProjectMetadata projectMetadata) {
        return DatasetRewriter.rewrite(parsed, projectMetadata, indexNameExpressionResolver);
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
