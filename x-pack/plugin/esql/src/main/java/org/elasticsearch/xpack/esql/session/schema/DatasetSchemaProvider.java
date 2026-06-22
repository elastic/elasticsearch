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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Map;

/**
 * Schema for datasets and external sources. A {@code FROM <dataset>} target is first rewritten into an external
 * relation by {@link DatasetRewriter} (a pre-analysis plan rewrite), and its schema is then read from the source
 * metadata by {@link ExternalSourceResolver}. The two steps run at different points in the pipeline, so the
 * provider keeps them as separate entry points rather than one call. Both forward verbatim.
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

    void resolveExternalSources(
        List<String> paths,
        Map<String, Map<String, Object>> pathConfigs,
        @Nullable Map<String, List<PartitionFilterHintExtractor.PartitionFilterHint>> filterHints,
        ActionListener<ExternalSourceResolution> listener
    ) {
        externalSourceResolver.resolve(paths, pathConfigs, filterHints, listener);
    }
}
