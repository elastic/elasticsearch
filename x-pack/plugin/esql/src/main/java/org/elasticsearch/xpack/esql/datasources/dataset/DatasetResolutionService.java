/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;

/**
 * Resolves dataset name expressions (including wildcards) to {@link Dataset} instances, following the same
 * resolution flow as {@link org.elasticsearch.xpack.esql.view.ViewResolutionService}. Wildcards may expand across
 * the wider index namespace and are filtered down to datasets, while an explicit name must resolve to a dataset;
 * explicit missing, hidden, or co-resident foreign resources are reported as not-found. This is deliberately
 * stricter than view resolution, which returns an empty result for an explicit name that only matches a
 * co-resident foreign resource rather than 404-ing it; see the dataset-strict contract from #152497.
 */
public class DatasetResolutionService {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public DatasetResolutionService(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public DatasetResolutionResult resolveDatasets(
        ProjectState projectState,
        String[] indexPatterns,
        IndicesOptions indicesOptions,
        ResolvedIndexExpressions resolvedIndexExpressions
    ) {
        if (indexPatterns == null || indexPatterns.length == 0) {
            return new DatasetResolutionResult(new Dataset[0], resolvedIndexExpressions);
        }

        IndexAbstractionResolver indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);
        var indicesLookup = projectState.metadata().getIndicesLookup();

        if (resolvedIndexExpressions == null) {
            resolvedIndexExpressions = indexAbstractionResolver.resolveIndexAbstractions(
                List.of(indexPatterns),
                indicesOptions,
                projectState.metadata(),
                componentSelector -> indicesLookup.keySet(),
                (index, selector) -> true, // Assume that a dataset is its own data component but has no failure component
                true
            );
        }
        checkDatasetsExist(resolvedIndexExpressions, indicesOptions, indicesLookup);
        Dataset[] datasets = resolvedIndexExpressions.getLocalIndicesList()
            .stream()
            .map(indicesLookup::get)
            .filter(indexAbstraction -> indexAbstraction != null && indexAbstraction.getType() == IndexAbstraction.Type.DATASET)
            .map(indexAbstraction -> (Dataset) indexAbstraction)
            .toArray(Dataset[]::new);

        return new DatasetResolutionResult(datasets, resolvedIndexExpressions);
    }

    private void checkDatasetsExist(
        ResolvedIndexExpressions resolvedIndexExpressions,
        IndicesOptions indicesOptions,
        Map<String, IndexAbstraction> indicesLookup
    ) {
        if (indicesOptions.ignoreUnavailable()) {
            return;
        }
        for (ResolvedIndexExpression expression : resolvedIndexExpressions.expressions()) {
            if (expression.localExpressions().localIndexResolutionResult() == CONCRETE_RESOURCE_NOT_VISIBLE) {
                throw new IndexNotFoundException(expression.original());
            }
            if (Regex.isSimpleMatchPattern(expression.original()) == false
                && expression.localExpressions().indices().isEmpty() == false
                && expression.localExpressions()
                    .indices()
                    .stream()
                    .map(indicesLookup::get)
                    .noneMatch(
                        indexAbstraction -> indexAbstraction != null && indexAbstraction.getType() == IndexAbstraction.Type.DATASET
                    )) {
                throw new IndexNotFoundException(expression.original());
            }
        }
    }

    public record DatasetResolutionResult(Dataset[] datasets, ResolvedIndexExpressions resolvedIndexExpressions) {}
}
