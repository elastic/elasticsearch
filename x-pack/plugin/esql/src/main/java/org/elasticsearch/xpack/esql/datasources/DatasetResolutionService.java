/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;

import java.util.List;

/**
 * Resolves dataset patterns to the dataset names the caller is authorized to read. Mirrors
 * {@code ViewResolutionService}: when the security layer has already authorized and replaced the
 * request's index expressions (a security-filtered {@code indices:data/read/...} action), the
 * pre-resolved expressions are used as-is; the names a caller cannot read are not in them. The
 * fallback resolution (no security filter — e.g. tests) assumes everything authorized.
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
            return new DatasetResolutionResult(List.of(), resolvedIndexExpressions);
        }

        IndexAbstractionResolver indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);
        var indicesLookup = projectState.metadata().getIndicesLookup();

        if (resolvedIndexExpressions == null) {
            resolvedIndexExpressions = indexAbstractionResolver.resolveIndexAbstractions(
                List.of(indexPatterns),
                indicesOptions,
                projectState.metadata(),
                componentSelector -> indicesLookup.keySet(),
                (index, selector) -> true,
                true
            );
        }

        List<String> datasetNames = resolvedIndexExpressions.getLocalIndicesList().stream().filter(name -> {
            IndexAbstraction abs = indicesLookup.get(name);
            return abs != null && abs.getType() == IndexAbstraction.Type.DATASET;
        }).toList();

        return new DatasetResolutionResult(datasetNames, resolvedIndexExpressions);
    }

    public record DatasetResolutionResult(List<String> datasetNames, ResolvedIndexExpressions resolvedIndexExpressions) {}
}
