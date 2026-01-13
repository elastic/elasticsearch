/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.View;

import java.util.List;

public class ViewResolutionService {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public ViewResolutionService(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public ViewResolutionResult resolveViews(
        ProjectState projectState,
        String[] indexPatterns,
        org.elasticsearch.action.support.IndicesOptions indicesOptions,
        ResolvedIndexExpressions resolvedIndexExpressions
    ) {
        IndexAbstractionResolver indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);
        var indicesLookup = projectState.metadata().getIndicesLookup();

        if (resolvedIndexExpressions == null) {
            resolvedIndexExpressions = indexAbstractionResolver.resolveIndexAbstractions(
                List.of(indexPatterns),
                indicesOptions,
                projectState.metadata(),
                componentSelector -> projectState.metadata().getIndicesLookup().keySet(),
                (index, selector) -> true, // Assume that a view is its own data component but has no failure component
                true
            );
        }

        View[] views = resolvedIndexExpressions.getLocalIndicesList()
            .stream()
            .map(indicesLookup::get)
            .filter(indexAbstraction -> indexAbstraction != null && indexAbstraction.getType() == IndexAbstraction.Type.VIEW)
            .map(indexAbstraction -> (View) indexAbstraction)
            .toArray(View[]::new);

        return new ViewResolutionResult(views, resolvedIndexExpressions);
    }

    public record ViewResolutionResult(View[] views, ResolvedIndexExpressions resolvedIndexExpressions) {}
}
