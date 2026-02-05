/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstractionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.List;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;

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
        if (indexPatterns == null || indexPatterns.length == 0) {
            return new ViewResolutionResult(new View[0], resolvedIndexExpressions);
        }

        IndexAbstractionResolver indexAbstractionResolver = new IndexAbstractionResolver(indexNameExpressionResolver);
        var indicesLookup = projectState.metadata().getIndicesLookup();

        if (resolvedIndexExpressions == null) {
            resolvedIndexExpressions = indexAbstractionResolver.resolveIndexAbstractions(
                List.of(indexPatterns),
                indicesOptions,
                projectState.metadata(),
                componentSelector -> indicesLookup.keySet(),
                (index, selector) -> true, // Assume that a view is its own data component but has no failure component
                true
            );
        }
        validateResolvedIndexExpressions(resolvedIndexExpressions, indicesOptions);
        View[] views = resolvedIndexExpressions.getLocalIndicesList()
            .stream()
            .map(indicesLookup::get)
            .filter(indexAbstraction -> indexAbstraction != null && indexAbstraction.getType() == IndexAbstraction.Type.VIEW)
            .map(indexAbstraction -> (View) indexAbstraction)
            .toArray(View[]::new);

        return new ViewResolutionResult(views, resolvedIndexExpressions);
    }

    private void validateResolvedIndexExpressions(ResolvedIndexExpressions resolvedIndexExpressions, IndicesOptions indicesOptions) {
        // Skip validation in lenient mode
        if (indicesOptions.allowNoIndices() && indicesOptions.ignoreUnavailable()) {
            return;
        }

        for (ResolvedIndexExpression expression : resolvedIndexExpressions.expressions()) {
            ResolvedIndexExpression.LocalExpressions localExpressions = expression.localExpressions();
            ResolvedIndexExpression.LocalIndexResolutionResult result = localExpressions.localIndexResolutionResult();
            String originalExpression = expression.original();

            // Check for missing or unauthorized concrete resources
            if (indicesOptions.ignoreUnavailable() == false) {
                if (result == CONCRETE_RESOURCE_NOT_VISIBLE) {
                    throw new IndexNotFoundException(originalExpression);
                } else if (result == CONCRETE_RESOURCE_UNAUTHORIZED) {
                    assert localExpressions.exception() != null
                        : "ResolvedIndexExpression should have exception set when concrete resource is unauthorized";
                    throw localExpressions.exception();
                }
            }

            // Check for empty wildcard expressions when allowNoIndices is false
            if (indicesOptions.allowNoIndices() == false && result == SUCCESS && localExpressions.indices().isEmpty()) {
                throw new IndexNotFoundException(originalExpression);
            }
        }
    }

    public record ViewResolutionResult(View[] views, ResolvedIndexExpressions resolvedIndexExpressions) {}
}
