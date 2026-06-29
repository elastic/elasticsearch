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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.esql.session.schema.AbstractionResolver;

import java.util.List;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;

public class ViewResolutionService {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public ViewResolutionService(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public ViewResolutionResult resolveViews(
        ProjectState projectState,
        String[] indexPatterns,
        IndicesOptions indicesOptions,
        ResolvedIndexExpressions resolvedIndexExpressions
    ) {
        if (indexPatterns == null || indexPatterns.length == 0) {
            return new ViewResolutionResult(new View[0], resolvedIndexExpressions);
        }

        AbstractionResolver abstractionResolver = new AbstractionResolver(indexNameExpressionResolver);
        var indicesLookup = projectState.metadata().getIndicesLookup();

        // Resolve + classify through the kind-blind front (stage ①). When the security filter already resolved the
        // expressions onto the request we only classify them; otherwise we resolve under the view visibility flags.
        AbstractionResolver.Resolution resolution = resolvedIndexExpressions == null
            ? abstractionResolver.resolve(
                List.of(indexPatterns),
                indicesOptions,
                projectState.metadata(),
                componentSelector -> indicesLookup.keySet(),
                (index, selector) -> true // Assume that a view is its own data component but has no failure component
            )
            : AbstractionResolver.classify(resolvedIndexExpressions, projectState.metadata());
        resolvedIndexExpressions = resolution.expressions();

        checkViewsExist(resolvedIndexExpressions, indicesOptions);
        View[] views = resolution.abstractionsOfKind(IndexAbstraction.Type.VIEW)
            .stream()
            .map(indexAbstraction -> (View) indexAbstraction)
            .toArray(View[]::new);

        return new ViewResolutionResult(views, resolvedIndexExpressions);
    }

    private void checkViewsExist(ResolvedIndexExpressions resolvedIndexExpressions, IndicesOptions indicesOptions) {
        if (indicesOptions.ignoreUnavailable()) {
            return;
        }
        for (ResolvedIndexExpression expression : resolvedIndexExpressions.expressions()) {
            if (expression.localExpressions().localIndexResolutionResult() == CONCRETE_RESOURCE_NOT_VISIBLE) {
                throw new IndexNotFoundException(expression.original());
            }
        }
    }

    public record ViewResolutionResult(View[] views, ResolvedIndexExpressions resolvedIndexExpressions) {}
}
