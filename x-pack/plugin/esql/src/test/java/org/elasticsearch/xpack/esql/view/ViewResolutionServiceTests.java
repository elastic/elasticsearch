/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED;
import static org.elasticsearch.action.support.IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS;
import static org.hamcrest.Matchers.containsString;

public class ViewResolutionServiceTests extends ESTestCase {

    public void testResolveMissing() {
        ViewResolutionService service = newService();
        ClusterState clusterState = emptyClusterState();
        assertThrows(
            IndexNotFoundException.class,
            () -> service.resolveViews(
                clusterState.projectState(ProjectId.DEFAULT),
                new String[] { "missing" },
                IndicesOptions.builder()
                    .wildcardOptions(IndicesOptions.WildcardOptions.builder().resolveViews(true))
                    .concreteTargetOptions(ERROR_WHEN_UNAVAILABLE_TARGETS)
                    .build(),
                null
            )
        );
    }

    public void testResolveUnauthorizedExpression() {
        ElasticsearchSecurityException unauthorized = new ElasticsearchSecurityException("unauthorized for test");
        ResolvedIndexExpressions resolvedIndexExpressions = new ResolvedIndexExpressions(
            List.of(resolvedIndexExpression("view-user1", Set.of("view-user1"), CONCRETE_RESOURCE_UNAUTHORIZED, unauthorized))
        );

        ElasticsearchSecurityException actual = expectThrows(
            ElasticsearchSecurityException.class,
            () -> newService().resolveViews(
                emptyClusterState().projectState(ProjectId.DEFAULT),
                new String[] { "view-user1" },
                strictResolveViewsOptions(),
                resolvedIndexExpressions
            )
        );
        assertSame(unauthorized, actual);
    }

    public void testResolveMixedNotVisibleAndUnauthorizedExpressions() {
        ElasticsearchSecurityException unauthorized = new ElasticsearchSecurityException("unauthorized for test");
        ResolvedIndexExpressions resolvedIndexExpressions = new ResolvedIndexExpressions(
            List.of(
                resolvedIndexExpression("missing-view", Set.of("missing-view"), CONCRETE_RESOURCE_NOT_VISIBLE, null),
                resolvedIndexExpression("view-user1", Set.of("view-user1"), CONCRETE_RESOURCE_UNAUTHORIZED, unauthorized)
            )
        );

        IndexNotFoundException actual = expectThrows(
            IndexNotFoundException.class,
            () -> newService().resolveViews(
                emptyClusterState().projectState(ProjectId.DEFAULT),
                new String[] { "missing-view", "view-user1" },
                strictResolveViewsOptions(),
                resolvedIndexExpressions
            )
        );
        assertThat(actual.getMessage(), containsString("missing-view"));
    }

    private static ViewResolutionService newService() {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
        return new ViewResolutionService(resolver);
    }

    private static ClusterState emptyClusterState() {
        return ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).build()).build();
    }

    private static IndicesOptions strictResolveViewsOptions() {
        return IndicesOptions.builder()
            .wildcardOptions(IndicesOptions.WildcardOptions.builder().resolveViews(true))
            .concreteTargetOptions(ERROR_WHEN_UNAVAILABLE_TARGETS)
            .build();
    }

    private static ResolvedIndexExpression resolvedIndexExpression(
        String original,
        Set<String> localExpressions,
        ResolvedIndexExpression.LocalIndexResolutionResult localIndexResolutionResult,
        ElasticsearchException exception
    ) {
        return new ResolvedIndexExpression(
            original,
            new ResolvedIndexExpression.LocalExpressions(localExpressions, localIndexResolutionResult, exception),
            Set.of()
        );
    }
}
