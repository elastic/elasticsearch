/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

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

import static org.elasticsearch.action.support.IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS;

public class ViewResolutionServiceTests extends ESTestCase {

    public void testResolveMissing() {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
        ViewResolutionService service = new ViewResolutionService(resolver);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).build())
            .build();
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
}
