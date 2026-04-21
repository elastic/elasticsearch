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
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.action.support.IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;

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
                    .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveViews(true).build())
                    .concreteTargetOptions(ERROR_WHEN_UNAVAILABLE_TARGETS)
                    .build(),
                null
            )
        );
    }

    public void testResolveDatasetsConcreteNames() {
        ViewResolutionService service = newService();
        final String n1 = "logs-dataset";
        final String n2 = "metrics-dataset";
        Dataset d1 = new Dataset(n1, new DataSourceReference("ds"), "s3://bucket/logs/*.parquet", null, Map.of());
        Dataset d2 = new Dataset(n2, new DataSourceReference("ds"), "s3://bucket/metrics/*.parquet", null, Map.of());
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).datasets(Map.of(n1, d1, n2, d2)).build())
            .build();

        ViewResolutionService.DatasetResolutionResult result = service.resolveDatasets(
            clusterState.projectState(ProjectId.DEFAULT),
            new String[] { n1, n2 },
            datasetOptions(true),
            null
        );

        assertThat(Arrays.stream(result.datasets()).map(Dataset::getName).toList(), containsInAnyOrder(n1, n2));
        assertNotNull(result.resolvedIndexExpressions());
    }

    public void testResolveDatasetsWildcard() {
        ViewResolutionService service = newService();
        final String n1 = "my-dataset-1";
        final String n2 = "other-dataset-2";
        Dataset d1 = new Dataset(n1, new DataSourceReference("ds-source"), "s3://bucket/logs/*.parquet", null, Map.of());
        Dataset d2 = new Dataset(n2, new DataSourceReference("ds-source"), "s3://bucket/metrics/*.parquet", null, Map.of());
        IndexMetadata index = IndexMetadata.builder("my-dataset-index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).put(index, false).datasets(Map.of(n1, d1, n2, d2)).build())
            .build();

        ViewResolutionService.DatasetResolutionResult result = service.resolveDatasets(
            clusterState.projectState(ProjectId.DEFAULT),
            new String[] { "*dataset*" },
            datasetOptions(true),
            null
        );

        assertThat(Arrays.stream(result.datasets()).map(Dataset::getName).toList(), containsInAnyOrder(n1, n2));
    }

    public void testResolveDatasetsExcludedWhenResolveDatasetsFalse() {
        ViewResolutionService service = newService();
        final String datasetName = "visible-dataset";
        Dataset dataset = new Dataset(datasetName, new DataSourceReference("ds"), "s3://bucket/p", null, Map.of());
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).datasets(Map.of(datasetName, dataset)).build())
            .build();

        ViewResolutionService.DatasetResolutionResult result = service.resolveDatasets(
            clusterState.projectState(ProjectId.DEFAULT),
            new String[] { "*" },
            datasetOptions(false),
            null
        );

        assertThat(result.datasets(), emptyArray());
    }

    public void testResolveMissingDatasetThrows() {
        ViewResolutionService service = newService();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).build())
            .build();
        assertThrows(
            IndexNotFoundException.class,
            () -> service.resolveDatasets(
                clusterState.projectState(ProjectId.DEFAULT),
                new String[] { "missing-dataset" },
                datasetOptions(true),
                null
            )
        );
    }

    public void testResolveDatasetsEmptyPatterns() {
        ViewResolutionService service = newService();
        ClusterState clusterState = emptyClusterState();
        ViewResolutionService.DatasetResolutionResult result = service.resolveDatasets(
            clusterState.projectState(ProjectId.DEFAULT),
            new String[0],
            datasetOptions(true),
            null
        );
        assertThat(result.datasets(), emptyArray());
    }

    private static IndicesOptions datasetOptions(boolean resolveDatasets) {
        return IndicesOptions.builder()
            .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(resolveDatasets).build())
            .concreteTargetOptions(ERROR_WHEN_UNAVAILABLE_TARGETS)
            .build();
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

}
