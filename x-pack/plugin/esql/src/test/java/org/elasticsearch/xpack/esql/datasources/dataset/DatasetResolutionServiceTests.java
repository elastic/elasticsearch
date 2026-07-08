/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
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

import java.util.Map;

import static org.elasticsearch.action.support.IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS;
import static org.hamcrest.Matchers.equalTo;

public class DatasetResolutionServiceTests extends ESTestCase {

    private static final IndicesOptions DATASET_OPTIONS = IndicesOptions.builder()
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).build())
        .concreteTargetOptions(ERROR_WHEN_UNAVAILABLE_TARGETS)
        .build();

    public void testResolveMissing() {
        DatasetResolutionService service = newService();
        ClusterState clusterState = emptyClusterState();
        assertThrows(
            IndexNotFoundException.class,
            () -> service.resolveDatasets(clusterState.projectState(ProjectId.DEFAULT), new String[] { "missing" }, DATASET_OPTIONS, null)
        );
    }

    public void testResolveExplicitCoresidentForeignResourceIsNotFound() {
        // Repro for https://github.com/elastic/elasticsearch/issues/152980: an explicit name that exists but as a
        // non-dataset resource (here a plain index, standing in for e.g. a co-resident data stream like Security's
        // entities-metadata-default) must retain explicit-name not-found semantics.
        final String foreignIndexName = "entities-metadata-default";
        IndexMetadata foreignIndex = IndexMetadata.builder(foreignIndexName)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).put(foreignIndex, false).build())
            .build();

        DatasetResolutionService service = newService();
        assertThrows(
            IndexNotFoundException.class,
            () -> service.resolveDatasets(
                clusterState.projectState(ProjectId.DEFAULT),
                new String[] { foreignIndexName },
                DATASET_OPTIONS,
                null
            )
        );
    }

    public void testResolveListIgnoresCoresidentForeignResource() {
        final String datasetName = "cloudtrail_logs";
        Dataset dataset = new Dataset(
            datasetName,
            new DataSourceReference("aws_s3_logs"),
            "s3://bucket/cloudtrail/*.json.gz",
            null,
            Map.of()
        );
        IndexMetadata foreignIndex = IndexMetadata.builder("entities-metadata-default")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(
                ProjectMetadata.builder(ProjectId.DEFAULT).put(foreignIndex, false).datasets(Map.of(datasetName, dataset)).build()
            )
            .build();

        DatasetResolutionService service = newService();
        DatasetResolutionService.DatasetResolutionResult result = service.resolveDatasets(
            clusterState.projectState(ProjectId.DEFAULT),
            new String[] { "*" },
            DATASET_OPTIONS,
            null
        );
        assertThat(result.datasets().length, equalTo(1));
        assertThat(result.datasets()[0].name(), equalTo(datasetName));
    }

    private static DatasetResolutionService newService() {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
        return new DatasetResolutionService(resolver);
    }

    private static ClusterState emptyClusterState() {
        return ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).build()).build();
    }
}
