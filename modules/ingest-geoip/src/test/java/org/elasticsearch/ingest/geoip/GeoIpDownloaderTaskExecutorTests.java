/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeoIpDownloaderTaskExecutorTests extends ESTestCase {

    public void testSettingsUpdateWiresToEnabledFlag() {
        final boolean localEnabled = randomBoolean();
        final var nodeSettings = Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), localEnabled).build();
        final var clusterSettings = new ClusterSettings(
            nodeSettings,
            Sets.union(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                Set.of(
                    GeoIpDownloaderTaskExecutor.ENABLED_SETTING,
                    GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING,
                    GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING
                )
            )
        );
        final var localNode = DiscoveryNodeUtils.create("local");
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)
        ) {
            final var projectResolver = mock(ProjectResolver.class);
            final var client = mock(org.elasticsearch.client.internal.Client.class);
            when(projectResolver.supportsMultipleProjects()).thenReturn(false);
            when(client.projectResolver()).thenReturn(projectResolver);

            var executor = new GeoIpDownloaderTaskExecutor(
                client,
                mock(HttpClient.class),
                clusterService,
                threadPool,
                mock(PersistentTasksService.class)
            );
            assertEquals(localEnabled, executor.isEnabled());

            boolean enabled = localEnabled;
            var state = clusterService.state();
            final int cycles = randomIntBetween(2, 5);
            for (int i = 0; i < cycles; i++) {
                if (randomBoolean()) {
                    enabled = randomBoolean();
                    state = stateWithEnabledSetting(state, enabled);
                }
                setState(clusterService, state);
                assertEquals(enabled, executor.isEnabled());
            }
        } finally {
            terminate(threadPool);
        }
    }

    public void testHasAtLeastOneGeoipProcessorWhenDownloadDatabaseOnPipelineCreationIsFalse() throws IOException {
        for (String pipelineConfigJson : getPipelinesWithGeoIpProcessors(false)) {
            var ingestMetadata = new IngestMetadata(
                Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipelineConfigJson), XContentType.JSON))
            );
            // The pipeline is not used in any index, expected to return false.
            var projectMetadata = projectMetadataWithIndex(b -> {}, ingestMetadata);
            assertFalse(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));

            // The pipeline is set as default pipeline in an index, expected to return true.
            projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.DEFAULT_PIPELINE.getKey(), "_id1"), ingestMetadata);
            assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));

            // The pipeline is set as final pipeline in an index, expected to return true.
            projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.FINAL_PIPELINE.getKey(), "_id1"), ingestMetadata);
            assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));
        }

    }

    /*
     * This tests that if a default or final pipeline has a pipeline processor that has a geoip processor that has
     * download_database_on_pipeline_creation set to false, then we will correctly acknowledge that the pipeline has a geoip processor so
     * that we download it appropriately.
     */
    public void testHasAtLeastOneGeoipProcessorInPipelineProcessorWhenDownloadDatabaseOnPipelineCreationIsFalse() throws IOException {
        String innerInnerPipelineJson = """
            {
              "processors":[""" + getGeoIpProcessor(false) + """
              ]
            }
            """;
        String innerPipelineJson = """
                        {
              "processors":[{"pipeline": {"name": "innerInnerPipeline"}}
              ]
            }
            """;
        String outerPipelineJson = """
                        {
              "processors":[{"pipeline": {"name": "innerPipeline"}}
              ]
            }
            """;
        IngestMetadata ingestMetadata = new IngestMetadata(
            Map.of(
                "innerInnerPipeline",
                new PipelineConfiguration("innerInnerPipeline", new BytesArray(innerInnerPipelineJson), XContentType.JSON),
                "innerPipeline",
                new PipelineConfiguration("innerPipeline", new BytesArray(innerPipelineJson), XContentType.JSON),
                "outerPipeline",
                new PipelineConfiguration("outerPipeline", new BytesArray(outerPipelineJson), XContentType.JSON)
            )
        );
        // The pipeline is not used in any index, expected to return false.
        var projectMetadata = projectMetadataWithIndex(b -> {}, ingestMetadata);
        assertFalse(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));

        // The pipeline is set as default pipeline in an index, expected to return true.
        projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.DEFAULT_PIPELINE.getKey(), "outerPipeline"), ingestMetadata);
        assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));

        // The pipeline is set as final pipeline in an index, expected to return true.
        projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.FINAL_PIPELINE.getKey(), "outerPipeline"), ingestMetadata);
        assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));
    }

    public void testHasAtLeastOneGeoipProcessorRecursion() throws IOException {
        /*
         * The pipeline in this test is invalid -- it has a cycle from outerPipeline -> innerPipeline -> innerInnerPipeline ->
         * innerPipeline. Since this method is called at server startup, we want to make sure that we don't get a StackOverFlowError and
         * that we don't throw any kind of validation exception (since that would be an unexpected change of behavior).
         */
        String innerInnerPipelineJson = """
            {
              "processors":[""" + getGeoIpProcessor(false) + """
              , {"pipeline": {"name": "innerPipeline"}}
              ]
            }
            """;
        String innerPipelineJson = """
                        {
              "processors":[{"pipeline": {"name": "innerInnerPipeline"}}
              ]
            }
            """;
        String outerPipelineJson = """
                        {
              "processors":[{"pipeline": {"name": "innerPipeline"}}
              ]
            }
            """;
        IngestMetadata ingestMetadata = new IngestMetadata(
            Map.of(
                "innerInnerPipeline",
                new PipelineConfiguration("innerInnerPipeline", new BytesArray(innerInnerPipelineJson), XContentType.JSON),
                "innerPipeline",
                new PipelineConfiguration("innerPipeline", new BytesArray(innerPipelineJson), XContentType.JSON),
                "outerPipeline",
                new PipelineConfiguration("outerPipeline", new BytesArray(outerPipelineJson), XContentType.JSON)
            )
        );
        // The pipeline is not used in any index, expected to return false.
        var projectMetadata = projectMetadataWithIndex(b -> {}, ingestMetadata);
        assertFalse(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));

        // The pipeline is set as default pipeline in an index, expected to return true.
        projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.DEFAULT_PIPELINE.getKey(), "outerPipeline"), ingestMetadata);
        assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));

        // The pipeline is set as final pipeline in an index, expected to return true.
        projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.FINAL_PIPELINE.getKey(), "outerPipeline"), ingestMetadata);
        assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));
    }

    public void testHasAtLeastOneGeoipProcessor() throws IOException {
        var projectId = Metadata.DEFAULT_PROJECT_ID;
        List<String> expectHitsInputs = getPipelinesWithGeoIpProcessors(true);
        List<String> expectMissesInputs = getPipelinesWithoutGeoIpProcessors();
        {
            // Test that hasAtLeastOneGeoipProcessor returns true for any pipeline with a geoip processor:
            for (String pipeline : expectHitsInputs) {
                var ingestMetadata = new IngestMetadata(
                    Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipeline), XContentType.JSON))
                );
                ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build();
                assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));
            }
        }
        {
            // Test that hasAtLeastOneGeoipProcessor returns false for any pipeline without a geoip processor:
            for (String pipeline : expectMissesInputs) {
                var ingestMetadata = new IngestMetadata(
                    Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipeline), XContentType.JSON))
                );
                ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build();
                assertFalse(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));
            }
        }
        {
            /*
             * Now test that hasAtLeastOneGeoipProcessor returns true for a mix of pipelines, some which have geoip processors and some
             * which do not:
             */
            Map<String, PipelineConfiguration> configs = new HashMap<>();
            for (String pipeline : expectHitsInputs) {
                String id = randomAlphaOfLength(20);
                configs.put(id, new PipelineConfiguration(id, new BytesArray(pipeline), XContentType.JSON));
            }
            for (String pipeline : expectMissesInputs) {
                String id = randomAlphaOfLength(20);
                configs.put(id, new PipelineConfiguration(id, new BytesArray(pipeline), XContentType.JSON));
            }
            var ingestMetadata = new IngestMetadata(configs);
            ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build();
            assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(projectMetadata));
        }
    }

    /*
     * This method returns an assorted list of pipelines that have geoip processors -- ones that ought to cause hasAtLeastOneGeoipProcessor
     *  to return true.
     */
    private List<String> getPipelinesWithGeoIpProcessors(boolean downloadDatabaseOnPipelineCreation) throws IOException {
        String simpleGeoIpProcessor = """
            {
              "processors":[""" + getGeoIpProcessor(downloadDatabaseOnPipelineCreation) + """
              ]
            }
            """;
        String onFailureWithGeoIpProcessor = """
            {
              "processors":[
                {
                  "rename":{
                    "field":"provider",
                    "target_field":"cloud.provider",
                    "on_failure":[""" + getGeoIpProcessor(downloadDatabaseOnPipelineCreation) + """
                    ]
                  }
                }
              ]
            }
            """;
        String foreachWithGeoIpProcessor = """
            {
              "processors":[
                {
                  "foreach":{
                    "field":"values",
                    "processor":""" + getGeoIpProcessor(downloadDatabaseOnPipelineCreation) + """
                  }
                }
              ]
            }
            """;
        String nestedForeachWithGeoIpProcessor = """
            {
              "processors":[
                {
                  "foreach":{
                    "field":"values",
                    "processor":
                      {
                        "foreach":{
                          "field":"someField",
                          "processor":""" + getGeoIpProcessor(downloadDatabaseOnPipelineCreation) + """
                        }
                      }
                  }
                }
              ]
            }
            """;
        String nestedForeachWithOnFailureWithGeoIpProcessor = """
            {
              "processors":[
                {
                  "foreach":{
                    "field":"values",
                    "processor":
                      {
                        "foreach":{
                          "field":"someField",
                          "processor":
                            {
                              "rename":{
                                "field":"provider",
                                "target_field":"cloud.provider",
                                "on_failure":[""" + getGeoIpProcessor(downloadDatabaseOnPipelineCreation) + """
                                ]
                              }
                            }
                        }
                      }
                  }
                }
              ]
            }
            """;
        String onFailureWithForeachWithGeoIp = """
            {
              "processors":[
                {
                  "rename":{
                    "field":"provider",
                    "target_field":"cloud.provider",
                    "on_failure":[
                      {
                        "foreach":{
                          "field":"values",
                          "processor":""" + getGeoIpProcessor(downloadDatabaseOnPipelineCreation) + """
                        }
                      }
                    ]
                  }
                }
              ]
            }
            """;
        return List.of(
            simpleGeoIpProcessor,
            onFailureWithGeoIpProcessor,
            foreachWithGeoIpProcessor,
            nestedForeachWithGeoIpProcessor,
            nestedForeachWithOnFailureWithGeoIpProcessor,
            onFailureWithForeachWithGeoIp
        );
    }

    /*
     * This method returns an assorted list of pipelines that _do not_ have geoip processors -- ones that ought to cause
     * hasAtLeastOneGeoipProcessor to return false.
     */
    private List<String> getPipelinesWithoutGeoIpProcessors() {
        String empty = """
            {
            }
            """;
        String noProcessors = """
            {
              "processors":[
              ]
            }
            """;
        String onFailureWithForeachWithSet = """
            {
              "processors":[
                {
                  "rename":{
                    "field":"provider",
                    "target_field":"cloud.provider",
                    "on_failure":[
                      {
                        "foreach":{
                          "field":"values",
                          "processor":
                            {
                              "set":{
                                "field":"someField"
                              }
                            }
                        }
                      }
                    ]
                  }
                }
              ]
            }
            """;
        return List.of(empty, noProcessors, onFailureWithForeachWithSet);
    }

    private String getGeoIpProcessor(boolean downloadDatabaseOnPipelineCreation) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject("geoip");
                {
                    builder.field("field", randomIdentifier());
                    if (downloadDatabaseOnPipelineCreation == false || randomBoolean()) {
                        builder.field("download_database_on_pipeline_creation", downloadDatabaseOnPipelineCreation);
                    }
                }
                builder.endObject();
            }
            builder.endObject();

            return Strings.toString(builder);
        }
    }

    private ProjectMetadata projectMetadataWithIndex(Consumer<Settings.Builder> consumer, IngestMetadata ingestMetadata) {
        var builder = indexSettings(IndexVersion.current(), 1, 1);
        consumer.accept(builder);
        var indexMetadata = new IndexMetadata.Builder("index").settings(builder.build()).build();
        return ProjectMetadata.builder(randomProjectIdOrDefault())
            .putCustom(IngestMetadata.TYPE, ingestMetadata)
            .put(indexMetadata, false)
            .build();
    }

    private static ClusterState stateWithEnabledSetting(ClusterState clusterState, boolean enabled) {
        final var persistentSettings = Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), enabled).build();
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).persistentSettings(persistentSettings))
            .build();
    }
}
