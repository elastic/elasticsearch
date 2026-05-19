/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IngestIpLocationPluginTests extends ESTestCase {

    /**
     * Creates an IngestIpLocationPlugin wired with a mock IpLocationService, ready for clusterChanged() testing.
     * Returns both the plugin and the mock service so tests can verify interactions.
     */
    @SuppressWarnings("NewClassNamingConvention")
    private record PluginWithMocks(IngestIpLocationPlugin plugin, IpLocationService service) {}

    private static PluginWithMocks createPluginWithMocks() {
        IngestIpLocationPlugin plugin = new IngestIpLocationPlugin();
        IpLocationService ipLocationService = mock(IpLocationService.class);
        IngestService ingestService = mock(IngestService.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(ingestService.getClusterService()).thenReturn(clusterService);

        Processor.Parameters params = new Processor.Parameters(
            null, // env
            null, // scriptService
            null, // analysisRegistry
            null, // threadContext
            null, // relativeTimeSupplier
            null, // scheduler
            ingestService,
            null, // client
            null, // genericExecutor
            null, // matcherWatchdog
            null, // userAgentParserRegistry
            ipLocationService
        );
        plugin.getProcessors(params);
        return new PluginWithMocks(plugin, ipLocationService);
    }

    private static final DiscoveryNodes MASTER_NODES = DiscoveryNodes.builder()
        .add(DiscoveryNodeUtils.create("_id1"))
        .localNodeId("_id1")
        .masterNodeId("_id1")
        .build();

    /**
     * Two-node view where the local node ({@code _id1}) is <em>not</em> the master yet; used as the
     * {@code previousState} for tests that simulate a master handover to the local node.
     */
    private static final DiscoveryNodes REMOTE_MASTER_NODES = DiscoveryNodes.builder()
        .add(DiscoveryNodeUtils.create("_id1"))
        .add(DiscoveryNodeUtils.create("_id2"))
        .localNodeId("_id1")
        .masterNodeId("_id2")
        .build();

    private static ClusterState clusterStateWithProject(ProjectMetadata projectMetadata) {
        return ClusterState.builder(new ClusterName("test")).putProjectMetadata(projectMetadata).nodes(MASTER_NODES).build();
    }

    private static ClusterState clusterStateWithProjectAndNodes(ProjectMetadata projectMetadata) {
        return ClusterState.builder(new ClusterName("test"))
            .putProjectMetadata(projectMetadata)
            .nodes(IngestIpLocationPluginTests.REMOTE_MASTER_NODES)
            .build();
    }

    private static ClusterState emptyClusterState() {
        return ClusterState.builder(new ClusterName("test")).nodes(MASTER_NODES).build();
    }

    public void testProjectWithGeoipPipelineRequestsDownloads() throws IOException {
        var mocks = createPluginWithMocks();
        ProjectId projectId = randomProjectIdOrDefault();

        String pipelineJson = """
            {"processors":[{"geoip":{"field":"ip"}}]}""";
        var ingestMetadata = new IngestMetadata(
            Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipelineJson), XContentType.JSON))
        );
        ProjectMetadata projectMeta = ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build();
        ClusterState state1 = clusterStateWithProject(projectMeta);
        mocks.plugin.clusterChanged(new ClusterChangedEvent("test", state1, emptyClusterState()));
        verify(mocks.service).requestDownloads(projectId.id(), IpLocationConsumer.INGEST);
    }

    public void testProjectWithoutGeoipPipelineDoesNotRequestDownloads() throws IOException {
        var mocks = createPluginWithMocks();
        ProjectId projectId = randomProjectIdOrDefault();

        ProjectMetadata projectMeta = ProjectMetadata.builder(projectId).build();
        ClusterState state1 = clusterStateWithProject(projectMeta);
        mocks.plugin.clusterChanged(new ClusterChangedEvent("test", state1, emptyClusterState()));
        verify(mocks.service, never()).requestDownloads(anyString(), any(IpLocationConsumer.class));
    }

    public void testPipelineRemovedWithinProject() throws IOException {
        var mocks = createPluginWithMocks();
        ProjectId projectId = randomProjectIdOrDefault();

        // State 1: project has a geoip pipeline -> downloads requested
        String pipelineJson = """
            {"processors":[{"geoip":{"field":"ip"}}]}""";
        var ingestMetadata = new IngestMetadata(
            Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipelineJson), XContentType.JSON))
        );
        ProjectMetadata projectMeta1 = ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build();
        ClusterState state1 = clusterStateWithProject(projectMeta1);
        mocks.plugin.clusterChanged(new ClusterChangedEvent("test", state1, emptyClusterState()));
        verify(mocks.service).requestDownloads(projectId.id(), IpLocationConsumer.INGEST);

        // State 2: project still exists, but pipeline removed -> downloads cancelled
        var emptyIngestMetadata = new IngestMetadata(Map.of());
        ProjectMetadata projectMeta2 = ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, emptyIngestMetadata).build();
        ClusterState state2 = clusterStateWithProject(projectMeta2);
        mocks.plugin.clusterChanged(new ClusterChangedEvent("test", state2, state1));
        verify(mocks.service).cancelDownloadRequest(projectId.id(), IpLocationConsumer.INGEST);
    }

    /**
     * Simulates the hand-over of master to the local node while the project already has a geoip pipeline
     * and the {@code IpLocationDownloadConsumers} custom is absent (as it would be right after a 9.4 -> 9.5
     * master upgrade: the old 9.4 master never wrote the custom). The plugin must register the
     * {@link IpLocationConsumer#INGEST} consumer; otherwise ingest nodes would silently lose their
     * databases and never get fresh updates.
     *
     * <p>The two states share the same {@code ProjectMetadata} instance, so per-project metadata guards
     * (ingest / indices) report no change. Without an explicit "became master" bootstrap branch, the
     * existing logic bails out early and this test fails.
     */
    public void testBootstrapsIngestConsumerWhenBecomingMasterWithGeoipPipeline() throws IOException {
        var mocks = createPluginWithMocks();
        ProjectId projectId = randomProjectIdOrDefault();

        String pipelineJson = """
            {"processors":[{"geoip":{"field":"ip"}}]}""";
        var ingestMetadata = new IngestMetadata(
            Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipelineJson), XContentType.JSON))
        );
        ProjectMetadata projectMeta = ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build();

        ClusterState previousState = clusterStateWithProjectAndNodes(projectMeta);
        ClusterState currentState = clusterStateWithProject(projectMeta);

        mocks.plugin.clusterChanged(new ClusterChangedEvent("test", currentState, previousState));
        verify(mocks.service).requestDownloads(projectId.id(), IpLocationConsumer.INGEST);
    }

    /**
     * Steady-state event on a long-running master: same project, same pipeline, no metadata change.
     * The bootstrap must not fire — otherwise every unrelated cluster state update would re-scan
     * pipelines and re-submit a master-routed transport action.
     */
    public void testNoBootstrapWhenAlreadyMasterAndNoMetadataChanges() throws IOException {
        var mocks = createPluginWithMocks();
        ProjectId projectId = randomProjectIdOrDefault();

        String pipelineJson = """
            {"processors":[{"geoip":{"field":"ip"}}]}""";
        var ingestMetadata = new IngestMetadata(
            Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipelineJson), XContentType.JSON))
        );
        ProjectMetadata projectMeta = ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build();

        ClusterState previousState = clusterStateWithProject(projectMeta);
        ClusterState currentState = clusterStateWithProject(projectMeta);

        mocks.plugin.clusterChanged(new ClusterChangedEvent("test", currentState, previousState));
        verify(mocks.service, never()).requestDownloads(anyString(), any(IpLocationConsumer.class));
        verify(mocks.service, never()).cancelDownloadRequest(anyString(), any(IpLocationConsumer.class));
    }

    /**
     * Hand-over to the local node, with a project that has no geoip pipeline. The reconcile must not register
     * {@link IpLocationConsumer#INGEST} — there is nothing to download. It does call
     * {@link org.elasticsearch.iplocation.api.IpLocationService#cancelDownloadRequest} as part of the unified
     * master-takeover reconcile; that call is an idempotent no-op in production
     * (the {@code IpLocationDownloadConsumers} custom is absent or doesn't contain {@code INGEST}, so the
     * service short-circuits before submitting a transport action).
     */
    public void testReconcileOnBecomingMasterWithoutGeoipPipeline() throws IOException {
        var mocks = createPluginWithMocks();
        ProjectId projectId = randomProjectIdOrDefault();

        ProjectMetadata projectMeta = ProjectMetadata.builder(projectId).build();

        ClusterState previousState = clusterStateWithProjectAndNodes(projectMeta);
        ClusterState currentState = clusterStateWithProject(projectMeta);

        mocks.plugin.clusterChanged(new ClusterChangedEvent("test", currentState, previousState));
        verify(mocks.service, never()).requestDownloads(anyString(), any(IpLocationConsumer.class));
        verify(mocks.service).cancelDownloadRequest(projectId.id(), IpLocationConsumer.INGEST);
    }

    public void testHasAtLeastOneGeoipProcessorWhenDownloadDatabaseOnPipelineCreationIsFalse() throws IOException {
        for (String pipelineConfigJson : getPipelinesWithGeoIpProcessors(false)) {
            var ingestMetadata = new IngestMetadata(
                Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipelineConfigJson), XContentType.JSON))
            );
            // The pipeline is not used in any index, expected to return false.
            var projectMetadata = projectMetadataWithIndex(b -> {}, ingestMetadata);
            assertFalse(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));

            // The pipeline is set as default pipeline in an index, expected to return true.
            projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.DEFAULT_PIPELINE.getKey(), "_id1"), ingestMetadata);
            assertTrue(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));

            // The pipeline is set as final pipeline in an index, expected to return true.
            projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.FINAL_PIPELINE.getKey(), "_id1"), ingestMetadata);
            assertTrue(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));
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
        assertFalse(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));

        // The pipeline is set as default pipeline in an index, expected to return true.
        projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.DEFAULT_PIPELINE.getKey(), "outerPipeline"), ingestMetadata);
        assertTrue(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));

        // The pipeline is set as final pipeline in an index, expected to return true.
        projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.FINAL_PIPELINE.getKey(), "outerPipeline"), ingestMetadata);
        assertTrue(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));
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
        assertFalse(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));

        // The pipeline is set as default pipeline in an index, expected to return true.
        projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.DEFAULT_PIPELINE.getKey(), "outerPipeline"), ingestMetadata);
        assertTrue(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));

        // The pipeline is set as final pipeline in an index, expected to return true.
        projectMetadata = projectMetadataWithIndex(b -> b.put(IndexSettings.FINAL_PIPELINE.getKey(), "outerPipeline"), ingestMetadata);
        assertTrue(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));
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
                assertTrue(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));
            }
        }
        {
            // Test that hasAtLeastOneGeoipProcessor returns false for any pipeline without a geoip processor:
            for (String pipeline : expectMissesInputs) {
                var ingestMetadata = new IngestMetadata(
                    Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipeline), XContentType.JSON))
                );
                ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build();
                assertFalse(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));
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
            assertTrue(IngestIpLocationPlugin.hasAtLeastOneGeoipProcessor(projectMetadata));
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
}
