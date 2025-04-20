/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class GeoIpDownloaderTaskExecutorTests extends ESTestCase {

    public void testHasAtLeastOneGeoipProcessorWhenDownloadDatabaseOnPipelineCreationIsFalse() throws IOException {
        for (String pipelineConfigJson : getPipelinesWithGeoIpProcessors(false)) {
            var ingestMetadata = new IngestMetadata(
                Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipelineConfigJson), XContentType.JSON))
            );
            // The pipeline is not used in any index, expected to return false.
            var clusterState = clusterStateWithIndex(b -> {}, ingestMetadata);
            assertFalse(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));

            // The pipeline is set as default pipeline in an index, expected to return true.
            clusterState = clusterStateWithIndex(b -> b.put(IndexSettings.DEFAULT_PIPELINE.getKey(), "_id1"), ingestMetadata);
            assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));

            // The pipeline is set as final pipeline in an index, expected to return true.
            clusterState = clusterStateWithIndex(b -> b.put(IndexSettings.FINAL_PIPELINE.getKey(), "_id1"), ingestMetadata);
            assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));
        }

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
                ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                    .putProjectMetadata(ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build())
                    .build();
                assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));
            }
        }
        {
            // Test that hasAtLeastOneGeoipProcessor returns false for any pipeline without a geoip processor:
            for (String pipeline : expectMissesInputs) {
                var ingestMetadata = new IngestMetadata(
                    Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipeline), XContentType.JSON))
                );
                ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                    .putProjectMetadata(ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build())
                    .build();
                assertFalse(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));
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
            ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(ProjectMetadata.builder(projectId).putCustom(IngestMetadata.TYPE, ingestMetadata).build())
                .build();
            assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));
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

    private ClusterState clusterStateWithIndex(Consumer<Settings.Builder> consumer, IngestMetadata ingestMetadata) {
        var builder = indexSettings(IndexVersion.current(), 1, 1);
        consumer.accept(builder);
        var indexMetadata = new IndexMetadata.Builder("index").settings(builder.build()).build();
        var project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)
            .putCustom(IngestMetadata.TYPE, ingestMetadata)
            .put(indexMetadata, false)
            .build();
        return ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build();
    }
}
