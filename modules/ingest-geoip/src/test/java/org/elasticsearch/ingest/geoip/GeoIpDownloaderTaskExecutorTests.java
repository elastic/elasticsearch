/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeoIpDownloaderTaskExecutorTests extends ESTestCase {

    public void testHasAtLeastOneGeoipProcessorWhenPipelineIsManaged() {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.getMetadata()).thenReturn(metadata);

        final IngestMetadata[] ingestMetadata = new IngestMetadata[1];
        when(metadata.custom(IngestMetadata.TYPE)).thenAnswer(invocationOnmock -> ingestMetadata[0]);

        final Settings[] indexSettings = new Settings[1];
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getSettings()).thenAnswer(invocationMock -> indexSettings[0]);
        when(metadata.indices()).thenReturn(Map.of("index", indexMetadata));

        List<String> pipelinesConfigJson = getPipelinesWithGeoIpProcessors().stream().map(jsonConfig -> {
            Map<String, Object> configMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, jsonConfig, false);
            configMap.put("_meta", Map.of("managed", true));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), baos)) {
                builder.map(configMap).close();
                return baos.toString(StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        {
            for (String pipelineConfigJson : pipelinesConfigJson) {
                ingestMetadata[0] = new IngestMetadata(
                    Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipelineConfigJson), XContentType.JSON))
                );
                // The pipeline is not used in any index, expected to return false.
                indexSettings[0] = Settings.EMPTY;
                assertFalse(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));

                // The pipeline is set as default pipeline in an index, expected to return true.
                indexSettings[0] = Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "_id1").build();
                assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));

                // The pipeline is set as final pipeline in an index, expected to return true.
                indexSettings[0] = Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "_id1").build();
                assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));
            }
        }
    }

    public void testHasAtLeastOneGeoipProcessor() {
        final IngestMetadata[] ingestMetadata = new IngestMetadata[1];
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(metadata.custom(IngestMetadata.TYPE)).thenAnswer(invocationOnmock -> ingestMetadata[0]);
        when(clusterState.getMetadata()).thenReturn(metadata);
        List<String> expectHitsInputs = getPipelinesWithGeoIpProcessors();
        List<String> expectMissesInputs = getPipelinesWithoutGeoIpProcessors();
        {
            // Test that hasAtLeastOneGeoipProcessor returns true for any pipeline with a geoip processor:
            for (String pipeline : expectHitsInputs) {
                ingestMetadata[0] = new IngestMetadata(
                    Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipeline), XContentType.JSON))
                );
                assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));
            }
        }
        {
            // Test that hasAtLeastOneGeoipProcessor returns false for any pipeline without a geoip processor:
            for (String pipeline : expectMissesInputs) {
                ingestMetadata[0] = new IngestMetadata(
                    Map.of("_id1", new PipelineConfiguration("_id1", new BytesArray(pipeline), XContentType.JSON))
                );
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
            ingestMetadata[0] = new IngestMetadata(configs);
            assertTrue(GeoIpDownloaderTaskExecutor.hasAtLeastOneGeoipProcessor(clusterState));
        }
    }

    /*
     * This method returns an assorted list of pipelines that have geoip processors -- ones that ought to cause hasAtLeastOneGeoipProcessor
     *  to return true.
     */
    private List<String> getPipelinesWithGeoIpProcessors() {
        String simpleGeoIpProcessor = """
            {
              "processors":[
                {
                  "geoip":{
                    "field":"provider"
                  }
                }
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
                    "on_failure":[
                      {
                        "geoip":{
                          "field":"error.message"
                        }
                      }
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
                    "processor":
                      {
                        "geoip":{
                          "field":"someField"
                        }
                      }
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
                          "processor":
                            {
                              "geoip":{
                                "field":"someField"
                              }
                            }
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
                                "on_failure":[
                                  {
                                    "geoip":{
                                      "field":"error.message"
                                    }
                                  }
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
                          "processor":
                            {
                              "geoip":{
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
}
