/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.utils;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class InferenceProcessorInfoExtractorTests extends ESTestCase {
    /*
     * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
     * or more contributor license agreements. Licensed under the Elastic License
     * 2.0; you may not use this file except in compliance with the Elastic License
     * 2.0.
     */

    public void testGetModelIdsFromInferenceProcessors() throws IOException {
        String modelId1 = "trained_model_1";
        String modelId2 = "trained_model_2";
        String modelId3 = "trained_model_3";
        Set<String> expectedModelIds = new HashSet<>(Arrays.asList(modelId1, modelId2, modelId3));

        ClusterState clusterState = buildClusterStateWithModelReferences(2, modelId1, modelId2, modelId3);
        IngestMetadata ingestMetadata = clusterState.metadata().custom(IngestMetadata.TYPE);
        Set<String> actualModelIds = InferenceProcessorInfoExtractor.getModelIdsFromInferenceProcessors(ingestMetadata);

        assertThat(actualModelIds, equalTo(expectedModelIds));
    }

    public void testGetModelIdsFromInferenceProcessorsWhenNull() throws IOException {

        Set<String> expectedModelIds = new HashSet<>(Arrays.asList());

        ClusterState clusterState = buildClusterStateWithModelReferences(0);
        IngestMetadata ingestMetadata = clusterState.metadata().custom(IngestMetadata.TYPE);
        Set<String> actualModelIds = InferenceProcessorInfoExtractor.getModelIdsFromInferenceProcessors(ingestMetadata);

        assertThat(actualModelIds, equalTo(expectedModelIds));
    }

    private static PipelineConfiguration newConfigurationWithOutInferenceProcessor(int i) throws IOException {
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .map(
                    Collections.singletonMap(
                        "processors",
                        Collections.singletonList(Collections.singletonMap("not_inference", Collections.emptyMap()))
                    )
                )
        ) {
            return new PipelineConfiguration("pipeline_without_model_" + i, BytesReference.bytes(xContentBuilder), XContentType.JSON);
        }
    }

    private static ClusterState buildClusterState(Metadata metadata) {
        return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
    }

    private static ClusterState buildClusterStateWithModelReferences(int numPipelineReferences, String... modelId) throws IOException {
        Map<String, PipelineConfiguration> configurations = Maps.newMapWithExpectedSize(modelId.length);
        for (String id : modelId) {
            for (int i = 0; i < numPipelineReferences; i++) {
                configurations.put(
                    "pipeline_with_model_" + id + i,
                    randomBoolean() ? newConfigurationWithInferenceProcessor(id) : newConfigurationWithForeachProcessorProcessor(id)
                );
            }

        }
        int numPipelinesWithoutModel = randomInt(5);
        for (int i = 0; i < numPipelinesWithoutModel; i++) {
            configurations.put("pipeline_without_model_" + i, newConfigurationWithOutInferenceProcessor(i));
        }
        IngestMetadata ingestMetadata = new IngestMetadata(configurations);

        return ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("min_node", new TransportAddress(InetAddress.getLoopbackAddress(), 9300)))
                    .add(DiscoveryNodeUtils.create("current_node", new TransportAddress(InetAddress.getLoopbackAddress(), 9302)))
                    .add(DiscoveryNodeUtils.create("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9304)))
                    .localNodeId("_node_id")
                    .masterNodeId("_node_id")
            )
            .build();
    }

    private static PipelineConfiguration newConfigurationWithInferenceProcessor(String modelId) throws IOException {
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .map(Collections.singletonMap("processors", Collections.singletonList(inferenceProcessorForModel(modelId))))
        ) {
            return new PipelineConfiguration("pipeline_with_model_" + modelId, BytesReference.bytes(xContentBuilder), XContentType.JSON);
        }
    }

    private static PipelineConfiguration newConfigurationWithForeachProcessorProcessor(String modelId) throws IOException {
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .map(Collections.singletonMap("processors", Collections.singletonList(forEachProcessorWithInference(modelId))))
        ) {
            return new PipelineConfiguration("pipeline_with_model_" + modelId, BytesReference.bytes(xContentBuilder), XContentType.JSON);
        }
    }

    private static Map<String, Object> forEachProcessorWithInference(String modelId) {
        return Collections.singletonMap("foreach", new HashMap<>() {
            {
                put("field", "foo");
                put("processor", inferenceProcessorForModel(modelId));
            }
        });
    }

    public static final String TYPE = "inference";
    public static final String INFERENCE_CONFIG = "inference_config";
    public static final String TARGET_FIELD = "target_field";
    public static final String FIELD_MAP = "field_map";

    private static Map<String, Object> inferenceProcessorForModel(String modelId) {
        return Collections.singletonMap(TYPE, new HashMap<>() {
            {
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, modelId);
                put(INFERENCE_CONFIG, Collections.singletonMap(RegressionConfig.NAME.getPreferredName(), Collections.emptyMap()));
                put(TARGET_FIELD, "new_field");
                put(FIELD_MAP, Collections.singletonMap("source", "dest"));
            }
        });
    }

}
