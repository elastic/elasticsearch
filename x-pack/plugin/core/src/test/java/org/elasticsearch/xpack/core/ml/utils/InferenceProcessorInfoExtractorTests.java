/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

public class InferenceProcessorInfoExtractorTests extends ESTestCase {

    public void testPipelineIdsByModelIds() throws IOException {
        String modelId1 = "trained_model_1";
        String modelId2 = "trained_model_2";
        String modelId3 = "trained_model_3";
        Set<String> modelIds = new HashSet<>(Arrays.asList(modelId1, modelId2, modelId3));

        ClusterState clusterState = buildClusterStateWithModelReferences(2, modelId1, modelId2, modelId3);

        Map<String, Set<String>> pipelineIdsByModelIds = InferenceProcessorInfoExtractor.pipelineIdsByResource(clusterState, modelIds);

        assertThat(pipelineIdsByModelIds.keySet(), equalTo(modelIds));
        assertThat(
            pipelineIdsByModelIds,
            hasEntry(modelId1, new HashSet<>(Arrays.asList("pipeline_with_model_" + modelId1 + 0, "pipeline_with_model_" + modelId1 + 1)))
        );
        assertThat(
            pipelineIdsByModelIds,
            hasEntry(modelId2, new HashSet<>(Arrays.asList("pipeline_with_model_" + modelId2 + 0, "pipeline_with_model_" + modelId2 + 1)))
        );
        assertThat(
            pipelineIdsByModelIds,
            hasEntry(modelId3, new HashSet<>(Arrays.asList("pipeline_with_model_" + modelId3 + 0, "pipeline_with_model_" + modelId3 + 1)))
        );
    }

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

    public void testNumInferenceProcessors() throws IOException {
        assertThat(InferenceProcessorInfoExtractor.countInferenceProcessors(buildClusterState(null)), equalTo(0));
        assertThat(InferenceProcessorInfoExtractor.countInferenceProcessors(buildClusterState(Metadata.EMPTY_METADATA)), equalTo(0));
        assertThat(
            InferenceProcessorInfoExtractor.countInferenceProcessors(buildClusterStateWithModelReferences(1, "model1", "model2", "model3")),
            equalTo(3)
        );
    }

    public void testNumInferenceProcessorsRecursivelyDefined() throws IOException {
        Map<String, PipelineConfiguration> configurations = new HashMap<>();
        configurations.put(
            "pipeline_with_model_top_level",
            randomBoolean()
                ? newConfigurationWithInferenceProcessor("top_level")
                : newConfigurationWithForeachProcessorProcessor("top_level")
        );
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .map(Collections.singletonMap("processors", Collections.singletonList(Collections.singletonMap("set", new HashMap<>() {
                    {
                        put("field", "foo");
                        put("value", "bar");
                        put(
                            "on_failure",
                            Arrays.asList(inferenceProcessorForModel("second_level"), forEachProcessorWithInference("third_level"))
                        );
                    }
                }))))
        ) {
            configurations.put(
                "pipeline_with_model_nested",
                new PipelineConfiguration("pipeline_with_model_nested", BytesReference.bytes(xContentBuilder), XContentType.JSON)
            );
        }

        IngestMetadata ingestMetadata = new IngestMetadata(configurations);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
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

        assertThat(InferenceProcessorInfoExtractor.countInferenceProcessors(cs), equalTo(3));
    }

    public void testScriptProcessorStringConfig() throws IOException {
        Set<String> expectedModelIds = Set.of("foo");

        ClusterState clusterState = buildClusterStateWithPipelineConfigurations(
            Map.of("processor_does_not_have_a_definition_object", newConfigurationWithScriptProcessor("foo"))
        );
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

        return buildClusterStateWithPipelineConfigurations(configurations);
    }

    private static ClusterState buildClusterStateWithPipelineConfigurations(Map<String, PipelineConfiguration> configurations)
        throws IOException {
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

    private static PipelineConfiguration newConfigurationWithScriptProcessor(String modelId) throws IOException {
        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .map(
                    Collections.singletonMap(
                        "processors",
                        List.of(forScriptProcessorWithStringConfig(), forEachProcessorWithInference(modelId))
                    )
                )
        ) {
            return new PipelineConfiguration(
                "pipeline_with_script_and_model_" + modelId,
                BytesReference.bytes(xContentBuilder),
                XContentType.JSON
            );
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

    private static Map<String, Object> inferenceProcessorForModel(String modelId) {
        return Collections.singletonMap(InferenceProcessorConstants.TYPE, new HashMap<>() {
            {
                put(InferenceResults.MODEL_ID_RESULTS_FIELD, modelId);
                put(
                    InferenceProcessorConstants.INFERENCE_CONFIG,
                    Collections.singletonMap(RegressionConfig.NAME.getPreferredName(), Collections.emptyMap())
                );
                put(InferenceProcessorConstants.TARGET_FIELD, "new_field");
                put(InferenceProcessorConstants.FIELD_MAP, Collections.singletonMap("source", "dest"));
            }
        });
    }

    private static Map<String, Object> forScriptProcessorWithStringConfig() {
        return Collections.singletonMap("script", "ctx.test=2;");
    }
}
