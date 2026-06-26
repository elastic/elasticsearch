/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.compatibility;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceChatCompletionTaskSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import static org.elasticsearch.inference.completion.Reasoning.ReasoningSummary;
import static org.elasticsearch.xpack.inference.InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS;
import static org.elasticsearch.xpack.inference.services.elastic.compatibility.ReasoningTaskSettingsCompatibility.REASONING_FIELD_UNSUPPORTED_MESSAGE;
import static org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModelTests.createModel;
import static org.hamcrest.Matchers.is;

public class ReasoningTaskSettingsCompatibilityTests extends ESTestCase {

    private static final String INFERENCE_ENTITY_ID = "test-id";
    private static final String MODEL_ID = "test-model";
    private static final String NODE_ID = "node-1";
    private static final String URL = "http://localhost";

    private static final ElasticInferenceServiceChatCompletionTaskSettings NON_EMPTY_TASK_SETTINGS =
        new ElasticInferenceServiceChatCompletionTaskSettings(new Reasoning(ReasoningEffort.MEDIUM, ReasoningSummary.DETAILED, null, null));

    private static final FeatureService FEATURE_SERVICE = new FeatureService(List.of(new InferenceFeatures()));

    private final ReasoningTaskSettingsCompatibility compatibility = new ReasoningTaskSettingsCompatibility();

    public void testClusterCompatibility_NonChatCompletionTaskType_NonEmptyReasoning_FeatureAbsent_ReturnsUnsupported() {
        var taskType = randomValueOtherThanMany(
            t -> t == TaskType.CHAT_COMPLETION || t == TaskType.ANY,
            () -> randomFrom(TaskType.values())
        );
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, taskType, NON_EMPTY_TASK_SETTINGS);

        var result = compatibility.clusterCompatibility(FEATURE_SERVICE, clusterState(false), model);

        assertFalse(result.isSupported());
        assertThat(result.errorMessage(), is(REASONING_FIELD_UNSUPPORTED_MESSAGE));
    }

    public void testClusterCompatibility_NonChatCompletionTaskType_NonEmptyReasoning_FeaturePresent_ReturnsSupported() {
        var taskType = randomValueOtherThanMany(
            t -> t == TaskType.CHAT_COMPLETION || t == TaskType.ANY,
            () -> randomFrom(TaskType.values())
        );
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, taskType, NON_EMPTY_TASK_SETTINGS);

        var result = compatibility.clusterCompatibility(FEATURE_SERVICE, clusterState(true), model);

        assertTrue(result.isSupported());
        assertNull(result.errorMessage());
    }

    public void testClusterCompatibility_EmptyTaskSettings_ReturnsSupported() {
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, TaskType.CHAT_COMPLETION, EmptyTaskSettings.INSTANCE);

        var result = compatibility.clusterCompatibility(FEATURE_SERVICE, clusterState(false), model);

        assertTrue(result.isSupported());
        assertNull(result.errorMessage());
    }

    public void testClusterCompatibility_EmptyReasoningTaskSettings_ReturnsSupported() {
        var model = createModel(
            URL,
            INFERENCE_ENTITY_ID,
            MODEL_ID,
            TaskType.CHAT_COMPLETION,
            ElasticInferenceServiceChatCompletionTaskSettings.EMPTY
        );

        // Empty task settings carry no reasoning field, so older nodes are unaffected.
        var result = compatibility.clusterCompatibility(FEATURE_SERVICE, clusterState(false), model);

        assertTrue(result.isSupported());
        assertNull(result.errorMessage());
    }

    public void testClusterCompatibility_NonEmptyReasoning_FeaturePresent_ReturnsSupported() {
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, TaskType.CHAT_COMPLETION, NON_EMPTY_TASK_SETTINGS);

        // The whole cluster understands the reasoning field, so the request is allowed.
        var result = compatibility.clusterCompatibility(FEATURE_SERVICE, clusterState(true), model);

        assertTrue(result.isSupported());
        assertNull(result.errorMessage());
    }

    public void testClusterCompatibility_NonEmptyReasoning_FeatureAbsent_ReturnsUnsupported() {
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, TaskType.CHAT_COMPLETION, NON_EMPTY_TASK_SETTINGS);

        // A non-empty reasoning field cannot be honored while some nodes lack the feature.
        var result = compatibility.clusterCompatibility(FEATURE_SERVICE, clusterState(false), model);

        assertFalse(result.isSupported());
        assertThat(result.errorMessage(), is(REASONING_FIELD_UNSUPPORTED_MESSAGE));
    }

    private static ClusterState clusterState(boolean hasReasoningFeature) {
        var features = hasReasoningFeature ? Set.of(INFERENCE_ELASTIC_REASONING_TASK_SETTINGS.id()) : Set.<String>of();
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(NODE_ID)).build())
            .nodeFeatures(Map.of(NODE_ID, features))
            .build();
    }
}
