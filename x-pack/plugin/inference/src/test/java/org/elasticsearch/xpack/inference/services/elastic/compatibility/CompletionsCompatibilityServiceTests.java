/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.compatibility;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceFeatureService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.ImmutableEmptyTaskSettings;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import static org.elasticsearch.inference.completion.Reasoning.ReasoningSummary;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EFFORT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.SUMMARY_FIELD;
import static org.elasticsearch.xpack.inference.InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS;
import static org.elasticsearch.xpack.inference.services.elastic.compatibility.CompletionsCompatibilityService.REASONING_FIELD_UNSUPPORTED_MESSAGE;
import static org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModelTests.createModel;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompletionsCompatibilityServiceTests extends ESTestCase {

    private static final String INFERENCE_ENTITY_ID = "test-id";
    private static final String MODEL_ID = "test-model";
    private static final String NODE_ID = "node-1";
    private static final String URL = "http://localhost";

    private static final Reasoning MEDIUM_DETAILED_REASONING = new Reasoning(ReasoningEffort.MEDIUM, ReasoningSummary.DETAILED, null, null);
    private static final ElasticInferenceServiceChatCompletionTaskSettings NON_EMPTY_TASK_SETTINGS =
        new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
    private static final Map<String, Object> MEDIUM_DETAILED_REASONING_MAP = Map.of(
        REASONING_FIELD,
        Map.of(EFFORT_FIELD, ReasoningEffort.MEDIUM, SUMMARY_FIELD, ReasoningSummary.DETAILED)
    );

    private static final FeatureService FEATURE_SERVICE = new FeatureService(List.of(new InferenceFeatures()));

    // clusterCompatibility(...) checks the ClusterState passed in, not the ClusterService's own state, so an arbitrary
    // ClusterService state is used to build the compatibility service under test for these cases.
    private final CompletionsCompatibilityService compatibilityService = createCompatibilityService(true);

    public void testClusterCompatibility_NonChatCompletionTaskType_NonEmptyReasoning_FeatureAbsent_ReturnsUnsupported() {
        var taskType = randomValueOtherThanMany(
            t -> t == TaskType.CHAT_COMPLETION || t == TaskType.ANY,
            () -> randomFrom(TaskType.values())
        );
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, taskType, NON_EMPTY_TASK_SETTINGS);

        var result = compatibilityService.clusterCompatibility(clusterState(false), model);

        assertFalse(result.isSupported());
        assertThat(result.errorMessage(), is(REASONING_FIELD_UNSUPPORTED_MESSAGE));
    }

    public void testClusterCompatibility_NonChatCompletionTaskType_NonEmptyReasoning_FeaturePresent_ReturnsSupported() {
        var taskType = randomValueOtherThanMany(
            t -> t == TaskType.CHAT_COMPLETION || t == TaskType.ANY,
            () -> randomFrom(TaskType.values())
        );
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, taskType, NON_EMPTY_TASK_SETTINGS);

        var result = compatibilityService.clusterCompatibility(clusterState(true), model);

        assertTrue(result.isSupported());
        assertNull(result.errorMessage());
    }

    public void testClusterCompatibility_EmptyTaskSettings_ReturnsSupported() {
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, TaskType.CHAT_COMPLETION, EmptyTaskSettings.INSTANCE);

        var result = compatibilityService.clusterCompatibility(clusterState(false), model);

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
        var result = compatibilityService.clusterCompatibility(clusterState(false), model);

        assertTrue(result.isSupported());
        assertNull(result.errorMessage());
    }

    public void testClusterCompatibility_NonEmptyReasoning_FeaturePresent_ReturnsSupported() {
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, TaskType.CHAT_COMPLETION, NON_EMPTY_TASK_SETTINGS);

        // The whole cluster understands the reasoning field, so the request is allowed.
        var result = compatibilityService.clusterCompatibility(clusterState(true), model);

        assertTrue(result.isSupported());
        assertNull(result.errorMessage());
    }

    public void testClusterCompatibility_NonEmptyReasoning_FeatureAbsent_ReturnsUnsupported() {
        var model = createModel(URL, INFERENCE_ENTITY_ID, MODEL_ID, TaskType.CHAT_COMPLETION, NON_EMPTY_TASK_SETTINGS);

        // A non-empty reasoning field cannot be honored while some nodes lack the feature.
        var result = compatibilityService.clusterCompatibility(clusterState(false), model);

        assertFalse(result.isSupported());
        assertThat(result.errorMessage(), is(REASONING_FIELD_UNSUPPORTED_MESSAGE));
    }

    public void testGetTaskSettingsStrategy_FeatureAbsent_ReturnsEnforceEmptyTaskSettingsStrategy() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(randomFrom(TaskType.values()));

        assertThat(strategy, instanceOf(CompletionsCompatibilityService.EnforceEmptyTaskSettingsStrategy.class));
    }

    public void testEnforceEmptyTaskSettingsStrategy_CreateTaskSettings_EmptyMap_ReturnsEmptyTaskSettings() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var taskSettings = strategy.createTaskSettings(Map.of(), ConfigurationParseContext.REQUEST);

        assertThat(taskSettings, sameInstance(EmptyTaskSettings.INSTANCE));
    }

    public void testEnforceEmptyTaskSettingsStrategy_CreateTaskSettings_NonEmptyMap_Persistent_ReturnsEmptyTaskSettings() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var taskSettings = strategy.createTaskSettings(MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.PERSISTENT);

        assertThat(taskSettings, sameInstance(EmptyTaskSettings.INSTANCE));
    }

    public void testEnforceEmptyTaskSettingsStrategy_CreateTaskSettings_NonEmptyMap_Request_ThrowsBadRequest() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> strategy.createTaskSettings(MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is("[task_settings] Configuration contains unknown settings [reasoning]"));
    }

    public void testGetTaskSettingsStrategy_FeaturePresent_ChatCompletion_ReturnsReasoningTaskSettingsStrategy() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        assertThat(strategy, instanceOf(CompletionsCompatibilityService.ReasoningTaskSettingsStrategy.class));
    }

    public void testReasoningTaskSettingsStrategy_CreateTaskSettings_ReturnsChatCompletionTaskSettings() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var taskSettings = strategy.createTaskSettings(MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.REQUEST);

        assertThat(taskSettings, is(NON_EMPTY_TASK_SETTINGS));
    }

    public void testGetTaskSettingsStrategy_FeaturePresent_NonChatCompletion_ReturnsImmutableEmptyTaskSettingsStrategy() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.COMPLETION);

        assertThat(strategy, instanceOf(CompletionsCompatibilityService.ImmutableEmptyTaskSettingsStrategy.class));
    }

    public void testImmutableEmptyTaskSettingsStrategy_CreateTaskSettings_ReturnsImmutableEmptyTaskSettings() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.COMPLETION);

        var taskSettings = strategy.createTaskSettings(Map.of(), ConfigurationParseContext.REQUEST);

        assertThat(taskSettings, sameInstance(ImmutableEmptyTaskSettings.INSTANCE));
    }

    private static CompletionsCompatibilityService createCompatibilityService(boolean hasReasoningFeature) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState(hasReasoningFeature));
        return new CompletionsCompatibilityService(new InferenceFeatureService(clusterService, FEATURE_SERVICE));
    }

    private static ClusterState clusterState(boolean hasReasoningFeature) {
        var features = hasReasoningFeature ? Set.of(INFERENCE_ELASTIC_REASONING_TASK_SETTINGS.id()) : Set.<String>of();
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(NODE_ID)).build())
            .nodeFeatures(Map.of(NODE_ID, features))
            .build();
    }
}
