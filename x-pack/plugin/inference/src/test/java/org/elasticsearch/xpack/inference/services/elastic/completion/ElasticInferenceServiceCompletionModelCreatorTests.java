/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.compatibility.CompletionCompatibilityService;
import org.elasticsearch.xpack.inference.services.settings.EnforcingEmptyTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.ImmutableEmptyTaskSettings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import static org.elasticsearch.inference.completion.Reasoning.ReasoningSummary;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EFFORT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.SUMMARY_FIELD;
import static org.elasticsearch.xpack.inference.InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceCompletionModelCreatorTests extends ESTestCase {

    private static final String URL = "http://eis-gateway.com";
    private static final String INFERENCE_ID = "inference-id";
    private static final String MODEL_ID_VALUE = "my-model-id";
    private static final String NODE_ID = "node-1";

    private static final Map<String, Object> MEDIUM_DETAILED_REASONING_MAP = Map.of(
        REASONING_FIELD,
        Map.of(EFFORT_FIELD, ReasoningEffort.MEDIUM, SUMMARY_FIELD, ReasoningSummary.DETAILED)
    );
    private static final Reasoning MEDIUM_DETAILED_REASONING = new Reasoning(ReasoningEffort.MEDIUM, ReasoningSummary.DETAILED, null, null);
    private static final FeatureService FEATURE_SERVICE = new FeatureService(List.of(new InferenceFeatures()));

    public void testCreateFromMaps_ChatCompletion_WithReasoning_ParsesChatCompletionTaskSettings() {
        var model = createFromMaps(true, TaskType.CHAT_COMPLETION, MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.REQUEST);

        assertThat(model.getTaskSettings(), is(new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING)));
        assertThat(model.getTaskType(), is(TaskType.CHAT_COMPLETION));
    }

    public void testCreateFromMaps_ChatCompletion_EmptyTaskSettings_ReturnsEmptyChatCompletionTaskSettings() {
        var model = createFromMaps(true, TaskType.CHAT_COMPLETION, Map.of(), ConfigurationParseContext.REQUEST);

        var taskSettings = model.getTaskSettings();
        assertThat(taskSettings, instanceOf(ElasticInferenceServiceChatCompletionTaskSettings.class));
        assertTrue(taskSettings.isEmpty());
    }

    public void testCreateFromMaps_Completion_EmptyTaskSettings_ReturnsImmutableEmptyTaskSettings() {
        var model = createFromMaps(true, TaskType.COMPLETION, Map.of(), ConfigurationParseContext.REQUEST);

        assertThat(model.getTaskSettings(), sameInstance(ImmutableEmptyTaskSettings.INSTANCE));
        assertThat(model.getTaskType(), is(TaskType.COMPLETION));
    }

    public void testCreateFromMaps_ChatCompletion_ReasoningFeatureAbsent_EmptyTaskSettings_ReturnsEnforcingEmptyTaskSettings() {
        var model = createFromMaps(false, TaskType.CHAT_COMPLETION, Map.of(), ConfigurationParseContext.REQUEST);

        assertThat(model.getTaskSettings(), sameInstance(EnforcingEmptyTaskSettings.INSTANCE));
        assertThat(model.getTaskType(), is(TaskType.CHAT_COMPLETION));
    }

    public void testCreateFromMaps_Completion_ReasoningFeatureAbsent_EmptyTaskSettings_ReturnsEnforcingEmptyTaskSettings() {
        var model = createFromMaps(false, TaskType.COMPLETION, Map.of(), ConfigurationParseContext.REQUEST);

        assertThat(model.getTaskSettings(), sameInstance(EnforcingEmptyTaskSettings.INSTANCE));
        assertThat(model.getTaskType(), is(TaskType.COMPLETION));
    }

    public void testCreateFromMaps_ChatCompletion_ReasoningFeatureAbsent_NonEmptyTaskSettings_Request_Throws() {
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> createFromMaps(false, TaskType.CHAT_COMPLETION, MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getMessage(), is("[task_settings] Configuration contains unknown settings [reasoning]"));
    }

    public void testCreateFromMaps_ChatCompletion_ReasoningFeatureAbsent_NonEmptySettings_Persistent_ReturnsEnforcingEmpty() {
        var model = createFromMaps(false, TaskType.CHAT_COMPLETION, MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.PERSISTENT);

        assertThat(model.getTaskSettings(), sameInstance(EnforcingEmptyTaskSettings.INSTANCE));
        assertThat(model.getTaskType(), is(TaskType.CHAT_COMPLETION));
    }

    private static ElasticInferenceServiceCompletionModel createFromMaps(
        boolean hasReasoningFeature,
        TaskType taskType,
        Map<String, Object> taskSettings,
        ConfigurationParseContext context
    ) {
        return createCreator(hasReasoningFeature).createFromMaps(
            INFERENCE_ID,
            taskType,
            ElasticInferenceService.NAME,
            serviceSettingsMap(),
            taskSettings,
            null,
            null,
            context,
            null
        );
    }

    private static ElasticInferenceServiceCompletionModelCreator createCreator(boolean hasReasoningFeature) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState(hasReasoningFeature));
        return new ElasticInferenceServiceCompletionModelCreator(
            ElasticInferenceServiceComponents.of(URL),
            new CompletionCompatibilityService(clusterService, FEATURE_SERVICE)
        );
    }

    private static ClusterState clusterState(boolean hasReasoningFeature) {
        var features = hasReasoningFeature ? Set.of(INFERENCE_ELASTIC_REASONING_TASK_SETTINGS.id()) : Set.<String>of();
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(NODE_ID)).build())
            .nodeFeatures(Map.of(NODE_ID, features))
            .build();
    }

    private static Map<String, Object> serviceSettingsMap() {
        return new HashMap<>(Map.of(MODEL_ID, MODEL_ID_VALUE));
    }
}
