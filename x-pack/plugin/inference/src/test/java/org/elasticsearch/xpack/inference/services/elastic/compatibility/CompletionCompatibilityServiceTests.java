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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.EnforcingEmptyTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.ImmutableEmptyTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import static org.elasticsearch.inference.completion.Reasoning.ReasoningSummary;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EFFORT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.SUMMARY_FIELD;
import static org.elasticsearch.xpack.inference.InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompletionCompatibilityServiceTests extends ESTestCase {

    private static final String NODE_ID = "node-1";

    private static final Reasoning MEDIUM_DETAILED_REASONING = new Reasoning(ReasoningEffort.MEDIUM, ReasoningSummary.DETAILED, null, null);
    private static final ElasticInferenceServiceChatCompletionTaskSettings NON_EMPTY_TASK_SETTINGS =
        new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
    private static final Map<String, Object> MEDIUM_DETAILED_REASONING_MAP = Map.of(
        REASONING_FIELD,
        Map.of(EFFORT_FIELD, ReasoningEffort.MEDIUM, SUMMARY_FIELD, ReasoningSummary.DETAILED)
    );

    private static final FeatureService FEATURE_SERVICE = new FeatureService(List.of(new InferenceFeatures()));

    public void testGetTaskSettingsStrategy_FeatureAbsent_ReturnsMixedClusterTaskSettingsStrategy() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(randomFrom(TaskType.values()));

        assertThat(strategy, instanceOf(CompletionCompatibilityService.MixedClusterTaskSettingsStrategy.class));
    }

    public void testMixedClusterTaskSettingsStrategy_CreateTaskSettings_EmptyMap_ReturnsEnforcingEmptyTaskSettings() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var taskSettings = strategy.createTaskSettings(Map.of(), ConfigurationParseContext.REQUEST);

        assertThat(taskSettings, sameInstance(EnforcingEmptyTaskSettings.INSTANCE));
    }

    public void testMixedClusterTaskSettingsStrategy_CreateTaskSettings_NonEmptyMap_Persistent_ReturnsEnforcingEmptyTaskSettings() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var taskSettings = strategy.createTaskSettings(MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.PERSISTENT);

        assertThat(taskSettings, sameInstance(EnforcingEmptyTaskSettings.INSTANCE));
    }

    public void testMixedClusterTaskSettingsStrategy_CreateTaskSettings_NonEmptyMap_Request_ThrowsBadRequest() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> strategy.createTaskSettings(MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is("[task_settings] Configuration contains unknown settings [reasoning]"));
    }

    public void testMixedClusterTaskSettingsStrategy_CreateTaskSettingsThenUpdate_NonEmptyMap_Throws() {
        // Mirrors TransportUpdateInferenceModelAction: load the existing (persisted) task settings, then merge the
        // incoming update's task settings into it via updatedTaskSettings.
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);
        var existingTaskSettings = strategy.createTaskSettings(Map.of(), ConfigurationParseContext.PERSISTENT);

        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> existingTaskSettings.updatedTaskSettings(MEDIUM_DETAILED_REASONING_MAP)
        );

        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is("[task_settings] Configuration contains unknown settings [reasoning]"));
    }

    public void testMixedClusterTaskSettingsStrategy_CreateTaskSettingsFromReasoning_Null_ReturnsEnforcingEmptyTaskSettings() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(randomFrom(TaskType.CHAT_COMPLETION, TaskType.COMPLETION));

        var taskSettings = strategy.createTaskSettings(null);

        assertThat(taskSettings, is(Optional.of(EnforcingEmptyTaskSettings.INSTANCE)));
    }

    public void testMixedClusterTaskSettingsStrategy_CreateTaskSettingsFromReasoning_NonNull_ReturnsEmpty() {
        var strategy = createCompatibilityService(false).getTaskSettingsStrategy(randomFrom(TaskType.CHAT_COMPLETION, TaskType.COMPLETION));

        var taskSettings = strategy.createTaskSettings(MEDIUM_DETAILED_REASONING);

        // A mixed cluster cannot support reasoning settings yet, so the endpoint should be skipped until the
        // cluster finishes upgrading.
        assertThat(taskSettings, is(Optional.empty()));
    }

    public void testGetTaskSettingsStrategy_FeaturePresent_ChatCompletion_ReturnsChatCompletionTaskSettingsStrategy() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        assertThat(strategy, instanceOf(CompletionCompatibilityService.ChatCompletionTaskSettingsStrategy.class));
    }

    public void testChatCompletionTaskSettingsStrategy_CreateTaskSettings_ReturnsChatCompletionTaskSettings() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var taskSettings = strategy.createTaskSettings(MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.REQUEST);

        assertThat(taskSettings, is(NON_EMPTY_TASK_SETTINGS));
    }

    public void testChatCompletionTaskSettingsStrategy_CreateTaskSettingsFromReasoning_NonNull_ReturnsChatCompletionTaskSettings() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var taskSettings = strategy.createTaskSettings(MEDIUM_DETAILED_REASONING);

        assertThat(taskSettings, is(Optional.of(NON_EMPTY_TASK_SETTINGS)));
    }

    public void testChatCompletionTaskSettingsStrategy_CreateTaskSettingsFromReasoning_Null_ReturnsEmptyChatCompletionTaskSettings() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.CHAT_COMPLETION);

        var taskSettings = strategy.createTaskSettings(null);

        assertThat(taskSettings, is(Optional.of(ElasticInferenceServiceChatCompletionTaskSettings.EMPTY)));
    }

    public void testGetTaskSettingsStrategy_FeaturePresent_NonChatCompletion_ReturnsCompletionTaskSettingsStrategy() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.COMPLETION);

        assertThat(strategy, instanceOf(CompletionCompatibilityService.CompletionTaskSettingsStrategy.class));
    }

    public void testCompletionTaskSettingsStrategy_CreateTaskSettings_ReturnsImmutableEmptyTaskSettings() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.COMPLETION);

        var taskSettings = strategy.createTaskSettings(Map.of(), ConfigurationParseContext.REQUEST);

        assertThat(taskSettings, sameInstance(ImmutableEmptyTaskSettings.INSTANCE));
    }

    public void testCompletionTaskSettingsStrategy_CreateTaskSettingsFromReasoning_Null_ReturnsImmutableEmptyTaskSettings() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.COMPLETION);

        var taskSettings = strategy.createTaskSettings(null);

        assertThat(taskSettings, is(Optional.of(ImmutableEmptyTaskSettings.INSTANCE)));
    }

    public void testCompletionTaskSettingsStrategy_CreateTaskSettingsFromReasoning_NonNull_ReturnsEmpty() {
        var strategy = createCompatibilityService(true).getTaskSettingsStrategy(TaskType.COMPLETION);

        var taskSettings = strategy.createTaskSettings(MEDIUM_DETAILED_REASONING);

        // Reasoning is not supported by the completion task type, so the endpoint should be skipped.
        assertThat(taskSettings, is(Optional.empty()));
    }

    // This encodes the BWC guarantee that the mixed-cluster strategy relies on: a not-yet-upgraded node's
    // NamedWriteableRegistry predates ImmutableEmptyTaskSettings and only knows EmptyTaskSettings under the
    // name "empty_task_settings".
    public void testEmptyCompletionTaskSettings_MixedCluster_DeserializesOnOldNodeRegistry() throws IOException {
        var oldNodeRegistry = new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(TaskSettings.class, EmptyTaskSettings.NAME, EmptyTaskSettings::new))
        );

        // EnforcingEmptyTaskSettings (used while mixed) reuses EmptyTaskSettings's writeable name, so an old
        // node reads it back as a plain EmptyTaskSettings instead of failing.
        var deserialized = copyTaskSettings(EnforcingEmptyTaskSettings.INSTANCE, oldNodeRegistry);
        assertThat(deserialized, instanceOf(EmptyTaskSettings.class));

        // ImmutableEmptyTaskSettings (used once fully upgraded) is registered under a different name that an
        // old node's registry does not recognize, so deserialization fails. NamedWriteableRegistry asserts
        // before throwing IllegalArgumentException, and tests run with assertions enabled, so an AssertionError
        // is what actually surfaces here.
        expectThrows(AssertionError.class, () -> copyTaskSettings(ImmutableEmptyTaskSettings.INSTANCE, oldNodeRegistry));
    }

    private static TaskSettings copyTaskSettings(TaskSettings taskSettings, NamedWriteableRegistry registry) throws IOException {
        try (var out = new BytesStreamOutput()) {
            out.writeNamedWriteable(taskSettings);
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                return in.readNamedWriteable(TaskSettings.class);
            }
        }
    }

    private static CompletionCompatibilityService createCompatibilityService(boolean hasReasoningFeature) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState(hasReasoningFeature));
        return new CompletionCompatibilityService(clusterService, FEATURE_SERVICE);
    }

    private static ClusterState clusterState(boolean hasReasoningFeature) {
        var features = hasReasoningFeature ? Set.of(INFERENCE_ELASTIC_REASONING_TASK_SETTINGS.id()) : Set.<String>of();
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(NODE_ID)).build())
            .nodeFeatures(Map.of(NODE_ID, features))
            .build();
    }
}
