/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.settings.ImmutableEmptyTaskSettings;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import static org.elasticsearch.inference.completion.Reasoning.ReasoningSummary;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EFFORT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.SUMMARY_FIELD;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceCompletionModelCreatorTests extends ESTestCase {

    private static final String URL = "http://eis-gateway.com";
    private static final String INFERENCE_ID = "inference-id";
    private static final String MODEL_ID_VALUE = "my-model-id";

    private static final Map<String, Object> MEDIUM_DETAILED_REASONING_MAP = Map.of(
        REASONING_FIELD,
        Map.of(EFFORT_FIELD, ReasoningEffort.MEDIUM, SUMMARY_FIELD, ReasoningSummary.DETAILED)
    );
    private static final Reasoning MEDIUM_DETAILED_REASONING = new Reasoning(ReasoningEffort.MEDIUM, ReasoningSummary.DETAILED, null, null);

    public void testCreateFromMaps_ChatCompletion_WithReasoning_ParsesChatCompletionTaskSettings() {
        var model = createFromMaps(TaskType.CHAT_COMPLETION, MEDIUM_DETAILED_REASONING_MAP, ConfigurationParseContext.REQUEST);

        assertThat(model.getTaskSettings(), is(new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING)));
        assertThat(model.getTaskType(), is(TaskType.CHAT_COMPLETION));
    }

    public void testCreateFromMaps_ChatCompletion_EmptyTaskSettings_ReturnsEmptyChatCompletionTaskSettings() {
        var model = createFromMaps(TaskType.CHAT_COMPLETION, Map.of(), ConfigurationParseContext.REQUEST);

        var taskSettings = model.getTaskSettings();
        assertThat(taskSettings, instanceOf(ElasticInferenceServiceChatCompletionTaskSettings.class));
        assertTrue(taskSettings.isEmpty());
    }

    public void testCreateFromMaps_Completion_EmptyTaskSettings_ReturnsImmutableEmptyTaskSettings() {
        var model = createFromMaps(TaskType.COMPLETION, Map.of(), ConfigurationParseContext.REQUEST);

        assertThat(model.getTaskSettings(), sameInstance(ImmutableEmptyTaskSettings.INSTANCE));
        assertThat(model.getTaskType(), is(TaskType.COMPLETION));
    }

    private static ElasticInferenceServiceCompletionModel createFromMaps(
        TaskType taskType,
        Map<String, Object> taskSettings,
        ConfigurationParseContext context
    ) {
        return createCreator().createFromMaps(
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

    private static ElasticInferenceServiceCompletionModelCreator createCreator() {
        return new ElasticInferenceServiceCompletionModelCreator(ElasticInferenceServiceComponents.of(URL));
    }

    private static Map<String, Object> serviceSettingsMap() {
        return new HashMap<>(Map.of(MODEL_ID, MODEL_ID_VALUE));
    }
}
