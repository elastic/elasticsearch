/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import static org.elasticsearch.inference.completion.Reasoning.ReasoningSummary;
import static org.elasticsearch.inference.completion.ReasoningTests.randomReasoning;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EFFORT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.SUMMARY_FIELD;
import static org.elasticsearch.xpack.inference.services.elastic.compatibility.CompletionCompatibilityService.REASONING_FIELD_UNSUPPORTED_MESSAGE;
import static org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceChatCompletionTaskSettings.EIS_REASONING_TASK_SETTINGS_ADDED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceChatCompletionTaskSettingsTests extends AbstractBWCSerializationTestCase<
    ElasticInferenceServiceChatCompletionTaskSettings> {

    private static final String UNKNOWN_TOP_LEVEL_FIELD = "unknown_field";
    private static final Map<String, Object> REASONING_TOP_LEVEL_UNKNOWN_FIELD_MAP = Map.of(
        REASONING_FIELD,
        Map.of(EFFORT_FIELD, ReasoningEffort.MEDIUM, SUMMARY_FIELD, ReasoningSummary.DETAILED),
        UNKNOWN_TOP_LEVEL_FIELD,
        "value"
    );
    private static final Map<String, Object> MEDIUM_DETAILED_REASONING_MAP = Map.of(
        REASONING_FIELD,
        Map.of(EFFORT_FIELD, ReasoningEffort.MEDIUM, SUMMARY_FIELD, ReasoningSummary.DETAILED)
    );
    private static final Reasoning MEDIUM_DETAILED_REASONING = new Reasoning(ReasoningEffort.MEDIUM, ReasoningSummary.DETAILED, null, null);
    private static final Reasoning LOW_REASONING = new Reasoning(ReasoningEffort.LOW, null, null, null);

    @Override
    protected Writeable.Reader<ElasticInferenceServiceChatCompletionTaskSettings> instanceReader() {
        return ElasticInferenceServiceChatCompletionTaskSettings::new;
    }

    @Override
    protected ElasticInferenceServiceChatCompletionTaskSettings createTestInstance() {
        return randomTaskSettings();
    }

    @Override
    protected ElasticInferenceServiceChatCompletionTaskSettings mutateInstance(ElasticInferenceServiceChatCompletionTaskSettings instance)
        throws IOException {
        return new ElasticInferenceServiceChatCompletionTaskSettings(
            randomValueOtherThan(instance.reasoning(), () -> randomFrom(randomReasoning(), null))
        );
    }

    @Override
    protected ElasticInferenceServiceChatCompletionTaskSettings mutateInstanceForVersion(
        ElasticInferenceServiceChatCompletionTaskSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    public void testIsEmpty_WhenReasoningIsNull() {
        var settings = new ElasticInferenceServiceChatCompletionTaskSettings((Reasoning) null);
        assertTrue(settings.isEmpty());
    }

    public void testIsEmpty_WhenReasoningIsPresent() {
        var settings = new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
        assertFalse(settings.isEmpty());
    }

    public void testFromMap_ParsesValidReasoning() {
        var settings = ElasticInferenceServiceChatCompletionTaskSettings.fromMap(
            MEDIUM_DETAILED_REASONING_MAP,
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.REQUEST
        );
        assertThat(settings.reasoning(), is(MEDIUM_DETAILED_REASONING));
    }

    public void testFromMap_EmptyMap_ReturnsEmptySettings() {
        var settings = ElasticInferenceServiceChatCompletionTaskSettings.fromMap(
            Map.of(),
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.REQUEST
        );
        assertTrue(settings.isEmpty());
    }

    public void testFromMap_InvalidReasoningField_Throws() {
        var map = Map.<String, Object>of(REASONING_FIELD, Map.of(EFFORT_FIELD, "super-high"));
        var ex = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceChatCompletionTaskSettings.fromMap(
                map,
                TaskType.CHAT_COMPLETION,
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            ex.getMessage(),
            containsString(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.TASK_SETTINGS, REASONING_FIELD))
        );
    }

    public void testFromMap_NonChatCompletionTaskType_WithReasoning_Throws() {
        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> ElasticInferenceServiceChatCompletionTaskSettings.fromMap(
                MEDIUM_DETAILED_REASONING_MAP,
                TaskType.COMPLETION,
                ConfigurationParseContext.REQUEST
            )
        );
        assertThat(
            ex.getMessage(),
            is(Strings.format("[%s] is only supported for the [%s] task type", REASONING_FIELD, TaskType.CHAT_COMPLETION))
        );
    }

    public void testFromMap_UnknownTopLevelField_StrictParser_Throws() {
        var ex = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceChatCompletionTaskSettings.fromMap(
                REASONING_TOP_LEVEL_UNKNOWN_FIELD_MAP,
                TaskType.CHAT_COMPLETION,
                ConfigurationParseContext.REQUEST
            )
        );

        assertThat(
            ex.getMessage(),
            containsString(Strings.format("[%s] unknown field [%s]", ModelConfigurations.TASK_SETTINGS, UNKNOWN_TOP_LEVEL_FIELD))
        );
    }

    public void testFromMap_UnknownTopLevelField_LenientParser_Succeeds() {
        var settings = ElasticInferenceServiceChatCompletionTaskSettings.fromMap(
            REASONING_TOP_LEVEL_UNKNOWN_FIELD_MAP,
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.PERSISTENT
        );
        assertThat(settings.reasoning(), is(MEDIUM_DETAILED_REASONING));
    }

    public void testToXContent_WithReasoning_ProducesExpectedJson() throws IOException {
        var settings = new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
        assertThat(Strings.toString(settings), is(XContentHelper.stripWhitespace("""
            {
              "reasoning": {
                "effort": "medium",
                "summary": "detailed"
              }
            }
            """)));
    }

    public void testToXContent_Empty_ProducesEmptyObject() {
        var settings = new ElasticInferenceServiceChatCompletionTaskSettings((Reasoning) null);
        assertThat(Strings.toString(settings), is("{}"));
    }

    public void testUpdatedTaskSettings_PresentReasoning_Replaces() {
        var stored = new ElasticInferenceServiceChatCompletionTaskSettings(LOW_REASONING);
        var updated = stored.updatedTaskSettings(MEDIUM_DETAILED_REASONING_MAP);

        assertThat(updated, is(new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING)));
    }

    public void testUpdatedTaskSettings_StoredKept_WhenNewSettingsHaveNoReasoning() {
        var stored = new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
        var updated = stored.updatedTaskSettings(Map.of());
        assertThat(updated, is(stored));
    }

    public void testUpdatedTaskSettings_ExplicitNullReasoning_ResetsReasoningToNull() {
        var stored = new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
        var map = new HashMap<String, Object>();
        map.put(REASONING_FIELD, null);
        var updated = (ElasticInferenceServiceChatCompletionTaskSettings) stored.updatedTaskSettings(map);
        assertNull(updated.reasoning());
        assertTrue(updated.isEmpty());
    }

    public void testUpdatedTaskSettings_UnknownTopLevelField_Throws() {
        var stored = new ElasticInferenceServiceChatCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
        var ex = expectThrows(XContentParseException.class, () -> stored.updatedTaskSettings(REASONING_TOP_LEVEL_UNKNOWN_FIELD_MAP));

        assertThat(
            ex.getMessage(),
            containsString(Strings.format("[%s] unknown field [%s]", ModelConfigurations.TASK_SETTINGS, UNKNOWN_TOP_LEVEL_FIELD))
        );
    }

    public void testMergeReasoning_BodyWins_WhenBodyNonNull() {
        var merged = ElasticInferenceServiceChatCompletionTaskSettings.mergeReasoning(MEDIUM_DETAILED_REASONING, LOW_REASONING);
        assertThat(merged, is(MEDIUM_DETAILED_REASONING));
    }

    public void testMergeReasoning_StoredUsed_WhenBodyNull() {
        var merged = ElasticInferenceServiceChatCompletionTaskSettings.mergeReasoning(null, LOW_REASONING);
        assertThat(merged, is(LOW_REASONING));
    }

    public void testMergeReasoning_NullBoth_ReturnsNull() {
        assertNull(ElasticInferenceServiceChatCompletionTaskSettings.mergeReasoning(null, null));
    }

    /**
     * Versions before {@link ElasticInferenceServiceChatCompletionTaskSettings#EIS_REASONING_TASK_SETTINGS_ADDED} throw an exception
     * when serializing non-null reasoning settings, so we filter those out of the bwc versions to avoid test failures.
     * The logic is tested directly by {@link #testReasoningField_IsNotBackwardsCompatible}
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(EIS_REASONING_TASK_SETTINGS_ADDED)).toList();
    }

    public void testReasoningField_IsNotBackwardsCompatible() throws IOException {
        testSerializationIsNotBackwardsCompatible(EIS_REASONING_TASK_SETTINGS_ADDED, ignore -> true, REASONING_FIELD_UNSUPPORTED_MESSAGE);
    }

    public static ElasticInferenceServiceChatCompletionTaskSettings randomTaskSettings() {
        if (randomBoolean()) {
            return new ElasticInferenceServiceChatCompletionTaskSettings((Reasoning) null);
        }
        return new ElasticInferenceServiceChatCompletionTaskSettings(randomReasoning());
    }

    @Override
    protected ElasticInferenceServiceChatCompletionTaskSettings doParseInstance(XContentParser parser) throws IOException {
        return ElasticInferenceServiceChatCompletionTaskSettings.createParser(false)
            .apply(parser, ConfigurationParseContext.REQUEST)
            .build(TaskType.CHAT_COMPLETION);
    }
}
