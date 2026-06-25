/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import static org.elasticsearch.inference.completion.Reasoning.ReasoningSummary;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceCompletionTaskSettingsTests extends InferenceSettingsTestCase<
    ElasticInferenceServiceCompletionTaskSettings> {

    // Reusable test values — effort=medium + summary=detailed, no exclude/enabled
    private static final Reasoning MEDIUM_DETAILED_REASONING = new Reasoning(ReasoningEffort.MEDIUM, ReasoningSummary.DETAILED, null, null);
    private static final Reasoning HIGH_REASONING = new Reasoning(ReasoningEffort.HIGH, null, null, null);
    private static final Reasoning LOW_REASONING = new Reasoning(ReasoningEffort.LOW, null, null, null);

    // ---------- InferenceSettingsTestCase abstract methods ----------

    @Override
    protected Writeable.Reader<ElasticInferenceServiceCompletionTaskSettings> instanceReader() {
        return ElasticInferenceServiceCompletionTaskSettings::new;
    }

    @Override
    protected ElasticInferenceServiceCompletionTaskSettings createTestInstance() {
        return randomTaskSettings();
    }

    /**
     * Drop the {@code reasoning} field for transport versions that predate its introduction.
     * Old nodes write/read nothing for this class's {@code reasoning} component, so the BWC
     * round-trip must use an instance that also has {@code null} reasoning.
     */
    @Override
    protected ElasticInferenceServiceCompletionTaskSettings mutateInstanceForVersion(
        ElasticInferenceServiceCompletionTaskSettings instance,
        TransportVersion version
    ) {
        if (version.supports(ElasticInferenceServiceCompletionTaskSettings.EIS_REASONING_TASK_SETTINGS_ADDED) == false) {
            return new ElasticInferenceServiceCompletionTaskSettings((Reasoning) null);
        }
        return instance;
    }

    @Override
    protected ElasticInferenceServiceCompletionTaskSettings fromMutableMap(Map<String, Object> mutableMap) {
        return ElasticInferenceServiceCompletionTaskSettings.fromMap(
            mutableMap,
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.PERSISTENT
        );
    }

    // ---------- isEmpty ----------

    public void testIsEmpty_WhenReasoningIsNull() {
        var settings = new ElasticInferenceServiceCompletionTaskSettings((Reasoning) null);
        assertTrue(settings.isEmpty());
    }

    public void testIsEmpty_WhenReasoningIsPresent() {
        var settings = new ElasticInferenceServiceCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
        assertFalse(settings.isEmpty());
    }

    // ---------- fromMap ----------

    public void testFromMap_ParsesValidReasoning() {
        var map = Map.<String, Object>of(REASONING_FIELD, Map.of("effort", "medium", "summary", "detailed"));
        var settings = ElasticInferenceServiceCompletionTaskSettings.fromMap(
            map,
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.REQUEST
        );
        assertThat(settings.reasoning(), is(MEDIUM_DETAILED_REASONING));
    }

    public void testFromMap_EmptyMap_ReturnsEmptySettings() {
        var settings = ElasticInferenceServiceCompletionTaskSettings.fromMap(
            new HashMap<>(),
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.REQUEST
        );
        assertTrue(settings.isEmpty());
    }

    public void testFromMap_NullMap_ReturnsEmptySettings() {
        var settings = ElasticInferenceServiceCompletionTaskSettings.fromMap(
            null,
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.REQUEST
        );
        assertTrue(settings.isEmpty());
    }

    public void testFromMap_InvalidEffort_Throws() {
        var map = Map.<String, Object>of(REASONING_FIELD, Map.of("effort", "super-high"));
        expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceCompletionTaskSettings.fromMap(map, TaskType.CHAT_COMPLETION, ConfigurationParseContext.REQUEST)
        );
    }

    public void testFromMap_NeitherEffortNorEnabled_Throws() {
        // summary alone does not satisfy the at-least-one-of rule enforced by Reasoning.PARSER
        var map = Map.<String, Object>of(REASONING_FIELD, Map.of("summary", "concise"));
        var ex = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceCompletionTaskSettings.fromMap(map, TaskType.CHAT_COMPLETION, ConfigurationParseContext.REQUEST)
        );
        // The outer parser wraps the Reasoning.PARSER exception — check the cause
        assertThat(ex.getCause().getMessage(), containsString("Required one of fields [effort, enabled], but none were specified."));
    }

    public void testFromMap_EnabledFalseWithoutEffort_Throws() {
        // Reasoning.validateFields enforces: when enabled=false, effort must be specified.
        // The XContentParseException wraps the cause via ConstructingObjectParser's "Failed to build" layer;
        // we verify only that parsing fails (not the nested message, which is tested in ReasoningTests).
        var map = Map.<String, Object>of(REASONING_FIELD, Map.of("enabled", false));
        expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceCompletionTaskSettings.fromMap(map, TaskType.CHAT_COMPLETION, ConfigurationParseContext.REQUEST)
        );
    }

    public void testFromMap_NonChatCompletionTaskType_WithReasoning_Throws() {
        var map = Map.<String, Object>of(REASONING_FIELD, Map.of("effort", "low"));
        var ex = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceCompletionTaskSettings.fromMap(map, TaskType.COMPLETION, ConfigurationParseContext.REQUEST)
        );
        assertThat(ex.getMessage(), containsString("[reasoning] is only supported for the [chat_completion] task type"));
    }

    public void testFromMap_UnknownTopLevelField_StrictParser_Throws() {
        var map = Map.<String, Object>of(REASONING_FIELD, Map.of("effort", "medium"), "unknown_field", "value");
        expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceCompletionTaskSettings.fromMap(map, TaskType.CHAT_COMPLETION, ConfigurationParseContext.REQUEST)
        );
    }

    public void testFromMap_UnknownTopLevelField_LenientParser_Succeeds() {
        var map = Map.<String, Object>of(REASONING_FIELD, Map.of("effort", "medium"), "unknown_field", "value");
        var settings = ElasticInferenceServiceCompletionTaskSettings.fromMap(
            map,
            TaskType.CHAT_COMPLETION,
            ConfigurationParseContext.PERSISTENT
        );
        assertNotNull(settings.reasoning());
        assertThat(settings.reasoning().effort(), is(ReasoningEffort.MEDIUM));
    }

    // ---------- toXContent ----------

    public void testToXContent_WithReasoning_ProducesExpectedJson() throws IOException {
        var settings = new ElasticInferenceServiceCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
        assertThat(Strings.toString(settings), is(XContentHelper.stripWhitespace("""
            {
              "reasoning": {
                "effort": "medium",
                "summary": "detailed"
              }
            }
            """)));
    }

    public void testToXContent_Empty_ProducesEmptyObject() throws IOException {
        var settings = new ElasticInferenceServiceCompletionTaskSettings((Reasoning) null);
        assertThat(toMap(settings), is(Map.of()));
    }

    // ---------- updatedTaskSettings ----------

    public void testUpdatedTaskSettings_BodyWins_WhenNewSettingsHaveReasoning() {
        var stored = new ElasticInferenceServiceCompletionTaskSettings(LOW_REASONING);
        var updated = stored.updatedTaskSettings(Map.of(REASONING_FIELD, Map.of("effort", "high")));
        assertThat(((ElasticInferenceServiceCompletionTaskSettings) updated).reasoning().effort(), is(ReasoningEffort.HIGH));
    }

    public void testUpdatedTaskSettings_StoredKept_WhenNewSettingsHaveNoReasoning() {
        var stored = new ElasticInferenceServiceCompletionTaskSettings(MEDIUM_DETAILED_REASONING);
        var updated = stored.updatedTaskSettings(new HashMap<>());
        assertThat(updated, is(stored));
    }

    public void testUpdatedTaskSettings_NullMap_ReturnsSelf() {
        var stored = new ElasticInferenceServiceCompletionTaskSettings(HIGH_REASONING);
        var updated = stored.updatedTaskSettings(null);
        assertThat(updated, is(stored));
    }

    // ---------- mergeReasoning ----------

    public void testMergeReasoning_BodyWins_WhenBodyNonNull() {
        var merged = ElasticInferenceServiceCompletionTaskSettings.mergeReasoning(HIGH_REASONING, LOW_REASONING);
        assertThat(merged, is(HIGH_REASONING));
    }

    public void testMergeReasoning_StoredUsed_WhenBodyNull() {
        var merged = ElasticInferenceServiceCompletionTaskSettings.mergeReasoning(null, LOW_REASONING);
        assertThat(merged, is(LOW_REASONING));
    }

    public void testMergeReasoning_NullBoth_ReturnsNull() {
        assertNull(ElasticInferenceServiceCompletionTaskSettings.mergeReasoning(null, null));
    }

    // ---------- helpers ----------

    /**
     * Creates a random {@link ElasticInferenceServiceCompletionTaskSettings} — either empty or
     * with a randomly generated {@link Reasoning} that satisfies the Reasoning validation rules.
     */
    public static ElasticInferenceServiceCompletionTaskSettings randomTaskSettings() {
        if (randomBoolean()) {
            return new ElasticInferenceServiceCompletionTaskSettings((Reasoning) null);
        }
        return new ElasticInferenceServiceCompletionTaskSettings(randomReasoning());
    }

    static Reasoning randomReasoning() {
        var effort = randomBoolean() ? randomFrom(ReasoningEffort.values()) : null;
        Boolean enabled;
        if (effort == null) {
            // at-least-one-of rule: enabled must be present (and != false) when effort is absent
            enabled = true;
        } else {
            enabled = randomBoolean() ? randomBoolean() : null;
        }
        return new Reasoning(
            effort,
            randomBoolean() ? randomFrom(ReasoningSummary.values()) : null,
            randomBoolean() ? randomBoolean() : null,
            enabled
        );
    }
}
