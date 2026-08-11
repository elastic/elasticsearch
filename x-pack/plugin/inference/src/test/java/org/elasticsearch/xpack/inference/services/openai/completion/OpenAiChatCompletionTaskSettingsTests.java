/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import static org.elasticsearch.inference.completion.ReasoningTests.randomReasoning;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.EFFORT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionTaskSettings.OPENAI_REASONING_TASK_SETTINGS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class OpenAiChatCompletionTaskSettingsTests extends OpenAiTaskSettingsTests<OpenAiChatCompletionTaskSettings> {

    private static final TransportVersion INFERENCE_API_OPENAI_HEADERS = TransportVersion.fromName("inference_api_openai_headers");

    private static final Reasoning NONE_REASONING = new Reasoning(ReasoningEffort.NONE, null, null, null);
    private static final Reasoning LOW_REASONING = new Reasoning(ReasoningEffort.LOW, null, null, null);

    @Override
    protected Writeable.Reader<OpenAiChatCompletionTaskSettings> instanceReader() {
        return OpenAiChatCompletionTaskSettings::new;
    }

    @Override
    protected OpenAiChatCompletionTaskSettings createTestInstance() {
        // Keep reasoning out of the random BWC instance: writing reasoning to an older
        // transport version throws (covered by testReasoningField_IsNotBackwardsCompatible).
        return createRandom();
    }

    @Override
    protected OpenAiChatCompletionTaskSettings mutateInstance(OpenAiChatCompletionTaskSettings instance) throws IOException {
        if (randomBoolean()) {
            return super.mutateInstance(instance);
        }
        return new OpenAiChatCompletionTaskSettings(
            instance.user(),
            instance.headers(),
            randomValueOtherThan(instance.reasoning(), () -> randomFrom(randomReasoning(), null))
        );
    }

    @Override
    protected OpenAiChatCompletionTaskSettings mutateInstanceForVersion(
        OpenAiChatCompletionTaskSettings instance,
        TransportVersion version
    ) {
        var user = instance.user();
        var headers = version.supports(INFERENCE_API_OPENAI_HEADERS) ? instance.headers() : null;
        var reasoning = version.supports(OPENAI_REASONING_TASK_SETTINGS) ? instance.reasoning() : null;
        return new OpenAiChatCompletionTaskSettings(user, headers, reasoning);
    }

    @Override
    protected OpenAiChatCompletionTaskSettings create(@Nullable String user, @Nullable Map<String, String> headers) {
        return new OpenAiChatCompletionTaskSettings(user, headers);
    }

    @Override
    protected OpenAiChatCompletionTaskSettings createFromMap(@Nullable Map<String, Object> map) {
        return new OpenAiChatCompletionTaskSettings(map);
    }

    public void testFromMap_ParsesReasoning() {
        var settings = OpenAiChatCompletionTaskSettings.fromMap(
            Map.of(REASONING_FIELD, Map.of(EFFORT_FIELD, "none")),
            TaskType.CHAT_COMPLETION
        );
        assertThat(settings.reasoning(), is(NONE_REASONING));
    }

    public void testFromMap_NonChatCompletionTaskType_WithReasoning_Throws() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> OpenAiChatCompletionTaskSettings.fromMap(Map.of(REASONING_FIELD, Map.of(EFFORT_FIELD, "none")), TaskType.COMPLETION)
        );
        assertThat(exception.getMessage(), containsString("is only supported for the [chat_completion] task type"));
    }

    public void testIsEmpty_WhenReasoningIsPresent() {
        assertFalse(new OpenAiChatCompletionTaskSettings(null, null, NONE_REASONING).isEmpty());
    }

    public void testToXContent_WritesReasoning() throws IOException {
        var settings = new OpenAiChatCompletionTaskSettings("user", null, NONE_REASONING);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "user": "user",
                "reasoning": {
                    "effort": "none"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testUpdatedTaskSettings_PresentReasoning_Replaces() {
        var initial = new OpenAiChatCompletionTaskSettings(null, null, NONE_REASONING);
        var updated = initial.updatedTaskSettings(new HashMap<>(Map.of(REASONING_FIELD, Map.of(EFFORT_FIELD, "low"))));
        assertThat(updated.reasoning(), is(LOW_REASONING));
    }

    public void testUpdatedTaskSettings_StoredKept_WhenNewSettingsHaveNoReasoning() {
        var initial = new OpenAiChatCompletionTaskSettings("user", null, NONE_REASONING);
        var updated = initial.updatedTaskSettings(new HashMap<>(Map.of(OpenAiServiceFields.USER, "user2")));
        assertThat(updated.reasoning(), is(NONE_REASONING));
        assertThat(updated.user(), is("user2"));
    }

    public void testUpdatedTaskSettings_ExplicitNullReasoning_ResetsReasoningToNull() {
        var initial = new OpenAiChatCompletionTaskSettings(null, null, NONE_REASONING);
        var update = new HashMap<String, Object>();
        update.put(REASONING_FIELD, null);
        var updated = initial.updatedTaskSettings(update);
        assertThat(updated.reasoning(), nullValue());
    }

    public void testMergeReasoning_BodyWins_WhenBodyNonNull() {
        assertThat(OpenAiChatCompletionTaskSettings.mergeReasoning(NONE_REASONING, LOW_REASONING), is(NONE_REASONING));
    }

    public void testMergeReasoning_StoredUsed_WhenBodyNull() {
        assertThat(OpenAiChatCompletionTaskSettings.mergeReasoning(null, LOW_REASONING), is(LOW_REASONING));
    }

    public void testMergeReasoning_NullBoth_ReturnsNull() {
        assertNull(OpenAiChatCompletionTaskSettings.mergeReasoning(null, null));
    }

    public void testReasoningField_IsNotBackwardsCompatible() throws IOException {
        testSerializationIsNotBackwardsCompatible(
            OPENAI_REASONING_TASK_SETTINGS,
            instance -> instance.reasoning() != null,
            OpenAiChatCompletionTaskSettings.REASONING_FIELD_UNSUPPORTED_MESSAGE
        );
    }
}
