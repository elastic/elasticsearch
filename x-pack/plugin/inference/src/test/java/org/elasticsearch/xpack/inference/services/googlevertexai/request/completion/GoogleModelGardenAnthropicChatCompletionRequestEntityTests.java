/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.UnifiedCompletionRequest.ContentString;
import org.elasticsearch.inference.UnifiedCompletionRequest.Message;
import org.elasticsearch.inference.UnifiedCompletionRequest.Tool;
import org.elasticsearch.inference.UnifiedCompletionRequest.ToolChoice;
import org.elasticsearch.inference.UnifiedCompletionRequest.ToolChoiceObject;
import org.elasticsearch.inference.UnifiedCompletionRequest.ToolChoiceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class GoogleModelGardenAnthropicChatCompletionRequestEntityTests extends ESTestCase {

    private static final String USER_MESSAGE_CONTENT = "Hello, world!";
    private static final String ANTHROPIC_VERSION = "vertex-2023-10-16";
    private static final String TOOL_NAME = "name";
    private static final String TOOL_DESCRIPTION = "description";
    private static final Map<String, Object> TOOL_PARAMETERS = Map.of(
        "type",
        "object",
        "properties",
        Map.of("item", Map.of("type", "string"))
    );

    private static final GoogleVertexAiChatCompletionTaskSettings EMPTY_SETTINGS = GoogleVertexAiChatCompletionTaskSettings.EMPTY_SETTINGS;

    // OpenAI-shaped tool_choice object {"type":"function","function":{"name":"function_name"}} must be translated to Anthropic's
    // {"type":"tool","name":"function_name"} form, otherwise Anthropic rejects the request with a 400 (unexpected tag 'function').
    private static final ToolChoice FUNCTION_TOOL_CHOICE = new ToolChoiceObject("function", new ToolChoiceObject.FunctionField(TOOL_NAME));
    private static final Tool TOOL = new Tool("function", new Tool.FunctionField(TOOL_DESCRIPTION, TOOL_NAME, TOOL_PARAMETERS, null));

    private static final String TOOL_CHOICE_OBJECT_JSON = Strings.format("""
        {
            "type": "tool",
            "name": "%s"
        }""", TOOL_NAME);

    public void testModelUserFieldsSerializationStreamingWithTemperatureAndTopK() throws IOException {
        var actual = render(FUNCTION_TOOL_CHOICE, List.of(TOOL), 0.2F, 0.2F, 100L, true, EMPTY_SETTINGS);
        assertThat(actual, is(expectedRequestJson(0.2F, TOOL_CHOICE_OBJECT_JSON, 0.2F, true, 100)));
    }

    public void testModelUserFieldsSerializationNonStreamDefaultMaxTokens() throws IOException {
        var actual = render(FUNCTION_TOOL_CHOICE, List.of(TOOL), null, null, null, false, EMPTY_SETTINGS);
        assertThat(actual, is(expectedRequestJson(null, TOOL_CHOICE_OBJECT_JSON, null, false, 1024)));
    }

    public void testModelUserFieldsSerializationNonStreamWithMaxTokensFromTaskSettings() throws IOException {
        var actual = render(
            FUNCTION_TOOL_CHOICE,
            List.of(TOOL),
            null,
            null,
            null,
            false,
            new GoogleVertexAiChatCompletionTaskSettings(new ThinkingConfig(123), 123)
        );
        assertThat(actual, is(expectedRequestJson(null, TOOL_CHOICE_OBJECT_JSON, null, false, 123)));
    }

    public void testToolChoiceStringAutoIsTranslated() throws IOException {
        assertToolChoiceStringTranslation("auto", "auto");
    }

    public void testToolChoiceStringNoneIsTranslated() throws IOException {
        assertToolChoiceStringTranslation("none", "none");
    }

    public void testToolChoiceStringRequiredIsTranslatedToAny() throws IOException {
        assertToolChoiceStringTranslation("required", "any");
    }

    public void testUnsupportedToolChoiceStringThrows() {
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> render(new ToolChoiceString("unsupported"), List.of(TOOL), null, null, null, false, EMPTY_SETTINGS)
        );
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), is("Unsupported tool_choice value [unsupported] for the Anthropic chat completion API."));
    }

    public void testStrictToolFieldIsIgnored() throws IOException {
        // The OpenAI "strict" field has no Anthropic equivalent, so it is silently dropped: the rendered request is identical to
        // one built from the same tool without "strict".
        var strictTool = new Tool("function", new Tool.FunctionField(TOOL_DESCRIPTION, TOOL_NAME, TOOL_PARAMETERS, randomBoolean()));
        var actual = render(new ToolChoiceString("auto"), List.of(strictTool), null, null, null, false, EMPTY_SETTINGS);
        var toolChoiceJson = """
            {
                "type": "auto"
            }""";
        assertThat(actual, is(expectedRequestJson(null, toolChoiceJson, null, false, 1024)));
    }

    private static void assertToolChoiceStringTranslation(String openAiValue, String anthropicType) throws IOException {
        var actual = render(new ToolChoiceString(openAiValue), List.of(TOOL), null, null, null, false, EMPTY_SETTINGS);
        var toolChoiceJson = Strings.format("""
            {
                "type": "%s"
            }""", anthropicType);
        assertThat(actual, is(expectedRequestJson(null, toolChoiceJson, null, false, 1024)));
    }

    /**
     * Builds the expected Anthropic-shaped request body in the field order produced by the entity. {@code temperature} and
     * {@code topP} segments collapse away when {@code null}, mirroring the optional fields in the real request.
     */
    private static String expectedRequestJson(Float temperature, String toolChoiceJson, Float topP, boolean stream, long maxTokens)
        throws IOException {
        return XContentHelper.stripWhitespace(
            Strings.format(
                """
                    {
                        "anthropic_version": "%s",
                        "messages": [
                            {
                                "content": "%s",
                                "role": "user"
                            }
                        ],
                        %s
                        "tool_choice": %s,
                        "tools": [
                            {
                                "name": "%s",
                                "description": "%s",
                                "input_schema": {
                                    "type": "object",
                                    "properties": {
                                        "item": {
                                            "type": "string"
                                        }
                                    }
                                }
                            }
                        ],
                        %s
                        "stream": %s,
                        "max_tokens": %s
                    }
                    """,
                ANTHROPIC_VERSION,
                USER_MESSAGE_CONTENT,
                temperature == null ? "" : Strings.format("\"temperature\": %s,", temperature),
                toolChoiceJson,
                TOOL_NAME,
                TOOL_DESCRIPTION,
                topP == null ? "" : Strings.format("\"top_p\": %s,", topP),
                stream,
                maxTokens
            )
        );
    }

    private static String render(
        ToolChoice toolChoice,
        List<Tool> tools,
        Float topP,
        Float temperature,
        Long maxCompletionTokens,
        boolean stream,
        GoogleVertexAiChatCompletionTaskSettings taskSettings
    ) throws IOException {
        var unifiedRequest = new UnifiedCompletionRequest(
            List.of(new Message(new ContentString(USER_MESSAGE_CONTENT), "user", null, null)),
            null,
            maxCompletionTokens,
            null,
            temperature,
            toolChoice,
            tools,
            topP
        );
        var entity = new GoogleModelGardenAnthropicChatCompletionRequestEntity(unifiedRequest, stream, taskSettings);
        try (var builder = JsonXContent.contentBuilder()) {
            entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder);
        }
    }
}
