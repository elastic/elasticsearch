/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.request;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.inference.completion.Reasoning.ReasoningEffort;
import static org.elasticsearch.xpack.inference.Utils.assertJsonEquals;
import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests.createChatCompletionModel;
import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests.createCompletionModel;
import static org.hamcrest.Matchers.containsString;

public class OpenAiUnifiedChatCompletionRequestEntityTests extends ESTestCase {

    private static final String ROLE = "user";
    private static final String USER = "a_user";

    public void testModelUserFieldsSerialization() throws IOException {
        Message message = new Message(new ContentString("Hello, world!"), ROLE, null, null);
        var messageList = new ArrayList<Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequest.of(messageList);

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);
        OpenAiChatCompletionModel model = createCompletionModel("test-url", "organizationId", "api-key", "test-endpoint", USER);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "test-endpoint",
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                },
                "user": "a_user"
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testReasoningEffort_IsMappedToOpenAiReasoningEffort() throws IOException {
        Message message = new Message(new ContentString("Hello, world!"), ROLE, null, null);
        var reasoning = new Reasoning(ReasoningEffort.NONE, null, null, null);
        var unifiedRequest = new UnifiedCompletionRequest(
            java.util.List.of(message),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            reasoning,
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, false);
        OpenAiChatCompletionModel model = createChatCompletionModel("test-url", "organizationId", "api-key", "gpt-5.6", USER);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "gpt-5.6",
                "n": 1,
                "stream": false,
                "user": "a_user",
                "reasoning_effort": "none"
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testReasoningEffort_WithoutEffort_Throws() {
        var reasoning = new Reasoning(null, Reasoning.ReasoningSummary.DETAILED, null, true);
        var exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> OpenAiUnifiedChatCompletionRequestEntity.writeReasoningEffort(JsonXContent.contentBuilder(), reasoning)
        );
        assertThat(exception.getMessage(), containsString("requires [reasoning.effort]"));
    }

    public void testMaxCompletionTokens_IsSerialized() throws IOException {
        Message message = new Message(new ContentString("Hello, world!"), ROLE, null, null);
        var unifiedRequest = new UnifiedCompletionRequest(
            java.util.List.of(message),
            null,
            128L,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, false);
        OpenAiChatCompletionModel model = createChatCompletionModel("test-url", "organizationId", "api-key", "gpt-5.6", USER);

        OpenAiUnifiedChatCompletionRequestEntity entity = new OpenAiUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "max_completion_tokens": 128,
                "model": "gpt-5.6",
                "n": 1,
                "stream": false,
                "user": "a_user"
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }
}
