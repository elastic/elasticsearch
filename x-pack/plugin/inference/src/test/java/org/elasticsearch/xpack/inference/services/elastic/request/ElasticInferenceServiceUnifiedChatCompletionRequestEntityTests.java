/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModelTests;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.request.OpenAiUnifiedChatCompletionRequestEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.inference.Utils.assertJsonEquals;
import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests.createCompletionModel;

public class ElasticInferenceServiceUnifiedChatCompletionRequestEntityTests extends ESTestCase {

    private static final String ROLE = "user";

    public void testModelUserFieldsSerialization() throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            ROLE,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequest.of(messageList);

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, true);
        OpenAiChatCompletionModel model = createCompletionModel("test-url", "organizationId", "api-key", "test-endpoint", null);

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
                }
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_NonStreaming_ForCompletion() throws IOException {
        // Test non-streaming case (used for COMPLETION task type)
        var unifiedChatInput = new UnifiedChatInput(List.of("What is 2+2?"), ROLE, false);
        var model = ElasticInferenceServiceCompletionModelTests.createModel("http://eis-gateway.com", "my-model-id");
        var entity = new ElasticInferenceServiceUnifiedChatCompletionRequestEntity(unifiedChatInput, model.getServiceSettings().modelId());

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "What is 2+2?",
                        "role": "user"
                    }
                ],
                "model": "my-model-id",
                "n": 1,
                "stream": false
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_MultipleInputs_NonStreaming() throws IOException {
        // Test multiple inputs converted to messages (used for COMPLETION task type)
        var unifiedChatInput = new UnifiedChatInput(List.of("What is 2+2?", "What is the capital of France?"), ROLE, false);
        var model = ElasticInferenceServiceCompletionModelTests.createModel("http://eis-gateway.com", "my-model-id");
        var entity = new ElasticInferenceServiceUnifiedChatCompletionRequestEntity(unifiedChatInput, model.getServiceSettings().modelId());

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "What is 2+2?",
                        "role": "user"
                    },
                    {
                        "content": "What is the capital of France?",
                        "role": "user"
                    }
                ],
                "model": "my-model-id",
                "n": 1,
                "stream": false
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_EmptyInput_NonStreaming() throws IOException {
        var unifiedChatInput = new UnifiedChatInput(List.of(""), ROLE, false);
        var model = ElasticInferenceServiceCompletionModelTests.createModel("http://eis-gateway.com", "my-model-id");
        var entity = new ElasticInferenceServiceUnifiedChatCompletionRequestEntity(unifiedChatInput, model.getServiceSettings().modelId());

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "",
                        "role": "user"
                    }
                ],
                "model": "my-model-id",
                "n": 1,
                "stream": false
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_AlwaysSetsNToOne_NonStreaming() throws IOException {
        // Verify n is always 1 regardless of number of inputs
        var unifiedChatInput = new UnifiedChatInput(List.of("input1", "input2", "input3"), ROLE, false);
        var model = ElasticInferenceServiceCompletionModelTests.createModel("http://eis-gateway.com", "my-model-id");
        var entity = new ElasticInferenceServiceUnifiedChatCompletionRequestEntity(unifiedChatInput, model.getServiceSettings().modelId());

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "input1",
                        "role": "user"
                    },
                    {
                        "content": "input2",
                        "role": "user"
                    },
                    {
                        "content": "input3",
                        "role": "user"
                    }
                ],
                "model": "my-model-id",
                "n": 1,
                "stream": false
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }

    public void testSerialization_AllMessagesHaveUserRole_NonStreaming() throws IOException {
        // Verify all messages have "user" role when converting from simple inputs
        var unifiedChatInput = new UnifiedChatInput(List.of("first", "second", "third"), ROLE, false);
        var model = ElasticInferenceServiceCompletionModelTests.createModel("http://eis-gateway.com", "test-model");
        var entity = new ElasticInferenceServiceUnifiedChatCompletionRequestEntity(unifiedChatInput, model.getServiceSettings().modelId());

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        String expectedJson = """
            {
                "messages": [
                    {
                        "content": "first",
                        "role": "user"
                    },
                    {
                        "content": "second",
                        "role": "user"
                    },
                    {
                        "content": "third",
                        "role": "user"
                    }
                ],
                "model": "test-model",
                "n": 1,
                "stream": false
            }
            """;
        assertJsonEquals(jsonString, expectedJson);
    }
}
