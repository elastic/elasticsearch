/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionModel;
import org.elasticsearch.xpack.inference.services.huggingface.request.completion.HuggingFaceUnifiedChatCompletionRequestEntity;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.xpack.inference.Utils.assertJsonEquals;
import static org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionModelTests.createCompletionModel;

public class HuggingFaceUnifiedChatCompletionRequestEntityTests extends ESTestCase {

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
        HuggingFaceChatCompletionModel model = createCompletionModel("test-url", "api-key", "test-endpoint");

        HuggingFaceUnifiedChatCompletionRequestEntity entity = new HuggingFaceUnifiedChatCompletionRequestEntity(unifiedChatInput, model);

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
}
