/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.request.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModel;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModelTests;

import java.io.IOException;
import java.util.ArrayList;

public class LlamaChatCompletionRequestEntityTests extends ESTestCase {
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
        LlamaChatCompletionModel model = LlamaChatCompletionModelTests.createChatCompletionModel("model", "url", "api-key");

        LlamaChatCompletionRequestEntity entity = new LlamaChatCompletionRequestEntity(unifiedChatInput, model);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String expectedJson = """
            {
                "messages": [{
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "model",
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """;
        assertEquals(XContentHelper.stripWhitespace(expectedJson), Strings.toString(builder));
    }

}
