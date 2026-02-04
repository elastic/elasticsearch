/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.huggingface.request.completion.HuggingFaceUnifiedChatCompletionRequestEntity;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.xpack.inference.Utils.assertJsonEquals;

public class HuggingFaceUnifiedChatCompletionRequestEntityTests extends ESTestCase {

    private static final String ROLE = "user";

    public void testSerializationWithModelIdStreaming() throws IOException {
        testSerialization("test-endpoint", true, """
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
            """);
    }

    public void testSerializationWithModelIdNonStreaming() throws IOException {
        testSerialization("test-endpoint", false, """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "test-endpoint",
                "n": 1,
                "stream": false
            }
            """);
    }

    public void testSerializationWithoutModelIdStreaming() throws IOException {
        testSerialization(null, true, """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }
            """);
    }

    public void testSerializationWithoutModelIdNonStreaming() throws IOException {
        testSerialization(null, false, """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "n": 1,
                "stream": false
            }
            """);
    }

    private static void testSerialization(String modelId, boolean isStreaming, String expectedJson) throws IOException {
        UnifiedCompletionRequest.Message message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            ROLE,
            null,
            null
        );
        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequest.of(messageList);

        UnifiedChatInput unifiedChatInput = new UnifiedChatInput(unifiedRequest, isStreaming);

        HuggingFaceUnifiedChatCompletionRequestEntity entity = new HuggingFaceUnifiedChatCompletionRequestEntity(unifiedChatInput, modelId);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String jsonString = Strings.toString(builder);
        assertJsonEquals(jsonString, XContentHelper.stripWhitespace(expectedJson));
    }
}
