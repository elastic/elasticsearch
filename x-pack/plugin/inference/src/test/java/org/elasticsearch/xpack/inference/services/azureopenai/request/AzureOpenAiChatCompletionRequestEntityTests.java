/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.Matchers.is;

public class AzureOpenAiChatCompletionRequestEntityTests extends ESTestCase {
    // Test values
    private static final String ROLE_VALUE = "user";
    private static final String INPUT_VALUE = "Hello, world!";
    private static final String USER_VALUE = "some_user";

    public void testSerializationWithModelIdStreamingWithUser() throws IOException {
        boolean isStreaming = true;
        testSerialization(isStreaming, USER_VALUE, Strings.format("""
            {
                "messages": [{
                        "content": "%s",
                        "role": "%s"
                    }
                ],
                "n": 1,
                "stream": %b,
                "stream_options": {
                    "include_usage": true
                },
                "user": "%s"
            }
            """, INPUT_VALUE, ROLE_VALUE, isStreaming, USER_VALUE));
    }

    public void testSerializationWithModelIdStreamingNoUser() throws IOException {
        boolean isStreaming = true;
        testSerialization(isStreaming, null, Strings.format("""
            {
                "messages": [{
                        "content": "%s",
                        "role": "%s"
                    }
                ],
                "n": 1,
                "stream": %b,
                "stream_options": {
                    "include_usage": true
                }
            }
            """, INPUT_VALUE, ROLE_VALUE, isStreaming));
    }

    public void testSerializationWithModelIdNonStreamingWithUser() throws IOException {
        boolean isStreaming = false;
        testSerialization(isStreaming, USER_VALUE, Strings.format("""
            {
                "messages": [{
                        "content": "%s",
                        "role": "%s"
                    }
                ],
                "n": 1,
                "stream": %b,
                "user": "%s"
            }
            """, INPUT_VALUE, ROLE_VALUE, isStreaming, USER_VALUE));
    }

    public void testSerializationWithModelIdNonStreamingNoUser() throws IOException {
        boolean isStreaming = false;
        testSerialization(isStreaming, null, Strings.format("""
            {
                "messages": [{
                        "content": "%s",
                        "role": "%s"
                    }
                ],
                "n": 1,
                "stream": %b
            }
            """, INPUT_VALUE, ROLE_VALUE, isStreaming));
    }

    public void testCreateRequestEntity_NoInput_ThrowsException() {
        assertThrows(NullPointerException.class, () -> new AzureOpenAiChatCompletionRequestEntity(null, USER_VALUE));
    }

    private static void testSerialization(boolean isStreaming, String userValue, String expectedJson) throws IOException {
        var message = new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString(INPUT_VALUE), ROLE_VALUE, null, null);

        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequest.of(messageList);
        var unifiedChatInput = new UnifiedChatInput(unifiedRequest, isStreaming);

        var entity = new AzureOpenAiChatCompletionRequestEntity(unifiedChatInput, userValue);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace(expectedJson)));
    }

}
