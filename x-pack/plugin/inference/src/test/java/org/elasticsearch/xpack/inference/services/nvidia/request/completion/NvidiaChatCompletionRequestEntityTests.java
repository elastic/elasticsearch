/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.completion;

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
import static org.mockito.Mockito.mock;

public class NvidiaChatCompletionRequestEntityTests extends ESTestCase {
    // Test values
    private static final String ROLE_VALUE = "user";
    private static final String MODEL_VALUE = "some_model";
    private static final String INPUT_VALUE = "Hello, world!";

    public void testSerializationWithModelIdStreaming() throws IOException {
        boolean isStreaming = true;
        testSerialization(isStreaming, Strings.format("""
            {
                "messages": [{
                        "content": "%s",
                        "role": "%s"
                    }
                ],
                "model": "%s",
                "n": 1,
                "stream": %b
            }
            """, INPUT_VALUE, ROLE_VALUE, MODEL_VALUE, isStreaming));
    }

    public void testSerializationWithModelIdNonStreaming() throws IOException {
        boolean isStreaming = false;
        testSerialization(isStreaming, Strings.format("""
            {
                "messages": [{
                        "content": "%s",
                        "role": "%s"
                    }
                ],
                "model": "%s",
                "n": 1,
                "stream": %b
            }
            """, INPUT_VALUE, ROLE_VALUE, MODEL_VALUE, isStreaming));
    }

    public void testCreateRequestEntity_NoInput_ThrowsException() {
        assertThrows(NullPointerException.class, () -> new NvidiaChatCompletionRequestEntity(null, MODEL_VALUE));
    }

    public void testCreateRequestEntity_NoModelId_ThrowsException() {
        assertThrows(NullPointerException.class, () -> new NvidiaChatCompletionRequestEntity(mock(UnifiedChatInput.class), null));
    }

    private static void testSerialization(boolean isStreaming, String expectedJson) throws IOException {
        var message = new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString(INPUT_VALUE), ROLE_VALUE, null, null);

        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequest.of(messageList);
        var unifiedChatInput = new UnifiedChatInput(unifiedRequest, isStreaming);

        var entity = new NvidiaChatCompletionRequestEntity(unifiedChatInput, MODEL_VALUE);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace(expectedJson)));
    }

}
