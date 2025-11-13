/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.completion;

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

public class OpenShiftAiChatCompletionRequestEntityTests extends ESTestCase {
    private static final String ROLE_VALUE = "user";

    public void testSerializationWithModelIdStreaming() throws IOException {
        testSerialization("modelId", true, """
            {
                "messages": [{
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "modelId",
                "n": 1,
                "stream": true
            }
            """);
    }

    public void testSerializationWithModelIdNonStreaming() throws IOException {
        testSerialization("modelId", false, """
            {
                "messages": [{
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "model": "modelId",
                "n": 1,
                "stream": false
            }
            """);
    }

    public void testSerializationWithoutModelIdStreaming() throws IOException {
        testSerialization(null, true, """
            {
                "messages": [{
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "n": 1,
                "stream": true
            }
            """);
    }

    public void testSerializationWithoutModelIdNonStreaming() throws IOException {
        testSerialization(null, false, """
            {
                "messages": [{
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
        var message = new UnifiedCompletionRequest.Message(
            new UnifiedCompletionRequest.ContentString("Hello, world!"),
            ROLE_VALUE,
            null,
            null
        );

        var messageList = new ArrayList<UnifiedCompletionRequest.Message>();
        messageList.add(message);

        var unifiedRequest = UnifiedCompletionRequest.of(messageList);
        var unifiedChatInput = new UnifiedChatInput(unifiedRequest, isStreaming);

        var entity = new OpenShiftAiChatCompletionRequestEntity(unifiedChatInput, modelId);

        XContentBuilder builder = JsonXContent.contentBuilder();
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace(expectedJson)));
    }

}
