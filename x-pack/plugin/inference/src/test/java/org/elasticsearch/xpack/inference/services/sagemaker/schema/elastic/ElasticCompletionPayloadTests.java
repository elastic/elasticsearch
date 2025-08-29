/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import software.amazon.awssdk.core.SdkBytes;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class ElasticCompletionPayloadTests extends ElasticPayloadTestCase<ElasticCompletionPayload> {
    @Override
    protected ElasticCompletionPayload payload() {
        return new ElasticCompletionPayload();
    }

    @Override
    protected Set<TaskType> expectedSupportedTaskTypes() {
        return Set.of(TaskType.CHAT_COMPLETION, TaskType.COMPLETION);
    }

    public void testNonStreamingResponse() throws Exception {
        var responseJson = """
            {
                "completion": [
                    {
                        "result": "hello"
                    }
                ]
            }
            """;

        var chatCompletionResults = payload.responseBody(mockModel(), invokeEndpointResponse(responseJson));

        assertThat(chatCompletionResults.getResults().size(), is(1));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("hello"));
    }

    public void testStreamingResponse() throws Exception {
        var responseJson = """
            {
                "completion": [
                    {
                        "delta": "hola"
                    }
                ]
            }
            """;

        var chatCompletionResults = payload.streamResponseBody(mockModel(), SdkBytes.fromUtf8String(responseJson));

        assertThat(chatCompletionResults.results().size(), is(1));
        assertThat(chatCompletionResults.results().iterator().next().delta(), is("hola"));
    }

    public void testChatCompletionRequest() throws Exception {
        var message = new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("Hello, world!"), "user", null, null);
        var unifiedRequest = new UnifiedCompletionRequest(
            List.of(message),
            "i am ignored",
            10L,
            List.of("right meow"),
            1.0F,
            null,
            null,
            null
        );
        var sdkBytes = payload.chatCompletionRequestBytes(mockModel(), unifiedRequest);
        assertJsonSdkBytes(sdkBytes, """
            {
                "messages": [
                    {
                        "content": "Hello, world!",
                        "role": "user"
                    }
                ],
                "stop": [
                    "right meow"
                ],
                "temperature": 1.0,
                "max_completion_tokens": 10
            }
            """);
    }

    public void testChatCompletionResponse() throws Exception {
        var responseJson = """
                        {
                          "id": "chunk1",
                          "choices": [
                            {
                              "delta": {
                                "content": "example_content",
                                "refusal": "example_refusal",
                                "role": "assistant",
                                "tool_calls": [
                                  {
                                    "index": 1,
                                    "id": "tool1",
                                    "function": {
                                      "arguments": "example_arguments",
                                      "name": "example_function"
                                    },
                                    "type": "function"
                                  }
                                ]
                              },
                              "finish_reason": "example_reason",
                              "index": 0
                            }
                          ],
                          "model": "example_model",
                          "object": "example_object",
                          "usage": {
                            "completion_tokens": 10,
                            "prompt_tokens": 5,
                            "total_tokens": 15
                          }
                        }
            """;

        var chatCompletionResponse = payload.chatCompletionResponseBody(mockModel(), SdkBytes.fromUtf8String(responseJson));

        XContentBuilder builder = JsonXContent.contentBuilder();
        chatCompletionResponse.toXContentChunked(null).forEachRemaining(xContent -> {
            try {
                xContent.toXContent(builder, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(XContentHelper.stripWhitespace(responseJson), Strings.toString(builder).trim());
    }
}
