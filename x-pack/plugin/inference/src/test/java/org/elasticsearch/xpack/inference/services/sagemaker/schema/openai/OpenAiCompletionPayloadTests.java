/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.openai;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemaPayloadTestCase;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenAiCompletionPayloadTests extends SageMakerSchemaPayloadTestCase<OpenAiCompletionPayload> {
    @Override
    protected OpenAiCompletionPayload payload() {
        return new OpenAiCompletionPayload();
    }

    @Override
    protected String expectedApi() {
        return "openai";
    }

    @Override
    protected Set<TaskType> expectedSupportedTaskTypes() {
        return Set.of(TaskType.CHAT_COMPLETION, TaskType.COMPLETION);
    }

    @Override
    protected SageMakerStoredServiceSchema randomApiServiceSettings() {
        return SageMakerStoredServiceSchema.NO_OP;
    }

    @Override
    protected SageMakerStoredTaskSchema randomApiTaskSettings() {
        return SageMakerOpenAiTaskSettingsTests.randomApiTaskSettings();
    }

    public void testRequest() throws Exception {
        var sdkByes = payload.requestBytes(mockModel("coolPerson"), request(false));
        assertJsonSdkBytes(sdkByes, """
            {
                "messages": [
                    {
                        "content": "hello",
                        "role": "user"
                    }
                ],
                "n": 1,
                "stream": false,
                "user": "coolPerson"
            }""");
    }

    public void testRequestWithoutUser() throws Exception {
        var sdkByes = payload.requestBytes(mockModel(null), request(false));
        assertJsonSdkBytes(sdkByes, """
            {
                "messages": [
                    {
                        "content": "hello",
                        "role": "user"
                    }
                ],
                "n": 1,
                "stream": false
            }""");
    }

    public void testStreamRequest() throws Exception {
        var sdkByes = payload.requestBytes(mockModel("user"), request(true));
        assertJsonSdkBytes(sdkByes, """
            {
                "messages":[
                    {
                        "content": "hello",
                        "role": "user"
                    }
                ],
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                },
                "user": "user"
            }""");
    }

    public void testStreamRequestWithoutUser() throws Exception {
        var sdkByes = payload.requestBytes(mockModel(null), request(true));
        assertJsonSdkBytes(sdkByes, """
            {
                "messages":[
                    {
                        "content": "hello",
                        "role": "user"
                    }
                ],
                "n": 1,
                "stream": true,
                "stream_options": {
                    "include_usage": true
                }
            }""");
    }

    private SageMakerInferenceRequest request(boolean stream) {
        return new SageMakerInferenceRequest(null, null, null, List.of("hello"), stream, InputType.UNSPECIFIED);
    }

    private SageMakerModel mockModel(String user) {
        SageMakerModel model = mock();
        when(model.apiTaskSettings()).thenReturn(new SageMakerOpenAiTaskSettings(user));
        return model;
    }

    public void testResponse() throws Exception {
        var responseJson = """
            {
              "id": "some-id",
              "object": "chat.completion",
              "created": 1705397787,
              "model": "gpt-3.5-turbo-0613",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "result"
                  },
                  "logprobs": null,
                  "finish_reason": "stop"
                }
              ],
              "usage": {
                "prompt_tokens": 46,
                "completion_tokens": 39,
                "total_tokens": 85
              },
              "system_fingerprint": null
            }
            """;

        var chatCompletionResults = (ChatCompletionResults) payload.responseBody(
            mockModel(),
            InvokeEndpointResponse.builder().body(SdkBytes.fromUtf8String(responseJson)).build()
        );

        assertThat(chatCompletionResults.getResults().size(), is(1));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("result"));
    }

    public void testStreamResponse() throws Exception {
        var responseJson = dataPayload("""
            {
                "id":"12345",
                "object":"chat.completion.chunk",
                "created":123456789,
                "model":"gpt-4o-mini",
                "system_fingerprint": "123456789",
                "choices":[
                    {
                        "index":0,
                        "delta":{
                            "content":"test"
                        },
                        "logprobs":null,
                        "finish_reason":null
                    }
                ]
            }
            """);

        var streamingResults = payload.streamResponseBody(mockModel(), responseJson);

        assertThat(streamingResults.results().size(), is(1));
        assertThat(streamingResults.results().iterator().next().delta(), is("test"));
    }

    private SdkBytes dataPayload(String json) throws IOException {
        return SdkBytes.fromUtf8String("data: " + XContentHelper.stripWhitespace(json) + "\n\n");
    }

    private SageMakerModel mockModel() {
        SageMakerModel model = mock();
        when(model.apiTaskSettings()).thenReturn(randomApiTaskSettings());
        return model;
    }

    public void testChatCompletionRequest() throws Exception {
        var message = new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("Hello, world!"), "user", null, null);
        var unifiedRequest = new UnifiedCompletionRequest(List.of(message), null, null, null, null, null, null, null);
        var sdkBytes = payload.chatCompletionRequestBytes(mockModel("coolUser"), unifiedRequest);
        assertJsonSdkBytes(sdkBytes, """
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
                },
                "user": "coolUser"
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

        var chatCompletionResponse = payload.chatCompletionResponseBody(mockModel(), dataPayload(responseJson));

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
