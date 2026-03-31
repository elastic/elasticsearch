/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AzureOpenAiCompletionResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
                 "choices": [
                     {
                         "content_filter_results": {
                             "hate": {
                                 "filtered": false,
                                 "severity": "safe"
                             },
                             "self_harm": {
                                 "filtered": false,
                                 "severity": "safe"
                             },
                             "sexual": {
                                 "filtered": false,
                                 "severity": "safe"
                             },
                             "violence": {
                                 "filtered": false,
                                 "severity": "safe"
                             }
                         },
                         "finish_reason": "stop",
                         "index": 0,
                         "logprobs": null,
                         "message": {
                             "content": "result",
                             "role": "assistant"
                         }
                     }
                 ],
                 "model": "gpt-4",
                 "object": "chat.completion",
                 "prompt_filter_results": [
                     {
                         "prompt_index": 0,
                         "content_filter_results": {
                             "hate": {
                                 "filtered": false,
                                 "severity": "safe"
                             },
                             "self_harm": {
                                 "filtered": false,
                                 "severity": "safe"
                             },
                             "sexual": {
                                 "filtered": false,
                                 "severity": "safe"
                             },
                             "violence": {
                                 "filtered": false,
                                 "severity": "safe"
                             }
                         }
                     }
                 ],
                 "usage": {
                     "completion_tokens": 138,
                     "prompt_tokens": 11,
                     "total_tokens": 149
                 }
             }""";

        ChatCompletionResults chatCompletionResults = AzureOpenAiCompletionResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(chatCompletionResults.getResults().size(), is(1));
        assertThat(chatCompletionResults.getResults().get(0).content(), is("result"));
    }

    public void testFromResponse_FailsWhenChoicesFieldIsNotPresent() {
        String responseJson = """
            {
                "not_choices": [
                            {
                                "finish_reason": "stop",
                                "index": 0,
                                "logprobs": null,
                                "message": {
                                    "content": "response",
                                    "role": "assistant"
                                    }
                                }
                            ],
                            "model": "gpt-4",
                            "object": "chat.completion"
            }""";

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> AzureOpenAiCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [choices] in Azure OpenAI completions response"));
    }

    public void testFromResponse_FailsWhenChoicesFieldIsNotAnArray() {
        String responseJson = """
            {
                "choices": {
                                "finish_reason": "stop",
                                "index": 0,
                                "logprobs": null,
                                "message": {
                                    "content": "response",
                                    "role": "assistant"
                                    }
                            },
                            "model": "gpt-4",
                            "object": "chat.completion"
                            ]
            }""";

        var thrownException = expectThrows(
            ParsingException.class,
            () -> AzureOpenAiCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_OBJECT] but found [FIELD_NAME]")
        );
    }

    public void testFromResponse_FailsWhenMessageDoesNotExist() {
        String responseJson = """
            {
                "choices": [
                            {
                                "finish_reason": "stop",
                                "index": 0,
                                "logprobs": null,
                                "not_message": {
                                    "content": "response",
                                    "role": "assistant"
                                    }
                                }
                            ],
                            "model": "gpt-4",
                            "object": "chat.completion"
            }""";

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> AzureOpenAiCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [message] in Azure OpenAI completions response"));
    }

    public void testFromResponse_FailsWhenMessageValueIsAString() {
        String responseJson = """
            {
                "choices": [
                            {
                                "finish_reason": "stop",
                                "index": 0,
                                "logprobs": null,
                                "message": "string"
                                }
                            ],
                            "model": "gpt-4",
                            "object": "chat.completion"
                            ]
            }""";

        var thrownException = expectThrows(
            ParsingException.class,
            () -> AzureOpenAiCompletionResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_OBJECT] but found [VALUE_STRING]")
        );
    }

}
