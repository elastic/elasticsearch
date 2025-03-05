/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.googleaistudio;

import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.response.googleaistudio.GoogleAiStudioCompletionResponseEntity;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.util.ArrayDeque;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onError;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onNext;
import static org.elasticsearch.xpack.inference.external.response.streaming.StreamingInferenceTestUtils.containsResults;
import static org.elasticsearch.xpack.inference.external.response.streaming.StreamingInferenceTestUtils.events;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class GoogleAiStudioStreamingProcessorTests extends ESTestCase {

    public void testParseSuccess() {
        var item = events("""
             {
              "candidates": [
                {
                  "content": {
                    "parts": [
                      {
                        "text": "Hello"
                      }
                    ],
                    "role": "model"
                  },
                  "finishReason": "STOP",
                  "index": 0,
                  "safetyRatings": [
                    {
                      "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                      "probability": "NEGLIGIBLE"
                    },
                    {
                      "category": "HARM_CATEGORY_HATE_SPEECH",
                      "probability": "NEGLIGIBLE"
                    },
                    {
                      "category": "HARM_CATEGORY_HARASSMENT",
                      "probability": "NEGLIGIBLE"
                    },
                    {
                      "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                      "probability": "NEGLIGIBLE"
                    }
                  ]
                }
              ],
              "usageMetadata": {
                "promptTokenCount": 1,
                "candidatesTokenCount": 1,
                "totalTokenCount": 1
              }
            }""", """
             {
              "candidates": [
                {
                  "content": {
                    "parts": [
                      {
                        "text": ", World"
                      }
                    ],
                    "role": "model"
                  },
                  "finishReason": "STOP",
                  "index": 0,
                  "safetyRatings": [
                    {
                      "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                      "probability": "NEGLIGIBLE"
                    },
                    {
                      "category": "HARM_CATEGORY_HATE_SPEECH",
                      "probability": "NEGLIGIBLE"
                    },
                    {
                      "category": "HARM_CATEGORY_HARASSMENT",
                      "probability": "NEGLIGIBLE"
                    },
                    {
                      "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                      "probability": "NEGLIGIBLE"
                    }
                  ]
                }
              ],
              "usageMetadata": {
                "promptTokenCount": 1,
                "candidatesTokenCount": 1,
                "totalTokenCount": 1
              }
            }""");

        var response = onNext(new GoogleAiStudioStreamingProcessor(GoogleAiStudioCompletionResponseEntity::content), item);
        assertThat(response.results().size(), equalTo(2));
        assertThat(response.results(), containsResults("Hello", ", World"));
    }

    public void testEmptyResultsRequestsMoreData() throws Exception {
        var emptyDeque = new ArrayDeque<ServerSentEvent>();

        var processor = new GoogleAiStudioStreamingProcessor(noOp -> {
            fail("This should not be called");
            return null;
        });

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(emptyDeque);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testOnError() {
        var expectedException = new RuntimeException("hello");

        var processor = new GoogleAiStudioStreamingProcessor(noOp -> { throw expectedException; });

        assertThat(onError(processor, events("hi")), sameInstance(expectedException));
    }
}
