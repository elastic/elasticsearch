/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onError;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onNext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GoogleVertexAiStreamingProcessorTests extends ESTestCase {

    public void testParseVertexAiResponse() throws IOException {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent(vertexAiJsonResponse("test", true)));

        var response = onNext(new GoogleVertexAiStreamingProcessor(), item);
        var json = toJsonString(response);

        assertThat(json, equalTo("""
            {"completion":[{"delta":"test"}]}"""));
    }

    public void testParseVertexAiResponseMultiple() throws IOException {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent(vertexAiJsonResponse("hello", false)));

        item.offer(new ServerSentEvent(vertexAiJsonResponse("world", true)));

        var response = onNext(new GoogleVertexAiStreamingProcessor(), item);
        var json = toJsonString(response);

        assertThat(json, equalTo("""
            {"completion":[{"delta":"hello"},{"delta":"world"}]}"""));
    }

    public void testParseErrorCallsOnError() {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("not json"));

        var exception = onError(new GoogleVertexAiStreamingProcessor(), item);
        assertThat(exception, instanceOf(XContentParseException.class));
    }

    private String vertexAiJsonResponse(String content, boolean includeFinishReason) {
        String finishReason = includeFinishReason ? "\"finishReason\": \"STOP\"," : "";

        return Strings.format("""
              {
              "candidates": [
                {
                  "content": {
                    "role": "model",
                    "parts": [
                      {
                        "text": "%s"
                      }
                    ]
                  },
                  %s
                  "avgLogprobs": -0.19326641248620074
                }
              ],
              "usageMetadata": {
                "promptTokenCount": 71,
                "candidatesTokenCount": 23,
                "totalTokenCount": 94,
                "trafficType": "ON_DEMAND",
                "promptTokensDetails": [
                  {
                    "modality": "TEXT",
                    "tokenCount": 71
                  }
                ],
                "candidatesTokensDetails": [
                  {
                    "modality": "TEXT",
                    "tokenCount": 23
                  }
                ]
              },
              "modelVersion": "gemini-2.0-flash-001",
              "createTime": "2025-05-28T15:08:20.049493Z",
              "responseId": "5CY3aNWCA6mm4_UPr-zduAE"
            }
            """, content, finishReason);
    }

    private String toJsonString(ChunkedToXContent chunkedToXContent) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            chunkedToXContent.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                try {
                    xContent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            });
            return XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());
        }
    }
}
