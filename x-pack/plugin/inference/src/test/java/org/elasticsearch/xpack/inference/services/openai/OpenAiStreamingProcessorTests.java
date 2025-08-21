/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.Flow;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onError;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onNext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class OpenAiStreamingProcessorTests extends ESTestCase {
    public void testParseOpenAiResponse() throws IOException {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("""
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
            """));

        var response = onNext(new OpenAiStreamingProcessor(), item);
        var json = toJsonString(response);

        assertThat(json, equalTo("""
            {"completion":[{"delta":"test"}]}"""));
    }

    public void testParseWithFinish() throws IOException {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("""
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
                            "content":"hello, world"
                        },
                        "logprobs":null,
                        "finish_reason":null
                    }
                ]
            }
            """));
        item.offer(new ServerSentEvent("""
            {
                "id":"12345",
                "object":"chat.completion.chunk",
                "created":123456789,
                "model":"gpt-4o-mini",
                "system_fingerprint": "123456789",
                "choices":[
                    {
                        "index":1,
                        "delta":{},
                        "logprobs":null,
                        "finish_reason":"stop"
                    }
                ]
            }
            """));

        var response = onNext(new OpenAiStreamingProcessor(), item);
        var json = toJsonString(response);

        assertThat(json, equalTo("""
            {"completion":[{"delta":"hello, world"}]}"""));
    }

    public void testParseErrorCallsOnError() {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("this isn't json"));

        var exception = onError(new OpenAiStreamingProcessor(), item);
        assertThat(exception, instanceOf(XContentParseException.class));
    }

    public void testEmptyResultsRequestsMoreData() throws Exception {
        var emptyDeque = new ArrayDeque<ServerSentEvent>();

        var processor = new OpenAiStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(emptyDeque);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testDoneMessageIsIgnored() throws Exception {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("[DONE]"));

        var processor = new OpenAiStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(item);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testInitialLlamaResponseIsIgnored() throws Exception {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("""
            {
                "id":"12345",
                "object":"chat.completion.chunk",
                "created":123456789,
                "model":"Llama-2-7b-chat",
                "system_fingerprint": "123456789",
                "choices":[
                    {
                        "index":0,
                        "delta":{
                            "role":"assistant"
                        },
                        "logprobs":null,
                        "finish_reason":null
                    }
                ]
            }
            """));

        var processor = new OpenAiStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(item);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
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
