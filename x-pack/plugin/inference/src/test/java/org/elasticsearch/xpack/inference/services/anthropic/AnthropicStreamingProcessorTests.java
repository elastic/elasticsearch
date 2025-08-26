/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onNext;
import static org.elasticsearch.xpack.inference.external.response.streaming.StreamingInferenceTestUtils.containsResults;
import static org.elasticsearch.xpack.inference.external.response.streaming.StreamingInferenceTestUtils.events;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AnthropicStreamingProcessorTests extends ESTestCase {

    public void testParseSuccess() {
        var item = events("""
            {
                "type": "message_start",
                "message": {
                    "id": "a cool id",
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": "claude, probably",
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {
                        "input_tokens": 25,
                        "output_tokens": 1
                    }
                }
            }""", """
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {
                    "type": "text",
                    "text": ""
                }
            }""", """
            {"type": "ping"}""", """
            {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Hello"}}""", """
            {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": ", World"}}""", """
            {"type": "content_block_stop", "index": 0}""", """
            {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence":null}, "usage": {"output_tokens": 4}}""", """
            {"type": "message_stop"}""");

        var response = onNext(new AnthropicStreamingProcessor(), item);
        assertThat(response.results().size(), equalTo(2));
        assertThat(response.results(), containsResults("Hello", ", World"));
    }

    public void testParseWithError() {
        var item = events("""
            {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Hello"}}""", """
            {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": ", World"}}""", """
            {"type": "error", "error": {"type": "rate_limit_error", "message": "You're going too fast, ahhhh!"}}""");

        var statusException = onError(item);
        assertThat(statusException.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertThat(statusException.getMessage(), equalTo("You're going too fast, ahhhh!"));
    }

    public void testErrors() {
        var errors = Map.of("""
            {"type": "error", "error": {"type": "invalid_request_error", "message": "blah"}}""", RestStatus.BAD_REQUEST, """
            {"type": "error", "error": {"type": "authentication_error", "message": "blah"}}""", RestStatus.UNAUTHORIZED, """
            {"type": "error", "error": {"type": "permission_error", "message": "blah"}}""", RestStatus.FORBIDDEN, """
            {"type": "error", "error": {"type": "not_found_error", "message": "blah"}}""", RestStatus.NOT_FOUND, """
            {"type": "error", "error": {"type": "request_too_large", "message": "blah"}}""", RestStatus.REQUEST_ENTITY_TOO_LARGE, """
            {"type": "error", "error": {"type": "rate_limit_error", "message": "blah"}}""", RestStatus.TOO_MANY_REQUESTS, """
            {"type": "error", "error": {"type": "overloaded_error", "message": "blah"}}""", RestStatus.INTERNAL_SERVER_ERROR, """
            {"type": "error", "error": {"type": "some_cool_new_error", "message": "blah"}}""", RestStatus.INTERNAL_SERVER_ERROR);
        errors.forEach((json, expectedStatus) -> { assertThat(onError(events(json)).status(), equalTo(expectedStatus)); });
    }

    public void testEmptyResultsRequestsMoreData() throws Exception {
        var emptyDeque = new ArrayDeque<ServerSentEvent>();

        var processor = new AnthropicStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(emptyDeque);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    public void testDroppedEventsRequestsMoreData() throws Exception {
        var item = events("""
            {"type": "ping"}""");

        var processor = new AnthropicStreamingProcessor();

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        processor.subscribe(downstream);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        processor.next(item);

        verify(upstream, times(1)).request(1);
        verify(downstream, times(0)).onNext(any());
    }

    private static ElasticsearchStatusException onError(Deque<ServerSentEvent> item) {
        var processor = new AnthropicStreamingProcessor();
        var response = new AtomicReference<Throwable>();

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);

        Flow.Subscriber<ChunkedToXContent> downstream = mock();
        doAnswer(ans -> {
            response.set(ans.getArgument(0));
            return null;
        }).when(downstream).onError(any());
        processor.subscribe(downstream);

        processor.onNext(item);
        assertThat("Error from processor was null", response.get(), notNullValue());
        assertThat(response.get(), isA(ElasticsearchStatusException.class));
        return (ElasticsearchStatusException) response.get();
    }
}
