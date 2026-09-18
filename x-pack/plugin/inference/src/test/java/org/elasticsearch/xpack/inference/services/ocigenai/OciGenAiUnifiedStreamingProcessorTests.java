/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.util.ArrayDeque;
import java.util.List;

import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onError;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onNext;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class OciGenAiUnifiedStreamingProcessorTests extends ESTestCase {

    private static final String MODEL_ID = "meta.llama-3.3-70b-instruct";

    public void testGenericStream() {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("""
            {"index":0,"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":"Hello"}]},"pad":"aaaaa"}"""));
        item.offer(new ServerSentEvent("""
            {"index":0,"message":{"role":"ASSISTANT","toolCalls":[{"type":"FUNCTION","id":"call_1","name":"get_weather"}]},"pad":"a"}"""));
        item.offer(new ServerSentEvent("""
            {"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":""}]},"finishReason":"stop","pad":"aaa"}"""));
        item.offer(new ServerSentEvent("[DONE]"));

        var results = onNext(createProcessor(), item);

        var chunks = List.copyOf(results.chunks());
        assertThat(chunks, hasSize(3));
        chunks.forEach(chunk -> {
            assertThat(chunk.id(), is("stream-id"));
            assertThat(chunk.model(), is(MODEL_ID));
            assertThat(chunk.object(), is("chat.completion.chunk"));
        });
        assertThat(chunks.get(0).choices().getFirst().message().content(), is("Hello"));
        assertThat(chunks.get(0).choices().getFirst().message().role(), is("assistant"));
        assertThat(chunks.get(1).choices().getFirst().message().toolCalls().getFirst().function().name(), is("get_weather"));
        assertThat(chunks.get(2).choices().getFirst().finishReason(), is("stop"));
        assertThat(chunks.get(2).choices().getFirst().message().content(), nullValue());
    }

    public void testCohereStream() {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("""
            {"apiFormat":"COHERE","text":"Hello","pad":"aaaaa"}"""));
        item.offer(new ServerSentEvent("""
            {"apiFormat":"COHERE","text":"Hello","chatHistory":[],"finishReason":"MAX_TOKENS","pad":"aaaaaaa"}"""));

        var results = onNext(createProcessor(), item);

        var chunks = List.copyOf(results.chunks());
        assertThat(chunks, hasSize(2));
        assertThat(chunks.get(0).choices().getFirst().message().content(), is("Hello"));
        assertThat(chunks.get(1).choices().getFirst().message().content(), nullValue());
        assertThat(chunks.get(1).choices().getFirst().finishReason(), is("length"));
    }

    public void testParseErrorIsReportedThroughTheErrorParser() {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("not json"));
        var expected = new RuntimeException("mapped");

        var exception = onError(new OciGenAiUnifiedStreamingProcessor("stream-id", MODEL_ID, (message, e) -> expected), item);

        assertThat(exception, sameInstance(expected));
    }

    private static OciGenAiUnifiedStreamingProcessor createProcessor() {
        return new OciGenAiUnifiedStreamingProcessor("stream-id", MODEL_ID, (message, e) -> new RuntimeException(message, e));
    }
}
