/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;

import java.io.IOException;
import java.util.ArrayDeque;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onError;
import static org.elasticsearch.xpack.inference.common.DelegatingProcessorTests.onNext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class OciGenAiStreamingProcessorTests extends ESTestCase {

    public void testParseGenericEvents() throws IOException {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("""
            {"index":0,"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":""}]},"pad":"aaa"}"""));
        item.offer(new ServerSentEvent("""
            {"index":0,"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":"Hello"}]},"pad":"aaaaa"}"""));
        item.offer(new ServerSentEvent("""
            {"index":0,"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":", how"}]},"pad":"aa"}"""));
        item.offer(new ServerSentEvent("""
            {"message":{"role":"ASSISTANT","content":[{"type":"TEXT","text":""}]},"finishReason":"stop","pad":"aaa"}"""));
        item.offer(new ServerSentEvent("[DONE]"));

        var response = onNext(new OciGenAiStreamingProcessor(), item);

        assertThat(toJsonString(response), equalTo("""
            {"completion":[{"delta":"Hello"},{"delta":", how"}]}"""));
    }

    public void testParseCohereEvents_IgnoresTheFinalFullText() throws IOException {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("""
            {"apiFormat":"COHERE","text":"Hello","pad":"aaaaa"}"""));
        item.offer(new ServerSentEvent("""
            {"apiFormat":"COHERE","text":", how","pad":"a"}"""));
        item.offer(new ServerSentEvent("""
            {"apiFormat":"COHERE","text":"Hello, how","chatHistory":[],"finishReason":"COMPLETE","pad":"aaaaaaa"}"""));

        var response = onNext(new OciGenAiStreamingProcessor(), item);

        assertThat(toJsonString(response), equalTo("""
            {"completion":[{"delta":"Hello"},{"delta":", how"}]}"""));
    }

    public void testParseErrorCallsOnError() {
        var item = new ArrayDeque<ServerSentEvent>();
        item.offer(new ServerSentEvent("not json"));

        var exception = onError(new OciGenAiStreamingProcessor(), item);

        assertThat(exception, notNullValue());
    }

    private String toJsonString(ChunkedToXContent chunkedToXContent) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            chunkedToXContent.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xContent -> {
                try {
                    xContent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    fail(e.getMessage());
                }
            });
            return XContentHelper.convertToJson(BytesReference.bytes(builder), false, builder.contentType());
        }
    }
}
