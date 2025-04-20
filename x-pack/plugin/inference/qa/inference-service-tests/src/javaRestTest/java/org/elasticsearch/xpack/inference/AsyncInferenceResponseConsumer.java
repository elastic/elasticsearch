/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import org.apache.http.nio.util.SimpleInputBuffer;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicReference;

class AsyncInferenceResponseConsumer extends AbstractAsyncResponseConsumer<HttpResponse> {
    private final AtomicReference<HttpResponse> httpResponse = new AtomicReference<>();
    private final Deque<ServerSentEvent> collector = new ArrayDeque<>();
    private final ServerSentEventParser sseParser = new ServerSentEventParser();
    private final SimpleInputBuffer inputBuffer = new SimpleInputBuffer(4096);

    @Override
    protected void onResponseReceived(HttpResponse httpResponse) {
        this.httpResponse.set(httpResponse);
    }

    @Override
    protected void onContentReceived(ContentDecoder contentDecoder, IOControl ioControl) throws IOException {
        inputBuffer.consumeContent(contentDecoder);
    }

    @Override
    protected void onEntityEnclosed(HttpEntity httpEntity, ContentType contentType) {
        httpResponse.updateAndGet(response -> {
            response.setEntity(httpEntity);
            return response;
        });
    }

    @Override
    protected HttpResponse buildResult(HttpContext httpContext) {
        var allBytes = new byte[inputBuffer.length()];
        try {
            inputBuffer.read(allBytes);
            sseParser.parse(allBytes).forEach(collector::offer);
        } catch (IOException e) {
            failed(e);
        }
        return httpResponse.get();
    }

    @Override
    protected void releaseResources() {}

    Deque<ServerSentEvent> events() {
        return collector;
    }
}
