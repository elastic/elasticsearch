/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Netty based implementation of {@link HttpBody.Stream}.
 */
public class Netty4HttpRequestBodyStream implements HttpBody.Stream {

    private final List<ChunkHandler> tracingHandlers = new ArrayList<>(4);
    private final ThreadContext threadContext;
    private final ChannelHandlerContext ctx;
    private boolean closing = false;
    private boolean readLastChunk = false;
    private HttpBody.ChunkHandler handler;
    private ThreadContext.StoredContext requestContext;
    private final ChannelFutureListener closeListener = future -> doClose();

    public Netty4HttpRequestBodyStream(ChannelHandlerContext ctx, ThreadContext threadContext) {
        this.ctx = ctx;
        this.threadContext = threadContext;
        this.requestContext = threadContext.newStoredContext();
        Netty4Utils.addListener(ctx.channel().closeFuture(), closeListener);
    }

    @Override
    public ChunkHandler handler() {
        return handler;
    }

    @Override
    public void setHandler(ChunkHandler chunkHandler) {
        assert ctx.channel().eventLoop().inEventLoop() : Thread.currentThread().getName();
        this.handler = chunkHandler;
    }

    @Override
    public void addTracingHandler(ChunkHandler chunkHandler) {
        assert tracingHandlers.contains(chunkHandler) == false;
        tracingHandlers.add(chunkHandler);
    }

    private void read() {
        ctx.channel().eventLoop().execute(ctx::read);
    }

    @Override
    public void next() {
        assert handler != null : "handler must be set before requesting next chunk";
        requestContext = threadContext.newStoredContext();
        read();
    }

    public void handleNettyContent(HttpContent httpContent) {
        assert ctx.channel().eventLoop().inEventLoop() : Thread.currentThread().getName();
        if (closing) {
            httpContent.release();
            read();
        } else {
            assert readLastChunk == false;
            try (var ignored = threadContext.restoreExistingContext(requestContext)) {
                var isLast = httpContent instanceof LastHttpContent;
                var buf = Netty4Utils.toReleasableBytesReference(httpContent.content());
                for (var tracer : tracingHandlers) {
                    tracer.onNext(buf, isLast);
                }
                handler.onNext(buf, isLast);
                if (isLast) {
                    readLastChunk = true;
                    ctx.channel().closeFuture().removeListener(closeListener);
                    read();
                }
            }
        }
    }

    @Override
    public void close() {
        if (ctx.channel().eventLoop().inEventLoop()) {
            doClose();
        } else {
            ctx.channel().eventLoop().submit(this::doClose);
        }
    }

    private void doClose() {
        assert ctx.channel().eventLoop().inEventLoop() : Thread.currentThread().getName();
        closing = true;
        try (var ignored = threadContext.restoreExistingContext(requestContext)) {
            for (var tracer : tracingHandlers) {
                Releasables.closeExpectNoException(tracer);
            }
            if (handler != null) {
                handler.close();
            }
        }
        if (readLastChunk == false) {
            read();
        }
    }
}
