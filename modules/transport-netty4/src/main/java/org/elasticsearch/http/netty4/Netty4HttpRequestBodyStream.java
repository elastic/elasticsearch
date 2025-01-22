/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.ArrayList;
import java.util.List;

/**
 * Netty based implementation of {@link HttpBody.Stream}.
 * This implementation utilize {@link io.netty.channel.ChannelConfig#setAutoRead(boolean)}
 * to prevent entire payload buffering. But sometimes upstream can send few chunks of data despite
 * autoRead=off. In this case chunks will be buffered until downstream calls {@link Stream#next()}
 */
public class Netty4HttpRequestBodyStream implements HttpBody.Stream {

    private final Channel channel;
    private final ChannelFutureListener closeListener = future -> doClose();
    private final List<ChunkHandler> tracingHandlers = new ArrayList<>(4);
    private final ThreadContext threadContext;
    private final ThreadWatchdog.ActivityTracker activityTracker;
    private ByteBuf buf;
    private boolean requested = false;
    private boolean closing = false;
    private HttpBody.ChunkHandler handler;
    private ThreadContext.StoredContext requestContext;

    // used in tests
    private volatile int bufSize = 0;
    private volatile boolean hasLast = false;

    public Netty4HttpRequestBodyStream(Channel channel, ThreadContext threadContext, ThreadWatchdog.ActivityTracker activityTracker) {
        this.channel = channel;
        this.threadContext = threadContext;
        this.requestContext = threadContext.newStoredContext();
        this.activityTracker = activityTracker;
        Netty4Utils.addListener(channel.closeFuture(), closeListener);
        channel.config().setAutoRead(false);
    }

    @Override
    public ChunkHandler handler() {
        return handler;
    }

    @Override
    public void setHandler(ChunkHandler chunkHandler) {
        this.handler = chunkHandler;
    }

    @Override
    public void addTracingHandler(ChunkHandler chunkHandler) {
        assert tracingHandlers.contains(chunkHandler) == false;
        tracingHandlers.add(chunkHandler);
    }

    @Override
    public void next() {
        assert handler != null : "handler must be set before requesting next chunk";
        requestContext = threadContext.newStoredContext();
        channel.eventLoop().submit(() -> {
            activityTracker.startActivity();
            requested = true;
            try {
                if (closing) {
                    return;
                }
                if (buf == null) {
                    channel.read();
                } else {
                    send();
                }
            } catch (Throwable e) {
                channel.pipeline().fireExceptionCaught(e);
            } finally {
                activityTracker.stopActivity();
            }
        });
    }

    public void handleNettyContent(HttpContent httpContent) {
        assert hasLast == false : "receive http content on completed stream";
        hasLast = httpContent instanceof LastHttpContent;
        if (closing) {
            httpContent.release();
        } else {
            addChunk(httpContent.content());
            if (requested) {
                send();
            }
        }
    }

    // adds chunk to current buffer, will allocate composite buffer when need to hold more than 1 chunk
    private void addChunk(ByteBuf chunk) {
        assert chunk != null;
        if (buf == null) {
            buf = chunk;
        } else if (buf instanceof CompositeByteBuf comp) {
            comp.addComponent(true, chunk);
        } else {
            var comp = channel.alloc().compositeBuffer();
            comp.addComponent(true, buf);
            comp.addComponent(true, chunk);
            buf = comp;
        }
        bufSize = buf.readableBytes();
    }

    // visible for test
    int bufSize() {
        return bufSize;
    }

    // visible for test
    boolean hasLast() {
        return hasLast;
    }

    private void send() {
        assert requested;
        assert handler != null : "must set handler before receiving next chunk";
        var bytesRef = Netty4Utils.toReleasableBytesReference(buf);
        requested = false;
        buf = null;
        bufSize = 0;
        try (var ignored = threadContext.restoreExistingContext(requestContext)) {
            for (var tracer : tracingHandlers) {
                tracer.onNext(bytesRef, hasLast);
            }
            handler.onNext(bytesRef, hasLast);
        }
        if (hasLast) {
            channel.config().setAutoRead(true);
            channel.closeFuture().removeListener(closeListener);
        }
    }

    @Override
    public void close() {
        if (channel.eventLoop().inEventLoop()) {
            doClose();
        } else {
            channel.eventLoop().submit(this::doClose);
        }
    }

    private void doClose() {
        closing = true;
        try (var ignored = threadContext.restoreExistingContext(requestContext)) {
            for (var tracer : tracingHandlers) {
                Releasables.closeExpectNoException(tracer);
            }
            if (handler != null) {
                handler.close();
            }
        }
        if (buf != null) {
            buf.release();
            buf = null;
            bufSize = 0;
        }
        channel.config().setAutoRead(true);
    }
}
