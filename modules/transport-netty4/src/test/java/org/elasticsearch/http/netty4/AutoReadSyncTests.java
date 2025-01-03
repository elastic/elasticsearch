/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;

import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;

import io.netty.handler.codec.http.HttpVersion;

import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.IntStream;

public class AutoReadSyncTests extends ESTestCase {

    Channel chan;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        chan = new EmbeddedChannel();
    }

    AutoReadSync.Handle getHandle() {
        return AutoReadSync.getHandle(chan);
    }

    public void testToggleSetAutoRead() {
        var autoRead = getHandle();
        assertTrue("must be enabled by default", autoRead.isEnabled());

        autoRead.disable();
        assertFalse("must disable handle", autoRead.isEnabled());
        assertFalse("must turn off chan autoRead", chan.config().isAutoRead());

        autoRead.enable();
        assertTrue("must enable handle", autoRead.isEnabled());
        assertTrue("must turn on chan autoRead", chan.config().isAutoRead());

        autoRead.disable();
        autoRead.release();
        assertTrue("must turn on chan autoRead on release", chan.config().isAutoRead());
    }

    public void testAnyToggleDisableAutoRead() {
        var handles = IntStream.range(0, 100).mapToObj(i -> getHandle()).toList();
        handles.forEach(AutoReadSync.Handle::enable);
        handles.get(between(0, 100)).disable();
        assertFalse(chan.config().isAutoRead());
    }

    public void testNewHandleDoesNotChangeAutoRead() {
        var handle1 = getHandle();

        handle1.disable();
        assertFalse(chan.config().isAutoRead());
        getHandle();
        assertFalse("acquiring new handle should enable autoRead", chan.config().isAutoRead());

        handle1.enable();
        assertTrue(chan.config().isAutoRead());
        getHandle();
        assertTrue("acquiring new handle should not disable autoRead", chan.config().isAutoRead());
    }

    public void testAllTogglesEnableAutoRead() {
        // mix-in acquire/release
        var handles = new HashSet<AutoReadSync.Handle>();
        IntStream.range(0, 100).mapToObj(i -> getHandle()).forEach(h -> {
            h.disable();
            handles.add(h);
        });
        assertFalse(chan.config().isAutoRead());

        var toRelease = between(1, 98); // release some but not all
        var releasedHandles = handles.stream().limit(toRelease).toList();
        releasedHandles.forEach(h -> {
            h.release();
            handles.remove(h);
        });
        assertFalse("releasing some but not all handles should not enable autoRead", chan.config().isAutoRead());

        var lastHandle = getHandle();
        lastHandle.disable();
        for (var handle : handles) {
            handle.enable();
            assertFalse("should not enable autoRead until lastHandle is enabled", chan.config().isAutoRead());
        }
        lastHandle.enable();
        assertTrue(chan.config().isAutoRead());
    }

    static final int BUF_SIZE = 1024;

    /**
     * Ensure that HttpStream does not set auto-read true when there is request waiting for auth.
     * This test emulates reception of a large TCP packet that contains 2 HTTP requests and using HTTP pipelining.
     * We authenticate first request and consume its stream, while second request should still wait for auth
     * and channel auto-read should be turned off.
     */
    public void testStreamDoesNotUnblockValidator() {
        var threadCtx = new ThreadContext(Settings.EMPTY);
        var validator = new BlockingValidator();
        var validationHandler = new Netty4HttpHeaderValidator(validator, threadCtx);
        var streamHandler = new StreamHandler();

        var chan = new EmbeddedChannel(validationHandler, streamHandler);

        // first request
        chan.writeInbound(httpRequest());
        chan.writeInbound(httpContent());
        chan.writeInbound(lastHttpContent());

        // second request
        chan.writeInbound(httpRequest());
        chan.writeInbound(lastHttpContent());

        assertEquals("validator should hold first request", 1, validator.queue.size());
        assertTrue(streamHandler.streams.isEmpty());
        assertFalse(chan.config().isAutoRead());

        // allow first request, validator's buffered chunks should move to stream buffer
        validator.allowNext();
        chan.runPendingTasks();
        assertEquals("validator should hold second request", 1, validator.queue.size());
        assertEquals("stream handler should hold first request", 1, streamHandler.streams.size());
        assertEquals("first request has exactly 2 chunks", 2 * BUF_SIZE, streamHandler.streams.getLast().bufSize());
        assertFalse(chan.config().isAutoRead());

        // consume stream and make sure validator is still holding second request and channel auto-read-off
        streamHandler.streams.getLast().next();
        chan.runPendingTasks();
        assertEquals(1, validator.queue.size());
        assertFalse(chan.config().isAutoRead());
    }

    HttpRequest httpRequest() {
        return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    }

    HttpContent httpContent() {
        return new DefaultHttpContent(Unpooled.wrappedBuffer(randomByteArrayOfLength(BUF_SIZE)));
    }

    LastHttpContent lastHttpContent() {
        return new DefaultLastHttpContent(Unpooled.wrappedBuffer(randomByteArrayOfLength(BUF_SIZE)));
    }

    class StreamHandler extends ChannelInboundHandlerAdapter {
        final List<Netty4HttpRequestBodyStream> streams = new ArrayList<>();
        final HttpBody.ChunkHandler discardChunk = (chunk, isLast) -> chunk.close();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpRequest) {
                var stream = new Netty4HttpRequestBodyStream(ctx.channel(), new ThreadContext(Settings.EMPTY));
                streams.add(stream);
                stream.setHandler(discardChunk);
            } else {
                streams.getLast().handleNettyContent((HttpContent) msg);
            }
        }
    }

    class BlockingValidator implements HttpValidator {
        Queue<ActionListener<Void>> queue = new LinkedBlockingDeque<>();

        @Override
        public void validate(HttpRequest httpRequest, Channel channel, ActionListener<Void> listener) {
            queue.add(listener);
        }

        void allowNext() {
            assert queue.isEmpty() == false;
            queue.poll().onResponse(null);
        }
    }

}
