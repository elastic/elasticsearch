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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.flow.FlowControlHandler;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.hasEntry;

public class Netty4HttpRequestBodyStreamTests extends ESTestCase {

    static HttpBody.ChunkHandler discardHandler = (chunk, isLast) -> chunk.close();
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private EmbeddedChannel channel;
    private Netty4HttpRequestBodyStream stream;
    private ThreadWatchdog.ActivityTracker activityTracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        channel = new EmbeddedChannel();
        activityTracker = new ThreadWatchdog.ActivityTracker();
        stream = new Netty4HttpRequestBodyStream(channel, threadContext, activityTracker);
        stream.setHandler(discardHandler); // set default handler, each test might override one
        channel.pipeline().addLast(new SimpleChannelInboundHandler<HttpContent>(false) {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, HttpContent msg) {
                stream.handleNettyContent(msg);
            }
        });
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        stream.close();
    }

    // ensures that no chunks are sent downstream without request
    public void testEnqueueChunksBeforeRequest() {
        var totalChunks = randomIntBetween(1, 100);
        for (int i = 0; i < totalChunks; i++) {
            channel.writeInbound(randomContent(1024));
        }
        assertEquals(totalChunks * 1024, stream.bufSize());
    }

    // ensures all received chunks can be flushed downstream
    public void testFlushAllReceivedChunks() {
        var chunks = new ArrayList<ReleasableBytesReference>();
        var totalBytes = new AtomicInteger();
        stream.setHandler((chunk, isLast) -> {
            chunks.add(chunk);
            totalBytes.addAndGet(chunk.length());
            chunk.close();
        });

        var chunkSize = 1024;
        var totalChunks = randomIntBetween(1, 100);
        for (int i = 0; i < totalChunks; i++) {
            channel.writeInbound(randomContent(chunkSize));
        }
        stream.next();
        channel.runPendingTasks();
        assertEquals("should receive all chunks as single composite", 1, chunks.size());
        assertEquals(chunkSize * totalChunks, totalBytes.get());
    }

    // ensures that channel.setAutoRead(true) only when we flush last chunk
    public void testSetAutoReadOnLastFlush() {
        channel.writeInbound(randomLastContent(10));
        assertFalse("should not auto-read on last content reception", channel.config().isAutoRead());
        stream.next();
        channel.runPendingTasks();
        assertTrue("should set auto-read once last content is flushed", channel.config().isAutoRead());
    }

    // ensures that we read from channel when no current chunks available
    // and pass next chunk downstream without holding
    public void testReadFromChannel() {
        var gotChunks = new ArrayList<ReleasableBytesReference>();
        var gotLast = new AtomicBoolean(false);
        stream.setHandler((chunk, isLast) -> {
            gotChunks.add(chunk);
            gotLast.set(isLast);
            chunk.close();
        });
        channel.pipeline().addFirst(new FlowControlHandler()); // block all incoming messages, need explicit channel.read()
        var chunkSize = 1024;
        var totalChunks = randomIntBetween(1, 32);
        for (int i = 0; i < totalChunks - 1; i++) {
            channel.writeInbound(randomContent(chunkSize));
        }
        channel.writeInbound(randomLastContent(chunkSize));

        for (int i = 0; i < totalChunks; i++) {
            assertEquals("should not enqueue chunks", 0, stream.bufSize());
            stream.next();
            channel.runPendingTasks();
            assertEquals("each next() should produce single chunk", i + 1, gotChunks.size());
        }
        assertTrue("should receive last content", gotLast.get());
    }

    public void testReadFromHasCorrectThreadContext() throws InterruptedException {
        AtomicReference<Map<String, String>> headers = new AtomicReference<>();
        var eventLoop = new DefaultEventLoop();
        var gotLast = new AtomicBoolean(false);
        var chunkSize = 1024;
        threadContext.putHeader("header1", "value1");
        try {
            // activity tracker requires stream execution in the same thread, setting up stream inside event-loop
            eventLoop.submit(() -> {
                channel = new EmbeddedChannel();
                stream = new Netty4HttpRequestBodyStream(channel, threadContext, new ThreadWatchdog.ActivityTracker());
                channel.pipeline().addLast(new SimpleChannelInboundHandler<HttpContent>(false) {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, HttpContent msg) {
                        stream.handleNettyContent(msg);
                    }
                });
                stream.setHandler(new HttpBody.ChunkHandler() {
                    @Override
                    public void onNext(ReleasableBytesReference chunk, boolean isLast) {
                        headers.set(threadContext.getHeaders());
                        gotLast.set(isLast);
                        chunk.close();
                    }

                    @Override
                    public void close() {
                        headers.set(threadContext.getHeaders());
                    }
                });
                channel.pipeline().addFirst(new FlowControlHandler()); // block all incoming messages, need explicit channel.read()
            }).await();

            channel.writeInbound(randomContent(chunkSize));
            channel.writeInbound(randomLastContent(chunkSize));

            threadContext.putHeader("header2", "value2");
            stream.next();

            eventLoop.submit(() -> channel.runPendingTasks()).await();
            assertThat(headers.get(), hasEntry("header1", "value1"));
            assertThat(headers.get(), hasEntry("header2", "value2"));

            threadContext.putHeader("header3", "value3");
            stream.next();

            eventLoop.submit(() -> channel.runPendingTasks()).await();
            assertThat(headers.get(), hasEntry("header1", "value1"));
            assertThat(headers.get(), hasEntry("header2", "value2"));
            assertThat(headers.get(), hasEntry("header3", "value3"));

            assertTrue("should receive last content", gotLast.get());

            headers.set(new HashMap<>());

            stream.close();

            assertThat(headers.get(), hasEntry("header1", "value1"));
            assertThat(headers.get(), hasEntry("header2", "value2"));
            assertThat(headers.get(), hasEntry("header3", "value3"));
        } finally {
            eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS);
        }
    }

    public void testStreamNextActivityTracker() {
        var t0 = activityTracker.get();
        var N = between(1, 10);
        for (int i = 0; i < N; i++) {
            channel.writeInbound(randomContent(1024));
            stream.next();
            channel.runPendingTasks();
        }
        var t1 = activityTracker.get();
        assertEquals("stream#next() must trigger activity tracker: N*step=" + N + "*2=" + N * 2L + " times", t1, t0 + N * 2L);
    }

    // ensure that we catch all exceptions and throw them into channel pipeline
    public void testCatchExceptions() {
        var gotExceptions = new CountDownLatch(3); // number of tests below

        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                gotExceptions.countDown();
            }
        });

        // catch exception for not buffered chunk, will be thrown on channel.fireChannelRead()
        stream.setHandler((a, b) -> { throw new RuntimeException(); });
        stream.next();
        channel.runPendingTasks();
        channel.writeInbound(randomContent(1));

        // catch exception for buffered chunk, will be thrown from eventLoop.submit()
        channel.writeInbound(randomContent(1));
        stream.next();
        channel.runPendingTasks();

        // should catch OOM exceptions too, see DieWithDignity
        // swallowing exceptions can result in dangling streams, hanging channels, and delayed shutdowns
        stream.setHandler((a, b) -> { throw new OutOfMemoryError(); });
        channel.writeInbound(randomContent(1));
        stream.next();
        channel.runPendingTasks();

        safeAwait(gotExceptions);
    }

    HttpContent randomContent(int size, boolean isLast) {
        var buf = Unpooled.wrappedBuffer(randomByteArrayOfLength(size));
        if (isLast) {
            return new DefaultLastHttpContent(buf);
        } else {
            return new DefaultHttpContent(buf);
        }
    }

    HttpContent randomContent(int size) {
        return randomContent(size, false);
    }

    HttpContent randomLastContent(int size) {
        return randomContent(size, true);
    }

}
