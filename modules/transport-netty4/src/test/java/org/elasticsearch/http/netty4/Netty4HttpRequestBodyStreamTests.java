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
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.flow.FlowControlHandler;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Netty4HttpRequestBodyStreamTests extends ESTestCase {

    EmbeddedChannel channel;
    Netty4HttpRequestBodyStream stream;
    static HttpBody.ChunkHandler discardHandler = (chunk, isLast) -> chunk.close();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        channel = new EmbeddedChannel();
        stream = new Netty4HttpRequestBodyStream(channel);
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
        assertEquals(totalChunks * 1024, stream.buf().readableBytes());
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
            assertNull("should not enqueue chunks", stream.buf());
            stream.next();
            channel.runPendingTasks();
            assertEquals("each next() should produce single chunk", i + 1, gotChunks.size());
        }
        assertTrue("should receive last content", gotLast.get());
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
