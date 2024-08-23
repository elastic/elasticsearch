/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Netty4HttpRequestBodyStreamTests extends ESTestCase {

    EmbeddedChannel channel;
    Netty4HttpRequestBodyStream stream;

    @Before
    public void createStream() {
        channel = new EmbeddedChannel();
        stream = new Netty4HttpRequestBodyStream(channel);
        channel.pipeline().addLast(new SimpleChannelInboundHandler<HttpContent>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, HttpContent msg) {
                msg.retain();
                stream.onHttpContent(msg);
            }
        });
    }

    // ensures that no chunks are sent downstream without request
    public void testEnqueueChunksBeforeRequest() {
        var totalChunks = randomIntBetween(1, 100);
        for (int i = 0; i < totalChunks; i++) {
            channel.writeInbound(randomContent(1024));
        }
        assertEquals(totalChunks, stream.queueSize());
    }

    // ensures all queued chunks can be flushed downstream
    public void testFlushQueued() {
        var chunks = new ArrayList<ReleasableBytesReference>();
        var totalBytes = new AtomicInteger();
        stream.setConsumingHandler((chunk, isLast) -> {
            chunks.add(chunk);
            totalBytes.addAndGet(chunk.length());
        });
        // enqueue chunks
        var chunkSize = 1024;
        var totalChunks = randomIntBetween(1, 100);
        for (int i = 0; i < totalChunks; i++) {
            channel.writeInbound(randomContent(chunkSize));
        }
        // consume all chunks
        for (var i = 0; i < totalChunks; i++) {
            stream.next();
        }
        assertEquals(totalChunks, chunks.size());
        assertEquals(chunkSize * totalChunks, totalBytes.get());
    }

    // ensures that we read from channel when chunks queue is empty
    // and pass next chunk downstream without queuing
    public void testReadFromChannel() {
        var gotChunks = new ArrayList<ReleasableBytesReference>();
        var gotLast = new AtomicBoolean(false);
        stream.setConsumingHandler((chunk, isLast) -> {
            gotChunks.add(chunk);
            gotLast.set(isLast);
        });
        channel.pipeline().addFirst(new FlowControlHandler()); // blocks all incoming messages, need explicit channel.read()
        var chunkSize = 1024;
        var totalChunks = randomIntBetween(1, 32);
        for (int i = 0; i < totalChunks - 1; i++) {
            channel.writeInbound(randomContent(chunkSize));
        }
        channel.writeInbound(randomLastContent(chunkSize));

        for (int i = 0; i < totalChunks; i++) {
            assertEquals("should not enqueue chunks", 0, stream.queueSize());
            stream.next();
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
