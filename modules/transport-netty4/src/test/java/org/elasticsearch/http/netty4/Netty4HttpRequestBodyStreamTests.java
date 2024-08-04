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
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Netty4HttpRequestBodyStreamTests extends ESTestCase {

    EmbeddedChannel channel;
    Netty4HttpRequestBodyStream stream;
    static HttpBody.ChunkHandler discardHandler = (chunk, isLast) -> chunk.close();

    @Before
    public void createStream() {
        channel = new EmbeddedChannel();
        stream = new Netty4HttpRequestBodyStream(channel);
        stream.setHandler(discardHandler); // set default handler, each test might override one
        channel.pipeline().addLast(new SimpleChannelInboundHandler<HttpContent>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, HttpContent msg) {
                msg.retain();
                stream.handleNettyContent(msg);
            }
        });
    }

    public void testEnqueueChunksBeforeRequest() {
        var chunkSize = 1024;
        var totalChunks = randomIntBetween(1, 100);
        for (int i = 0; i < totalChunks; i++) {
            channel.writeInbound(randomContent(chunkSize));
        }
        assertEquals(totalChunks, stream.contentChunkQueue().size());
        assertEquals(totalChunks * chunkSize, stream.contentChunkQueue().availableBytes());
    }

    // test that every small request consume at least one chunk
    public void testFlushQueuedSmallRequests() {
        var chunks = new ArrayList<ReleasableBytesReference>();
        var totalBytes = new AtomicInteger();
        stream.setHandler((chunk, isLast) -> {
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
            stream.requestBytes(1);
        }
        assertEquals(totalChunks, chunks.size());
        assertEquals(chunkSize * totalChunks, totalBytes.get());
    }

    // test that one large request consume all chunks
    public void testFlushQueuedLargeRequest() {
        var requestedChunkSize = new AtomicInteger();
        stream.setHandler((chunk, isLast) -> { requestedChunkSize.addAndGet(chunk.length()); });
        var chunkSize = 1024;
        var totalChunks = randomIntBetween(1, 100);
        for (int i = 0; i < totalChunks; i++) {
            channel.writeInbound(randomContent(chunkSize));
        }
        stream.requestBytes(chunkSize * totalChunks);
        assertEquals(chunkSize * totalChunks, requestedChunkSize.get());
    }

    // test that we flush queued chunks with last content when request is larger than availableBytes
    public void testFlushQueuedLastContent() {
        var gotLast = new AtomicBoolean(false);
        var contentSize = new AtomicInteger(0);
        stream.setHandler((chunk, isLast) -> {
            contentSize.addAndGet(chunk.length());
            gotLast.set(isLast);
        });
        channel.writeInbound(randomContent(1024));
        channel.writeInbound(randomLastContent(0));
        stream.requestBytes(1025);
        assertTrue(gotLast.get());
        assertEquals(1024, contentSize.get());
    }

    // ensures that we enqueue chunk if cannot fulfill current request
    public void testEnqueueWhenNotEnoughBytes() {
        stream.requestBytes(1024);
        var chunkSize = 32;
        var totalChunks = randomIntBetween(1, 1024 / chunkSize - 1);
        for (int i = 0; i < totalChunks; i++) {
            channel.writeInbound(randomContent(chunkSize));
        }
        assertEquals(chunkSize * totalChunks, stream.contentChunkQueue().availableBytes());
    }

    public void testFlushSingleLastContent() {
        var flushed = new AtomicBoolean(false);
        stream.setHandler((chunk, isLast) -> flushed.set(true));
        stream.requestBytes(1024);
        channel.writeInbound(randomLastContent(0));
        assertTrue(flushed.get());
    }

    // test that we read from channel when not enough available bytes
    public void testReadFromChannel() {
        var gotChunks = new ArrayList<ReleasableBytesReference>();
        var gotLast = new AtomicBoolean(false);
        stream.setHandler((chunk, isLast) -> {
            gotChunks.add(chunk);
            gotLast.set(isLast);
        });
        channel.pipeline().addFirst(new FlowControlHandler()); // block all incoming messages, need explicit channel.read()
        var chunkSize = 1024;
        var totalChunks = randomIntBetween(1, 32);
        for (int i = 0; i < totalChunks; i++) {
            channel.writeInbound(randomContent(chunkSize));
        }

        assertEquals("should not read until requested", 0, stream.contentChunkQueue().size());
        stream.requestBytes(chunkSize);
        assertEquals(1, gotChunks.size());

        stream.requestBytes(Integer.MAX_VALUE);
        assertEquals("should enqueue remaining chunks", totalChunks - 1, stream.contentChunkQueue().size());

        // send last content and flush queued content
        channel.writeInbound(randomLastContent(0));
        assertEquals(0, stream.contentChunkQueue().size());
        assertEquals(2, gotChunks.size());
        assertTrue(gotLast.get());
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
