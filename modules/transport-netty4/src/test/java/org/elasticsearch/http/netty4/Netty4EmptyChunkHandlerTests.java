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
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.test.ESTestCase;

public class Netty4EmptyChunkHandlerTests extends ESTestCase {

    private EmbeddedChannel channel;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        channel = new EmbeddedChannel(new Netty4EmptyChunkHandler());
        channel.config().setAutoRead(false);
    }

    public void testNonChunkedPassthrough() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        var content = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(req, content);
        assertEquals(req, channel.readInbound());
        assertEquals(content, channel.readInbound());
    }

    public void testDecodingFailurePassthrough() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        req.setDecoderResult(DecoderResult.failure(new Exception()));
        channel.writeInbound(req);
        var recvReq = (HttpRequest) channel.readInbound();
        assertTrue(recvReq.decoderResult().isFailure());
        assertTrue(HttpUtil.isTransferEncodingChunked(recvReq));
    }

    public void testHoldChunkedRequest() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        var readSniffer = new ReadSniffer();
        channel.pipeline().addFirst(readSniffer);
        channel.writeInbound(req);
        assertNull("should hold on HTTP request until first chunk arrives", channel.readInbound());
        assertEquals("must read first chunk when holding request", 1, readSniffer.readCount);
    }

    public void testRemoveEncodingFromEmpty() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        var content = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(req, content);
        var recvReq = channel.readInbound();
        assertEquals(req, recvReq);
        assertEquals(content, channel.readInbound());
        assertFalse("should remove Transfer-Encoding from empty content", HttpUtil.isTransferEncodingChunked((HttpMessage) recvReq));
    }

    public void testKeepEncodingForNonEmpty() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        var content = new DefaultLastHttpContent(Unpooled.wrappedBuffer(randomByteArrayOfLength(between(1, 1024))));
        channel.writeInbound(req, content);
        var recvReq = channel.readInbound();
        assertEquals(req, recvReq);
        assertEquals(content, channel.readInbound());
        assertTrue("should keep Transfer-Encoding for non-empty content", HttpUtil.isTransferEncodingChunked((HttpMessage) recvReq));
    }

    public void testRandomizedChannelReuse() {
        for (int i = 0; i < 1000; i++) {
            switch (between(0, 3)) {
                case 0 -> testNonChunkedPassthrough();
                case 1 -> testKeepEncodingForNonEmpty();
                case 2 -> testDecodingFailurePassthrough();
                default -> testRemoveEncodingFromEmpty();
            }
        }
    }
}
