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
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

public class Netty4HttpContentSizeHandlerTests extends ESTestCase {

    private static final int MAX_CONTENT_LENGTH = 1024;
    private static final int OVERSIZED_LENGTH = MAX_CONTENT_LENGTH + 1;
    private static final int REPS = 1000;
    private EmbeddedChannel channel;
    private EmbeddedChannel encoder; // channel to encode HTTP objects into bytes
    private ReadSniffer readSniffer;

    private static HttpContent httpContent(int size) {
        return new DefaultHttpContent(Unpooled.wrappedBuffer(randomByteArrayOfLength(size)));
    }

    private static LastHttpContent lastHttpContent(int size) {
        return new DefaultLastHttpContent(Unpooled.wrappedBuffer(randomByteArrayOfLength(size)));
    }

    private HttpRequest httpRequest() {
        return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
    }

    // encodes multiple HTTP objects into single ByteBuf
    private ByteBuf encode(HttpObject... objs) {
        var out = Unpooled.compositeBuffer();
        Arrays.stream(objs).forEach(encoder::writeOutbound);
        while (encoder.outboundMessages().isEmpty() == false) {
            out.addComponent(true, encoder.readOutbound());
        }
        return out;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        var decoder = new HttpRequestDecoder();
        encoder = new EmbeddedChannel(new HttpRequestEncoder());
        readSniffer = new ReadSniffer();
        channel = new EmbeddedChannel();
        channel.config().setAutoRead(false);
        channel.pipeline().addLast(decoder, readSniffer, new Netty4HttpContentSizeHandler(MAX_CONTENT_LENGTH));
    }

    public void testDecodingFailurePassThrough() {
        for (var i = 0; i < REPS; i++) {
            var sendReq = httpRequest();
            sendReq.setDecoderResult(DecoderResult.failure(new Exception("bad")));
            channel.writeInbound(sendReq);
            assertEquals(sendReq, channel.readInbound());
        }
        assertEquals("should not read from channel, failures are handled downstream", 0, readSniffer.readCount);
    }

    /**
     * Assert that handler replies 100-continue for acceptable request and pass request further.
     */
    public void testContinue() {
        for (var i = 0; i < REPS; i++) {
            var sendRequest = httpRequest();
            HttpUtil.set100ContinueExpected(sendRequest, true);
            channel.writeInbound(encode(sendRequest));
            assertEquals("should send back 100-continue", Netty4HttpContentSizeHandler.CONTINUE, channel.readOutbound());
            var recvRequest = (HttpRequest) channel.readInbound();
            assertNotNull(recvRequest);
            assertFalse(HttpUtil.is100ContinueExpected(recvRequest));
            channel.writeInbound(encode(LastHttpContent.EMPTY_LAST_CONTENT));
            assertEquals(LastHttpContent.EMPTY_LAST_CONTENT, channel.readInbound());
            assertEquals("must not read from channel", 0, readSniffer.readCount);
        }
    }

    /**
     * Assert that handler pass through acceptable request.
     */
    public void testWithoutContinue() {
        for (var i = 0; i < REPS; i++) {
            var sendRequest = httpRequest();
            channel.writeInbound(encode(sendRequest));
            assertNull("should not receive response", channel.readOutbound());
            assertNotNull("request should pass", channel.readInbound());
            channel.writeInbound(encode(LastHttpContent.EMPTY_LAST_CONTENT));
            assertEquals(LastHttpContent.EMPTY_LAST_CONTENT, channel.readInbound());
            assertEquals("must not read from channel", 0, readSniffer.readCount);
        }
    }

    /**
     * Assert that handler pass through request and content for acceptable request.
     */
    public void testContinueWithContent() {
        for (var i = 0; i < REPS; i++) {
            var sendRequest = httpRequest();
            HttpUtil.set100ContinueExpected(sendRequest, true);
            HttpUtil.setContentLength(sendRequest, MAX_CONTENT_LENGTH);
            var sendContent = lastHttpContent(MAX_CONTENT_LENGTH);
            channel.writeInbound(encode(sendRequest, sendContent));
            var resp = (FullHttpResponse) channel.readOutbound();
            assertEquals("should send back 100-continue", Netty4HttpContentSizeHandler.CONTINUE, resp);
            resp.release();
            var recvRequest = (HttpRequest) channel.readInbound();
            assertNotNull(recvRequest);
            var recvContent = (HttpContent) channel.readInbound();
            assertNotNull(recvContent);
            assertEquals(MAX_CONTENT_LENGTH, recvContent.content().readableBytes());
            recvContent.release();
            assertEquals("must not read from channel", 0, readSniffer.readCount);
        }
    }

    /**
     * Assert that handler return 417 Expectation Failed and closes channel on request
     * with "Expect" header other than "100-Continue".
     */
    public void testExpectationFailed() {
        var sendRequest = httpRequest();
        sendRequest.headers().set(HttpHeaderNames.EXPECT, randomValueOtherThan(HttpHeaderValues.CONTINUE, ESTestCase::randomIdentifier));
        channel.writeInbound(encode(sendRequest));
        var resp = (FullHttpResponse) channel.readOutbound();
        assertEquals(HttpResponseStatus.EXPECTATION_FAILED, resp.status());
        assertEquals("expect 2 reads, one from size handler and HTTP decoder will emit LastHttpContent", 2, readSniffer.readCount);
        assertFalse(channel.isOpen());
        resp.release();
    }

    private static void assertTooLargeResponseClosesConnection(FullHttpResponse resp, EmbeddedChannel channel) {
        assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, resp.status());
        assertEquals(HttpHeaderValues.CLOSE.toString(), resp.headers().get(HttpHeaderNames.CONNECTION));
        assertFalse("should close channel", channel.isOpen());
    }

    /**
     * Assert that handler returns 413 Request Entity Too Large for oversized request
     * and closes the connection.
     */
    public void testEntityTooLarge() {
        var sendRequest = httpRequest();
        HttpUtil.set100ContinueExpected(sendRequest, true);
        HttpUtil.setContentLength(sendRequest, OVERSIZED_LENGTH);
        channel.writeInbound(encode(sendRequest, LastHttpContent.EMPTY_LAST_CONTENT));
        var resp = (FullHttpResponse) channel.readOutbound();
        assertTooLargeResponseClosesConnection(resp, channel);
        assertNull("request should not pass", channel.readInbound());
        assertEquals("must read from channel", 1, readSniffer.readCount);
        resp.release();
    }

    /**
     * Mixed load of oversized and normal requests with Exepct:100-Continue.
     */
    public void testMixedContent() {
        for (int i = 0; i < REPS; i++) {
            var isOversized = randomBoolean();
            var sendRequest = httpRequest();
            HttpUtil.set100ContinueExpected(sendRequest, true);
            if (isOversized) {
                HttpUtil.setContentLength(sendRequest, OVERSIZED_LENGTH);
                channel.writeInbound(encode(sendRequest));
                var resp = (FullHttpResponse) channel.readOutbound();
                assertTooLargeResponseClosesConnection(resp, channel);
                assertNull(channel.readInbound());
                resp.release();
                return; // channel is closed, no more to do
            }

            var normalSize = between(1, MAX_CONTENT_LENGTH);
            HttpUtil.setContentLength(sendRequest, normalSize);
            channel.writeInbound(encode(sendRequest));
            var resp = (FullHttpResponse) channel.readOutbound();
            assertEquals(HttpResponseStatus.CONTINUE, resp.status());
            resp.release();
            var sendContent = lastHttpContent(normalSize);
            channel.writeInbound(encode(sendContent));
            var recvRequest = (HttpRequest) channel.readInbound();
            var recvContent = (LastHttpContent) channel.readInbound();
            assertEquals("content length header should match", normalSize, HttpUtil.getContentLength(recvRequest));
            assertFalse("should remove expect header", HttpUtil.is100ContinueExpected(recvRequest));
            assertEquals("actual content size should match", normalSize, recvContent.content().readableBytes());
            recvContent.release();
            assertEquals(0, readSniffer.readCount);
        }
    }

    /**
     * Assert that handler returns 413 Request Entity Too Large, skips following content, and closes the connection.
     */
    public void testEntityTooLargeWithContentWithoutExpect() {
        var sendRequest = httpRequest();
        HttpUtil.setContentLength(sendRequest, OVERSIZED_LENGTH);
        var unexpectedContent = lastHttpContent(OVERSIZED_LENGTH);
        channel.writeInbound(encode(sendRequest, unexpectedContent));
        var resp = (FullHttpResponse) channel.readOutbound();
        assertTooLargeResponseClosesConnection(resp, channel);
        resp.release();
        assertNull("request and content should not pass", channel.readInbound());
        assertEquals("expect two reads, one for request and one for content", 2, readSniffer.readCount);
    }

    /**
     * Oversized Expect requests must not repurpose owed body bytes as a new HTTP request when coalesced on the wire.
     */
    public void testEntityTooLargeExpectDoesNotParseSmuggledRequestCoalesced() {
        var sendRequest = httpRequest();
        HttpUtil.set100ContinueExpected(sendRequest, true);
        HttpUtil.setContentLength(sendRequest, OVERSIZED_LENGTH);
        var followUpRequest = httpRequest();
        HttpUtil.setContentLength(followUpRequest, 0);

        var coalesced = Unpooled.compositeBuffer();
        for (var httpRequest : List.of(sendRequest, followUpRequest)) {
            var encoder = new EmbeddedChannel(new HttpRequestEncoder());
            httpRequest.headers().set(HttpHeaderNames.HOST, "localhost");
            encoder.writeOutbound(httpRequest);
            while (encoder.outboundMessages().isEmpty() == false) {
                coalesced.addComponent(true, encoder.readOutbound());
            }
        }
        channel.writeInbound(coalesced);
        var resp = (FullHttpResponse) channel.readOutbound();
        assertTooLargeResponseClosesConnection(resp, channel);
        assertNull("smuggled request must not pass downstream", channel.readInbound());
        resp.release();
    }

    /**
     * Oversized Expect requests close the connection once the 413 is sent.
     */
    public void testEntityTooLargeExpectDoesNotParseSmuggledRequestAfterResponse() {
        var sendRequest = httpRequest();
        HttpUtil.set100ContinueExpected(sendRequest, true);
        HttpUtil.setContentLength(sendRequest, OVERSIZED_LENGTH);
        channel.writeInbound(encode(sendRequest));
        var resp = (FullHttpResponse) channel.readOutbound();
        assertTooLargeResponseClosesConnection(resp, channel);
        assertNull("request must not pass downstream", channel.readInbound());
        resp.release();
    }

    /**
     * Assert that handler return 413 Request Entity Too Large and closes channel for oversized
     * requests with chunked content.
     */
    public void testEntityTooLargeWithChunkedContent() {
        var sendRequest = httpRequest();
        HttpUtil.setTransferEncodingChunked(sendRequest, true);
        channel.writeInbound(encode(sendRequest));
        assertTrue("request should pass", channel.readInbound() instanceof HttpRequest);

        int contentBytesSent = 0;
        do {
            var thisPartSize = between(1, MAX_CONTENT_LENGTH * 2);
            channel.writeInbound(encode(httpContent(thisPartSize)));
            contentBytesSent += thisPartSize;

            if (contentBytesSent <= MAX_CONTENT_LENGTH) {
                ((HttpContent) channel.readInbound()).release();
            } else {
                break;
            }
        } while (true);

        var resp = (FullHttpResponse) channel.readOutbound();
        assertEquals("should respond with 413", HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, resp.status());
        assertFalse("should close channel", channel.isOpen());
        assertEquals("expect read after response", 1, readSniffer.readCount);
        resp.release();
    }
}
