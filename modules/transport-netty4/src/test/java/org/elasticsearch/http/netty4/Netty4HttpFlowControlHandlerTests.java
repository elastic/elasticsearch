/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

public class Netty4HttpFlowControlHandlerTests extends ESTestCase {

    private EmbeddedChannel channel;
    private ReadSniffer readSniffer;
    private ReadCompleteCounter readCompletes;

    @Before
    public void initChannel() throws Exception {
        channel = new EmbeddedChannel();
        channel.config().setAutoRead(false);
        readSniffer = new ReadSniffer();
        readCompletes = new ReadCompleteCounter();
        // readSniffer sits upstream so it counts the reads this handler forwards towards the network
        channel.pipeline().addLast(readSniffer, new Netty4HttpFlowControlHandler(), readCompletes);
    }

    public void testNonChunkedPassthrough() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        var content = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(req, content);
        assertSame(req, readOne());
        assertSame(content, readOne());
    }

    public void testDecodingFailurePassthrough() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        req.setDecoderResult(DecoderResult.failure(new Exception()));
        channel.writeInbound(req);
        HttpRequest recvReq = readOne();
        assertTrue(recvReq.decoderResult().isFailure());
        assertTrue("should not wait for content it will never classify", HttpUtil.isTransferEncodingChunked(recvReq));
    }

    public void testHoldChunkedRequest() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        channel.writeInbound(req);
        channel.read();
        assertNull("should hold on HTTP request until first chunk arrives", channel.readInbound());
        assertEquals("must read first chunk when holding request", 1, readSniffer.readCount);

        var content = randomLastContent(between(1, 1024));
        channel.writeInbound(content);
        assertSame("request must be released as soon as its first chunk arrives", req, channel.readInbound());
        assertNull("must not release the chunk in the same read as the request", channel.readInbound());
        assertSame(content, readOne());
        content.release();
    }

    public void testRemoveEncodingFromEmpty() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        var content = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(req, content);
        HttpRequest recvReq = readOne();
        assertSame(req, recvReq);
        assertFalse("should remove Transfer-Encoding from empty content", HttpUtil.isTransferEncodingChunked(recvReq));
        assertSame(content, readOne());
    }

    public void testKeepEncodingForNonEmpty() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        var content = randomLastContent(between(1, 1024));
        channel.writeInbound(req, content);
        HttpRequest recvReq = readOne();
        assertSame(req, recvReq);
        assertTrue("should keep Transfer-Encoding for non-empty content", HttpUtil.isTransferEncodingChunked(recvReq));
        assertSame(content, readOne());
        content.release();
    }

    public void testSingleChunkIsNotCombined() {
        var content = randomContent(between(1, 64));
        channel.writeInbound(content);
        assertSame("a run of one must be released untouched", content, readOne());
        content.release();
    }

    public void testCombinedRunWithoutLastContentStaysNonTerminal() {
        var first = randomContent(between(1, 64));
        var second = randomContent(between(1, 64));
        int expectedBytes = first.content().readableBytes() + second.content().readableBytes();

        channel.writeInbound(first, second);
        HttpContent combined = readOne();
        assertFalse("a run with no LastHttpContent must not terminate the body", combined instanceof LastHttpContent);
        assertEquals(expectedBytes, combined.content().readableBytes());
        combined.release();
    }

    public void testNoLookaheadReadWhenBatchAlreadyHasFirstChunk() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        var content = randomLastContent(between(1, 1024));

        // request and first chunk arrive in one batch, so the empty-body check needs no extra read
        channel.writeInbound(req, content);
        assertEquals("must not read when the batch already supplied the first chunk", 0, readSniffer.readCount);
        assertSame(req, readOne());
        assertSame(content, readOne());
        content.release();
    }

    public void testChunkedRequestWithCombinedBody() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        HttpUtil.setTransferEncodingChunked(req, true);
        var first = randomContent(between(1, 64));
        var last = randomLastContent(between(1, 64));
        int expectedBytes = first.content().readableBytes() + last.content().readableBytes();

        channel.writeInbound(req, first, last);
        HttpRequest recvReq = readOne();
        assertSame(req, recvReq);
        assertTrue("body is not empty, so the encoding must survive", HttpUtil.isTransferEncodingChunked(recvReq));

        LastHttpContent body = readOne();
        assertEquals("the whole body must arrive as one combined chunk", expectedBytes, body.content().readableBytes());
        body.release();
    }

    public void testCombinedContentIsReadableAsBytesReference() {
        var first = randomContent(between(1, 64));
        var empty = new DefaultHttpContent(Unpooled.EMPTY_BUFFER);
        var last = randomLastContent(between(1, 64));
        var expected = new ByteArrayOutputStream();
        expected.writeBytes(ByteBufUtil.getBytes(first.content()));
        expected.writeBytes(ByteBufUtil.getBytes(last.content()));

        channel.writeInbound(first, empty, last);
        HttpContent combined = readOne();
        // Netty4HttpRequestBodyStream converts every chunk this way. An interior empty component would leave the
        // composite reporting a buffer with no backing array, which Netty4Utils#toBytesReference cannot read.
        assertArrayEquals(expected.toByteArray(), BytesReference.toBytes(Netty4Utils.toBytesReference(combined.content())));
        combined.release();
    }

    public void testCombinedContentKeepsTrailingHeaders() {
        var first = randomContent(between(1, 64));
        var last = randomLastContent(between(1, 64));
        last.trailingHeaders().add("some-trailer", "some-value");
        int expectedBytes = first.content().readableBytes() + last.content().readableBytes();

        channel.writeInbound(first, last);
        LastHttpContent combined = readOne();
        assertNotSame("chunks must have been combined", last, combined);
        assertEquals("trailing headers must survive combining", "some-value", combined.trailingHeaders().get("some-trailer"));
        assertEquals(expectedBytes, combined.content().readableBytes());
        combined.release();
    }

    /**
     * Writes a batch of pipelined requests in a single read, as the decoder would, and drains it one message at a time.
     * Combining must not reorder requests, leak bytes, or let one request's body absorb the next request's chunks.
     */
    public void testDrainPipelinedRequestsInOrder() {
        var requests = new ArrayList<HttpRequest>();
        var expectedBodies = new ArrayList<byte[]>();
        var inbound = new ArrayList<HttpObject>();

        int requestCount = between(1, 4);
        for (int i = 0; i < requestCount; i++) {
            var request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/" + i);
            requests.add(request);
            inbound.add(request);
            var body = new ByteArrayOutputStream();
            int chunkCount = between(1, 5);
            for (int c = 0; c < chunkCount; c++) {
                var chunk = c == chunkCount - 1 ? randomLastContent(between(0, 64)) : randomContent(between(0, 64));
                body.writeBytes(ByteBufUtil.getBytes(chunk.content()));
                inbound.add(chunk);
            }
            expectedBodies.add(body.toByteArray());
        }

        channel.writeInbound(inbound.toArray());

        for (int i = 0; i < requestCount; i++) {
            assertSame("requests must be released in order", requests.get(i), readOne());
            var body = new ByteArrayOutputStream();
            boolean isLast = false;
            while (isLast == false) {
                HttpContent content = readOne();
                assertNotNull("body must terminate with a LastHttpContent", content);
                isLast = content instanceof LastHttpContent;
                body.writeBytes(ByteBufUtil.getBytes(content.content()));
                content.release();
            }
            assertArrayEquals("body must survive combining intact", expectedBodies.get(i), body.toByteArray());
        }

        channel.read();
        assertNull("everything must have been released", channel.readInbound());
    }

    public void testSatisfiesReadArmedBeforeMessageArrives() {
        channel.read(); // the transport arms a read when it sets the channel up, before any message exists
        assertEquals(1, readSniffer.readCount);

        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        var content = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(req, content);
        assertSame("an armed read must be satisfied as soon as a message arrives", req, channel.readInbound());
        assertNull("an armed read must be satisfied only once", channel.readInbound());
        assertSame(content, readOne());
    }

    public void testRepeatedReadsAreDeduplicated() {
        for (int i = 0; i < between(2, 5); i++) {
            channel.read();
        }
        assertEquals("repeated reads with nothing to release must issue a single upstream read", 1, readSniffer.readCount);

        var content = randomContent(between(1, 64));
        channel.writeInbound(content);
        assertSame(content, channel.readInbound());
        assertNull("repeated reads must not release more than one message", channel.readInbound());
        content.release();
    }

    public void testFiresReadCompletePerReleasedMessage() {
        var req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "");
        var content = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(req, content);
        assertEquals("nothing released yet, so no read-complete downstream", 0, readCompletes.count);
        readOne();
        assertEquals(1, readCompletes.count);
        readOne();
        assertEquals(2, readCompletes.count);
    }

    public void testReleasesQueuedContentOnClose() {
        var content = randomContent(between(1, 64));
        channel.writeInbound(content);
        assertEquals(1, content.refCnt());
        channel.close();
        assertEquals("queued content must be released when the channel closes", 0, content.refCnt());
    }

    /**
     * Reuses one channel across cases on purpose: a case can leave an unsatisfied read behind, so the next one starts
     * with a read already armed and exercises the arrival-satisfies-read path rather than the read-after-arrival path.
     */
    public void testRandomizedChannelReuse() {
        for (int i = 0; i < 1000; i++) {
            switch (between(0, 6)) {
                case 0 -> testNonChunkedPassthrough();
                case 1 -> testKeepEncodingForNonEmpty();
                case 2 -> testDecodingFailurePassthrough();
                case 3 -> testSingleChunkIsNotCombined();
                case 4 -> testDrainPipelinedRequestsInOrder();
                case 5 -> testChunkedRequestWithCombinedBody();
                case 6 -> testRemoveEncodingFromEmpty();
                default -> throw new AssertionError("unexpected case");
            }
        }
    }

    /**
     * Returns the next released message, reading only if an earlier read has not already been satisfied, and asserts the
     * one-message-per-read contract as it goes.
     */
    private <T> T readOne() {
        T msg = channel.readInbound();
        if (msg == null) {
            channel.read();
            msg = channel.readInbound();
        }
        assertNull("must release at most one message per read", channel.readInbound());
        return msg;
    }

    private HttpContent randomContent(int size) {
        return new DefaultHttpContent(Unpooled.wrappedBuffer(randomByteArrayOfLength(size)));
    }

    private LastHttpContent randomLastContent(int size) {
        return new DefaultLastHttpContent(Unpooled.wrappedBuffer(randomByteArrayOfLength(size)));
    }

    private static class ReadCompleteCounter extends ChannelInboundHandlerAdapter {
        int count;

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            count++;
            ctx.fireChannelReadComplete();
        }
    }
}
