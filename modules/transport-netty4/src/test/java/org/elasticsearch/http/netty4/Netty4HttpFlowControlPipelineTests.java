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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class Netty4HttpFlowControlPipelineTests extends ESTestCase {

    private static final int MAX_FORWARD_BYTES = 64 * 1024;

    private int maxChunkSize;
    private int socketReadSize;
    private EmbeddedChannel channel;
    private PerReadCycleCounter intoDecompressor;
    private Recorder recorder;

    @Before
    public void initChannel() {
        maxChunkSize = between(1024, 32 * 1024);
        socketReadSize = between(512, 64 * 1024);
        var decoder = new HttpRequestDecoder(4096, 16 * 1024, maxChunkSize);
        decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
        intoDecompressor = new PerReadCycleCounter();
        recorder = new Recorder();
        channel = new EmbeddedChannel();
        channel.config().setAutoRead(false);
        channel.pipeline()
            .addLast(decoder)
            .addLast(new Netty4HttpFlowControlHandler())
            .addLast(intoDecompressor)
            .addLast(new HttpContentDecompressor())
            .addLast(new Netty4HttpFlowControlHandler())
            .addLast(recorder);
    }

    @After
    public void closeChannel() {
        channel.finishAndReleaseAll();
    }

    public void testDeliversOneMessagePerReadWithBodyIntact() {
        var scheme = randomFrom("gzip", "deflate", null);
        var body = scheme == null ? randomByteArrayOfLength(between(1, 64 * 1024)) : compressibleBody(between(1, 512) * 1024);
        writeRequest(scheme == null ? body : compress(body, scheme), scheme);
        drain();

        assertArrayEquals("body must arrive intact and in order", body, recorder.bytes());
        int bound = scheme == null ? maxChunkSize : MAX_FORWARD_BYTES;
        for (int length : recorder.lengths) {
            assertThat("each delivery must stay within " + bound + " bytes", length, lessThanOrEqualTo(bound));
        }
    }

    /**
     * The decompressor flushes once its accumulated output crosses a threshold, so the largest delivery pins that
     * threshold from both sides: raising it makes deliveries bigger, lowering it makes them smaller.
     */
    public void testLargestDecompressedMessageIsMaxForwardBytes() {
        var body = new byte[8 * MAX_FORWARD_BYTES];
        var compressed = compress(body, "gzip");
        assertThat("body must fit one wire chunk so the run comes from a single input", compressed.length, lessThan(maxChunkSize));

        writeRequest(compressed, "gzip");
        drain();

        assertThat("one wire chunk must expand into several messages", recorder.lengths.size(), greaterThan(1));
        assertEquals("largest delivery must be exactly the flush threshold", MAX_FORWARD_BYTES, recorder.largest());
    }

    /**
     * A read cycle at the decompressor is one message plus the {@code channelReadComplete} that follows it. Feeding it
     * more than one message per cycle would let a single read expand several wire chunks at once.
     */
    public void testFeedsDecompressorOneMessagePerRead() {
        writeRequest(compress(compressibleBody(4 * 1024 * 1024), "gzip"), "gzip");
        drain();

        assertThat("decompressor must see several read cycles", intoDecompressor.perCycle.size(), greaterThan(1));
        assertEquals("decompressor must be fed one message per read cycle", 1, intoDecompressor.largestCycle());
    }

    private void drain() {
        for (int read = 0; read < 100_000 && recorder.sawLast == false; read++) {
            int before = recorder.lengths.size();
            channel.read();
            assertThat("at most one message per read", recorder.lengths.size() - before, lessThanOrEqualTo(1));
        }
        assertTrue("body must terminate", recorder.sawLast);
    }

    private void writeRequest(byte[] body, String contentEncoding) {
        var head = new StringBuilder("POST /_bulk HTTP/1.1\r\nContent-Length: ").append(body.length).append("\r\n");
        if (contentEncoding != null) {
            head.append("Content-Encoding: ").append(contentEncoding).append("\r\n");
        }
        head.append("\r\n");

        var wire = new ByteArrayOutputStream();
        wire.writeBytes(head.toString().getBytes(StandardCharsets.UTF_8));
        wire.writeBytes(body);
        var bytes = wire.toByteArray();

        for (int off = 0; off < bytes.length; off += socketReadSize) {
            channel.writeInbound(Unpooled.wrappedBuffer(bytes, off, Math.min(socketReadSize, bytes.length - off)));
        }
    }

    private static byte[] compressibleBody(int size) {
        var body = new StringBuilder(size + 128);
        while (body.length() < size) {
            body.append("{\"index\":{\"_index\":\"logs\"}}\n{\"@timestamp\":\"2026-09-15T00:00:00Z\",\"msg\":\"hello world\"}\n");
        }
        return body.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] compress(byte[] raw, String scheme) {
        var bos = new ByteArrayOutputStream();
        try (var out = scheme.equals("gzip") ? new GZIPOutputStream(bos) : new DeflaterOutputStream(bos)) {
            out.write(raw);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    private static class Recorder extends ChannelInboundHandlerAdapter {

        final List<Integer> lengths = new ArrayList<>();
        private final ByteArrayOutputStream body = new ByteArrayOutputStream();
        boolean sawLast;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpContent content) {
                lengths.add(content.content().readableBytes());
                body.writeBytes(ByteBufUtil.getBytes(content.content()));
                sawLast |= content instanceof LastHttpContent;
            }
            ReferenceCountUtil.release(msg);
        }

        byte[] bytes() {
            return body.toByteArray();
        }

        int largest() {
            return lengths.stream().mapToInt(Integer::intValue).max().orElse(0);
        }
    }

    private static class PerReadCycleCounter extends ChannelInboundHandlerAdapter {

        final List<Integer> perCycle = new ArrayList<>();
        private int inCycle;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpContent) {
                inCycle++;
            }
            ctx.fireChannelRead(msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            perCycle.add(inCycle);
            inCycle = 0;
            ctx.fireChannelReadComplete();
        }

        int largestCycle() {
            return perCycle.stream().mapToInt(Integer::intValue).max().orElse(0);
        }
    }
}
