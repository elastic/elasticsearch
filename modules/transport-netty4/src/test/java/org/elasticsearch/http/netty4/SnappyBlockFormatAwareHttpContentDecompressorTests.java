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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Snappy;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests decompressor selection for Prometheus remote write requests.
 */
public class SnappyBlockFormatAwareHttpContentDecompressorTests extends ESTestCase {

    public void testSelectsSnappyBlockDecoderForPrometheusRemoteWrite() {
        byte[] original = "prometheus-remote-write".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = snappyBlockCompress(original);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockFormatAwareHttpContentDecompressor());
        try {
            assertTrue(channel.writeInbound(prometheusRemoteWriteRequest("/_prometheus/api/v1/write", true, true)));
            assertTrue(channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(compressed))));

            assertThat(channel.readInbound(), instanceOf(HttpRequest.class));
            byte[] decompressedBody = readInboundBody(channel);
            assertArrayEquals(original, decompressedBody);
            assertNull(channel.readInbound());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testDoesNotSelectSnappyBlockDecoderOutsidePrometheusPath() {
        byte[] original = "prometheus-remote-write".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = snappyBlockCompress(original);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockFormatAwareHttpContentDecompressor());
        try {
            assertTrue(channel.writeInbound(prometheusRemoteWriteRequest("/_bulk", true, true)));
            expectThrows(Exception.class, () -> channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(compressed))));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testDoesNotSelectSnappyBlockDecoderWithoutRemoteWriteVersionHeader() {
        byte[] original = "prometheus-remote-write".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = snappyBlockCompress(original);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockFormatAwareHttpContentDecompressor());
        try {
            assertTrue(channel.writeInbound(prometheusRemoteWriteRequest("/_prometheus/api/v1/write", false, true)));
            expectThrows(Exception.class, () -> channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(compressed))));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testDoesNotSelectSnappyBlockDecoderWithoutProtobufContentType() {
        byte[] original = "prometheus-remote-write".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = snappyBlockCompress(original);

        EmbeddedChannel channel = new EmbeddedChannel(new SnappyBlockFormatAwareHttpContentDecompressor());
        try {
            assertTrue(channel.writeInbound(prometheusRemoteWriteRequest("/_prometheus/api/v1/write", true, false)));
            expectThrows(Exception.class, () -> channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(compressed))));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static HttpRequest prometheusRemoteWriteRequest(String uri, boolean includeVersionHeader, boolean includeProtobufContentType) {
        DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
        request.headers().set(HttpHeaderNames.CONTENT_ENCODING, "snappy");
        if (includeProtobufContentType) {
            request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf");
        } else {
            request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        }
        if (includeVersionHeader) {
            request.headers().set("X-Prometheus-Remote-Write-Version", "0.1.0");
        }
        return request;
    }

    private static byte[] readInboundBody(EmbeddedChannel channel) {
        ByteBuf combined = Unpooled.buffer();
        try {
            Object bodyMessage;
            while ((bodyMessage = channel.readInbound()) != null) {
                try {
                    assertThat(bodyMessage, instanceOf(HttpContent.class));
                    combined.writeBytes(((HttpContent) bodyMessage).content());
                } finally {
                    ReferenceCountUtil.release(bodyMessage);
                }
            }
            return ByteBufUtil.getBytes(combined);
        } finally {
            combined.release();
        }
    }

    private static byte[] snappyBlockCompress(byte[] data) {
        ByteBuf input = Unpooled.wrappedBuffer(data);
        ByteBuf output = Unpooled.buffer();
        try {
            new Snappy().encode(input, output, data.length);
            byte[] result = new byte[output.readableBytes()];
            output.readBytes(result);
            return result;
        } finally {
            input.release();
            output.release();
        }
    }
}
