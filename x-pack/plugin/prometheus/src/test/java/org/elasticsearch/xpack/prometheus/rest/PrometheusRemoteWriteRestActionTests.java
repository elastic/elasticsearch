/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.compression.Snappy;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class PrometheusRemoteWriteRestActionTests extends ESTestCase {

    public void testHandlesContentDecoding() {
        var action = new PrometheusRemoteWriteRestAction();
        assertTrue(action.handlesContentDecoding());
    }

    public void testDecompressSnappyBlockContent() {
        byte[] original = "Hello, snappy block format!".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = snappyBlockCompress(original);

        RestRequest request = buildRequestWithSnappy(compressed);

        BytesReference decompressed = PrometheusRemoteWriteRestAction.decompressContent(request);
        assertThat(BytesReference.toBytes(decompressed), equalTo(original));
    }

    public void testDecompressLargePayload() {
        byte[] original = new byte[10_000];
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) (i % 256);
        }
        byte[] compressed = snappyBlockCompress(original);

        RestRequest request = buildRequestWithSnappy(compressed);

        BytesReference decompressed = PrometheusRemoteWriteRestAction.decompressContent(request);
        assertThat(BytesReference.toBytes(decompressed), equalTo(original));
    }

    public void testNoDecompressionWithoutContentEncoding() {
        byte[] raw = "raw bytes".getBytes(StandardCharsets.UTF_8);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_prometheus/api/v1/write")
            .withContent(new BytesArray(raw), null)
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusRemoteWriteRestAction.decompressContent(request)
        );
        assertThat(
            e.getMessage(),
            org.hamcrest.Matchers.containsString("Prometheus remote write requests must use 'Content-Encoding: snappy' compression")
        );
    }

    public void testRejectsOversizedPayload() {
        byte[] original = new byte[PrometheusRemoteWriteRestAction.MAX_UNCOMPRESSED_SIZE + 1];
        byte[] compressed = snappyBlockCompress(original);

        RestRequest request = buildRequestWithSnappy(compressed);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusRemoteWriteRestAction.decompressContent(request)
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("exceeds maximum allowed size"));
    }

    public void testReadUncompressedLength() {
        // 255 -> varint bytes: 0xff 0x01
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[] { (byte) 0xff, 0x01 });
        assertThat(PrometheusRemoteWriteRestAction.readUncompressedLength(buf), equalTo(255));
        buf.release();

        // 300 -> varint bytes: 0xac 0x02
        buf = Unpooled.wrappedBuffer(new byte[] { (byte) 0xac, 0x02 });
        assertThat(PrometheusRemoteWriteRestAction.readUncompressedLength(buf), equalTo(300));
        buf.release();

        // 1 -> varint byte: 0x01
        buf = Unpooled.wrappedBuffer(new byte[] { 0x01 });
        assertThat(PrometheusRemoteWriteRestAction.readUncompressedLength(buf), equalTo(1));
        buf.release();
    }

    private RestRequest buildRequestWithSnappy(byte[] compressed) {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put("Content-Encoding", List.of("snappy"));
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_prometheus/api/v1/write")
            .withHeaders(headers)
            .withContent(new BytesArray(compressed), null)
            .build();
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
