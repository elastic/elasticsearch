/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gzip;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

/**
 * Unit tests for {@link GzipDecompressionCodec}.
 */
public class GzipDecompressionCodecTests extends ESTestCase {

    public void testNameAndExtensions() {
        GzipDecompressionCodec codec = new GzipDecompressionCodec();
        assertEquals("gzip", codec.name());
        assertTrue(codec.extensions().contains(".gz"));
        assertTrue(codec.extensions().contains(".gzip"));
    }

    public void testRoundTripDecompression() throws IOException {
        String original = "hello,world\n1,2\nbar,baz";
        byte[] compressed = gzip(original.getBytes(StandardCharsets.UTF_8));

        GzipDecompressionCodec codec = new GzipDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testInvalidGzipThrows() throws IOException {
        byte[] invalidGzip = new byte[] { 0x00, 0x01, 0x02 };
        GzipDecompressionCodec codec = new GzipDecompressionCodec();

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream ignored = codec.decompress(new ByteArrayInputStream(invalidGzip))) {
                ignored.readAllBytes();
            }
        });
        assertNotNull(e.getMessage());
    }

    public void testEmptyInput() throws IOException {
        byte[] compressed = gzip(new byte[0]);
        GzipDecompressionCodec codec = new GzipDecompressionCodec();

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(0, result.length);
        }
    }

    public void testBreakerAccountingReturnsToZero() throws IOException {
        byte[] original = "hello,world\n1,2\nbar,baz".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(original);
        GzipDecompressionCodec codec = new GzipDecompressionCodec();
        UsedBytesCircuitBreaker breaker = new UsedBytesCircuitBreaker();
        for (int i = 0; i < 50; i++) {
            try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed), breaker)) {
                assertEquals("iteration " + i, GzipDecompressionCodec.NATIVE_INFLATER_BYTES, breaker.getUsed());
                assertEquals(new String(original, StandardCharsets.UTF_8), new String(decompressed.readAllBytes(), StandardCharsets.UTF_8));
            }
            assertEquals("iteration " + i, 0L, breaker.getUsed());
        }
    }

    public void testConstructionChargeTripsBreaker() throws IOException {
        byte[] compressed = gzip("x".getBytes(StandardCharsets.UTF_8));
        GzipDecompressionCodec codec = new GzipDecompressionCodec();
        UsedBytesCircuitBreaker breaker = new UsedBytesCircuitBreaker(1L);
        AtomicInteger closes = new AtomicInteger();
        InputStream raw = new FilterInputStream(new ByteArrayInputStream(compressed)) {
            @Override
            public void close() throws IOException {
                closes.incrementAndGet();
                super.close();
            }
        };
        expectThrows(CircuitBreakingException.class, () -> codec.decompress(raw, breaker));
        assertEquals("trip must close the gzip stream (and therefore raw)", 1, closes.get());
        assertEquals(0L, breaker.getUsed());
    }

    public void testNullBreakerRoundTrips() throws IOException {
        String original = "hello,world\n1,2\nbar,baz";
        byte[] compressed = gzip(original.getBytes(StandardCharsets.UTF_8));
        GzipDecompressionCodec codec = new GzipDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed), null)) {
            assertEquals(original, new String(decompressed.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(input);
        }
        return baos.toByteArray();
    }

    private static final class UsedBytesCircuitBreaker implements CircuitBreaker {
        private final AtomicLong used = new AtomicLong();
        private final long limit;

        UsedBytesCircuitBreaker() {
            this(Long.MAX_VALUE);
        }

        UsedBytesCircuitBreaker(long limit) {
            this.limit = limit;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {}

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            long next = used.addAndGet(bytes);
            if (next > limit) {
                used.addAndGet(-bytes);
                throw new CircuitBreakingException("gzip-inflater", bytes, limit, Durability.TRANSIENT);
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }

        @Override
        public long getLimit() {
            return limit;
        }

        @Override
        public double getOverhead() {
            return 1.0;
        }

        @Override
        public long getTrippedCount() {
            return 0;
        }

        @Override
        public String getName() {
            return "test-used-bytes";
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {}
    }
}
