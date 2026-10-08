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
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;

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

    /**
     * Raw stream whose bulk reads never cross a member boundary and whose {@code available()} is always 0, like a
     * network stream with an empty socket buffer. This is what made the JDK decoder stop after the first member.
     */
    private static InputStream boundaryAlignedStream(byte[] all, int[] boundaries) {
        return new InputStream() {
            int pos = 0;

            @Override
            public int read() {
                return pos < all.length ? all[pos++] & 0xff : -1;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (pos >= all.length) {
                    return -1;
                }
                int limit = all.length;
                for (int boundary : boundaries) {
                    if (boundary > pos) {
                        limit = boundary;
                        break;
                    }
                }
                int n = Math.min(Math.min(len, limit - pos), between(1, 70_000));
                System.arraycopy(all, pos, b, off, n);
                pos += n;
                return n;
            }

            @Override
            public int available() {
                return 0;
            }
        };
    }

    public void testMultiMemberOverStreamReportingNothingAvailable() throws IOException {
        int members = between(2, 50);
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        int[] boundaries = new int[members];
        for (int i = 0; i < members; i++) {
            byte[] data = randomBoolean() ? randomByteArrayOfLength(between(0, 200)) : compressibleBytes(between(1, 100_000));
            expected.write(data);
            compressed.write(gzip(data));
            boundaries[i] = compressed.size();
        }
        byte[] all = compressed.toByteArray();
        try (InputStream in = new GzipDecompressionCodec().decompress(boundaryAlignedStream(all, boundaries))) {
            assertArrayEquals(expected.toByteArray(), in.readAllBytes());
        }
    }

    public void testBgzfSizedMembers() throws IOException {
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        int members = between(2, 30);
        int[] boundaries = new int[members];
        for (int i = 0; i < members; i++) {
            byte[] data = compressibleBytes(65_280);
            expected.write(data);
            compressed.write(gzip(data));
            boundaries[i] = compressed.size();
        }
        try (InputStream in = new GzipDecompressionCodec().decompress(boundaryAlignedStream(compressed.toByteArray(), boundaries))) {
            assertArrayEquals(expected.toByteArray(), in.readAllBytes());
        }
    }

    public void testSingleByteReadsMatchBulkReads() throws IOException {
        byte[] first = compressibleBytes(between(1, 5000));
        byte[] second = compressibleBytes(between(1, 5000));
        byte[] all = concat(gzip(first), gzip(second));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (InputStream in = new GzipDecompressionCodec().decompress(new ByteArrayInputStream(all))) {
            int b;
            while ((b = in.read()) != -1) {
                out.write(b);
            }
        }
        assertArrayEquals(concat(first, second), out.toByteArray());
    }

    public void testHeadersWithOptionalFields() throws IOException {
        byte[] data = compressibleBytes(between(1, 10_000));
        byte[] member = gzipWithHeaderFields(data, randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        byte[] plain = gzip(data);
        byte[] all = concat(member, plain, member);
        try (InputStream in = new GzipDecompressionCodec().decompress(new ByteArrayInputStream(all))) {
            assertArrayEquals(concat(data, data, data), in.readAllBytes());
        }
    }

    public void testCorruptHeaderCrcThrows() throws IOException {
        byte[] member = gzipWithHeaderFields(compressibleBytes(100), false, false, false, true);
        member[10] ^= 0x55; // first byte of the header CRC16 after the 10 fixed header bytes
        expectThrows(ZipException.class, () -> readAll(member));
    }

    public void testTrailingZeroPaddingIsTolerated() throws IOException {
        byte[] data = compressibleBytes(between(1, 10_000));
        byte[] all = concat(gzip(data), new byte[between(1, 200_000)]);
        assertArrayEquals(data, readAll(all));
        byte[] twoMembers = concat(gzip(data), gzip(data), new byte[between(1, 200_000)]);
        assertArrayEquals(concat(data, data), readAll(twoMembers));
    }

    public void testTrailingGarbageThrows() throws IOException {
        byte[] data = compressibleBytes(between(1, 10_000));
        byte[] garbage = randomByteArrayOfLength(between(1, 100));
        int firstGarbageByte = randomValueOtherThanMany(b -> b == 0 || b == 0x1f, () -> between(1, 255));
        garbage[0] = (byte) firstGarbageByte;
        ZipException e = expectThrows(ZipException.class, () -> readAll(concat(gzip(data), garbage)));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("Trailing garbage"));
    }

    public void testZeroPaddingFollowedByNonZeroThrows() throws IOException {
        byte[] data = compressibleBytes(between(1, 1000));
        byte[] tail = new byte[between(2, 100_000)];
        tail[tail.length - 1] = 1;
        expectThrows(ZipException.class, () -> readAll(concat(gzip(data), tail)));
    }

    public void testPartialNextHeaderThrows() throws IOException {
        byte[] data = compressibleBytes(between(1, 1000));
        byte[] partial = new byte[] { 0x1f, (byte) 0x8b, 8 };
        expectThrows(EOFException.class, () -> readAll(concat(gzip(data), partial)));
        expectThrows(ZipException.class, () -> readAll(concat(gzip(data), new byte[] { 0x1f, 0x00, 8, 0, 0, 0, 0, 0, 0, 0 })));
    }

    public void testTruncatedMemberThrows() throws IOException {
        byte[] all = concat(gzip(compressibleBytes(5000)), gzip(compressibleBytes(5000)));
        byte[] truncated = Arrays.copyOf(all, between(1, all.length - 1));
        expectThrows(IOException.class, () -> readAll(truncated));
    }

    public void testCorruptTrailerThrows() throws IOException {
        byte[] member = gzip(compressibleBytes(between(1, 5000)));
        byte[] badCrc = member.clone();
        badCrc[badCrc.length - 8] ^= 1;
        expectThrows(ZipException.class, () -> readAll(badCrc));
        byte[] badSize = member.clone();
        badSize[badSize.length - 1] ^= 1;
        expectThrows(ZipException.class, () -> readAll(badSize));
    }

    public void testEmptyRawInputThrows() {
        expectThrows(EOFException.class, () -> readAll(new byte[0]));
    }

    private static byte[] readAll(byte[] gz) throws IOException {
        try (InputStream in = new GzipDecompressionCodec().decompress(new ByteArrayInputStream(gz))) {
            return in.readAllBytes();
        }
    }

    private static byte[] compressibleBytes(int length) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) ('a' + (i * 31 + i / 7) % 23);
        }
        return b;
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    /** Builds a gzip member by hand, with the optional header fields of RFC 1952 section 2.3. */
    private static byte[] gzipWithHeaderFields(byte[] data, boolean extra, boolean name, boolean comment, boolean headerCrc)
        throws IOException {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        int flags = (extra ? 4 : 0) | (name ? 8 : 0) | (comment ? 16 : 0) | (headerCrc ? 2 : 0);
        header.writeBytes(new byte[] { 0x1f, (byte) 0x8b, 8, (byte) flags, 1, 2, 3, 4, 0, (byte) 0xff });
        if (extra) {
            byte[] field = randomByteArrayOfLength(between(0, 300));
            header.write(field.length & 0xff);
            header.write(field.length >> 8);
            header.writeBytes(field);
        }
        if (name) {
            header.writeBytes("file.csv".getBytes(StandardCharsets.UTF_8));
            header.write(0);
        }
        if (comment) {
            header.writeBytes("a comment".getBytes(StandardCharsets.UTF_8));
            header.write(0);
        }
        if (headerCrc) {
            CRC32 crc = new CRC32();
            crc.update(header.toByteArray());
            int v = (int) crc.getValue() & 0xffff;
            header.write(v & 0xff);
            header.write(v >> 8);
        }
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
        deflater.setInput(data);
        deflater.finish();
        byte[] chunk = new byte[8192];
        while (deflater.finished() == false) {
            int n = deflater.deflate(chunk);
            header.write(chunk, 0, n);
        }
        deflater.end();
        CRC32 crc = new CRC32();
        crc.update(data);
        for (long v : new long[] { crc.getValue(), data.length }) {
            for (int i = 0; i < 4; i++) {
                header.write((int) (v >> (8 * i)) & 0xff);
            }
        }
        return header.toByteArray();
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
