/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class NdJsonRecordCappingInputStreamTests extends ESTestCase {

    public void testLfTerminatedRecordAtExactLimitPasses() throws IOException {
        int max = 8;
        byte[] bytes = bytes("x".repeat(max - 1) + "\n");
        assertReadsAll(bytes, max);
    }

    public void testLfTerminatedRecordOneByteOverLimitThrows() throws IOException {
        int max = 8;
        byte[] bytes = bytes("x".repeat(max) + "\n");
        assertCapThrows(bytes, max);
    }

    public void testLoneCrTerminatedRecordOneByteOverLimitThrows() throws IOException {
        int max = 8;
        byte[] bytes = bytes("x".repeat(max) + "\rnext");
        assertCapThrows(bytes, max);
    }

    public void testCrLfRecordCountsLfTowardSameRecord() throws IOException {
        int max = 8;
        byte[] bytes = bytes("x".repeat(max) + "\r\n");
        assertCapThrows(bytes, max);
    }

    public void testCrLfAtExactLimitPasses() throws IOException {
        int max = 8;
        byte[] bytes = bytes("x".repeat(max - 2) + "\r\n");
        assertReadsAll(bytes, max);
    }

    public void testCounterResetsAfterTerminator() throws IOException {
        int max = 4;
        byte[] bytes = bytes("ab\ncd\nef\n");
        try (InputStream in = wrap(bytes, max)) {
            assertArrayEquals(bytes, in.readAllBytes());
        }
    }

    public void testCrossBufferCrLfDoesNotDoubleCount() throws IOException {
        // Record "xxxx\r\n" is 6 bytes total; with max=6 it fits exactly. The cross-buffer split puts the
        // '\r' at the tail of one read and the '\n' at the head of the next, exercising the pendingCr carry.
        int max = 6;
        byte[] bytes = bytes("xxxx\r\nyyy");
        try (
            TwoByteReadStream src = new TwoByteReadStream(bytes);
            InputStream in = new NdJsonRecordCappingInputStream(src, new NdJsonRecordSplitter(max))
        ) {
            assertArrayEquals(bytes, in.readAllBytes());
        }
    }

    public void testCrossBufferLoneCrResetsBeforeNextRecord() throws IOException {
        // Records "aaa\r" (4 bytes) and "bbbb" (4 bytes, EOF-terminated) both fit at max=4.
        int max = 4;
        byte[] bytes = bytes("aaa\rbbbb");
        try (
            TwoByteReadStream src = new TwoByteReadStream(bytes);
            InputStream in = new NdJsonRecordCappingInputStream(src, new NdJsonRecordSplitter(max))
        ) {
            assertArrayEquals(bytes, in.readAllBytes());
        }
    }

    public void testEofWithoutTerminatorRespectsCap() throws IOException {
        int max = 4;
        byte[] safe = bytes("x".repeat(max));
        try (InputStream in = wrap(safe, max)) {
            assertArrayEquals(safe, in.readAllBytes());
        }
        byte[] tooLarge = bytes("x".repeat(max + 1));
        IOException ex = expectThrows(IOException.class, () -> {
            try (InputStream in = wrap(tooLarge, max)) {
                in.readAllBytes();
            }
        });
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("max_record_size [" + max + "]"));
    }

    public void testSingleByteReadEnforcesSameCap() throws IOException {
        int max = 4;
        byte[] bytes = bytes("x".repeat(max + 1) + "\n");
        try (InputStream in = wrap(bytes, max)) {
            IOException ex = expectThrows(IOException.class, () -> {
                while (in.read() != -1) {
                    // drain byte-by-byte
                }
            });
            assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("max_record_size [" + max + "]"));
        }
    }

    public void testMarkResetDisabled() throws IOException {
        try (InputStream in = wrap(bytes("abc"), 8)) {
            assertFalse(in.markSupported());
            in.mark(100); // no-op
            IOException ex = expectThrows(IOException.class, in::reset);
            assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("not supported"));
        }
    }

    public void testNullSplitterFailsAtConstructionBoundary() {
        NullPointerException ex = expectThrows(
            NullPointerException.class,
            () -> new NdJsonRecordCappingInputStream(new ByteArrayInputStream(bytes("x")), null)
        );
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("recordSplitter"));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static NdJsonRecordCappingInputStream wrap(byte[] data, int maxRecordBytes) {
        return new NdJsonRecordCappingInputStream(new ByteArrayInputStream(data), new NdJsonRecordSplitter(maxRecordBytes));
    }

    private static void assertReadsAll(byte[] bytes, int maxRecordBytes) throws IOException {
        try (InputStream in = wrap(bytes, maxRecordBytes)) {
            assertArrayEquals(bytes, in.readAllBytes());
        }
    }

    private static void assertCapThrows(byte[] bytes, int maxRecordBytes) {
        IOException ex = expectThrows(IOException.class, () -> {
            try (InputStream in = wrap(bytes, maxRecordBytes)) {
                in.readAllBytes();
            }
        });
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("max_record_size [" + maxRecordBytes + "]"));
    }

    /** Returns at most two bytes per {@code read(byte[], off, len)} so cross-buffer carry paths get exercised. */
    private static final class TwoByteReadStream extends InputStream {
        private final byte[] data;
        private int pos;

        TwoByteReadStream(byte[] data) {
            this.data = data;
        }

        @Override
        public int read() {
            return pos < data.length ? (data[pos++] & 0xff) : -1;
        }

        @Override
        public int read(byte[] buf, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(2, Math.min(len, data.length - pos));
            System.arraycopy(data, pos, buf, off, n);
            pos += n;
            return n;
        }
    }
}
