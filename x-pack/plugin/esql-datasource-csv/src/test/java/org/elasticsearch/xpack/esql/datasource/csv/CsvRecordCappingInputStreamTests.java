/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class CsvRecordCappingInputStreamTests extends ESTestCase {

    public void testConstructorRejectsNonPositiveMaxRecordBytes() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CsvRecordCappingInputStream(new ByteArrayInputStream(new byte[0]), 0)
        );
        assertEquals("maxRecordBytes must be positive, got: 0", ex.getMessage());
    }

    public void testLfTerminatedRecordAtExactLimitPasses() throws IOException {
        // Splitter rule: boundary - recordStart + 1 > max. A record of `max - 1` content bytes plus the LF
        // terminator is `max` bytes total, which must pass.
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
        // "x"*max + "\r\n" has byte length max+2; the LF still belongs to the terminated record so the
        // cap must trip on the LF, matching CsvRecordSplitter.recordExceedsLimit + CsvLogicalRecordReader.
        int max = 8;
        byte[] bytes = bytes("x".repeat(max) + "\r\n");
        assertCapThrows(bytes, max);
    }

    public void testCrLfAtExactLimitPasses() throws IOException {
        // max == content + \r + \n. "x"*(max-2) + "\r\n" is exactly max bytes total.
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
        // Record "xxxx\r\n" is 6 bytes total; with max=6 the record fits exactly. The cross-buffer split puts
        // the '\r' at the tail of one read and the '\n' at the head of the next, so the LF must count toward
        // the same already-terminated-by-CR record without re-counting beyond the cap.
        int max = 6;
        byte[] bytes = bytes("xxxx\r\nyyy");
        try (TwoByteReadStream src = new TwoByteReadStream(bytes); InputStream in = new CsvRecordCappingInputStream(src, max)) {
            assertArrayEquals(bytes, in.readAllBytes());
        }
    }

    public void testCrossBufferLoneCrResetsBeforeNextRecord() throws IOException {
        // Records "aaa\r" (4 bytes) and "bbbb" (4 bytes, EOF-terminated) both fit at max=4. The '\r' lands at
        // the tail of a 2-byte read so the next read's first byte ('b') must start a fresh record.
        int max = 4;
        byte[] bytes = bytes("aaa\rbbbb");
        try (TwoByteReadStream src = new TwoByteReadStream(bytes); InputStream in = new CsvRecordCappingInputStream(src, max)) {
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
                    // drain byte-by-byte to exercise the single-byte read path
                }
            });
            assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("max_record_size [" + max + "]"));
        }
    }

    public void testEmptyRecordsBetweenTerminatorsResetCleanly() throws IOException {
        int max = 4;
        byte[] bytes = bytes("ab\n\n\ncd\n");
        try (InputStream in = wrap(bytes, max)) {
            assertArrayEquals(bytes, in.readAllBytes());
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static CsvRecordCappingInputStream wrap(byte[] data, int maxRecordBytes) {
        return new CsvRecordCappingInputStream(new ByteArrayInputStream(data), maxRecordBytes);
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

    /** Returns no more than two bytes per {@code read(byte[], off, len)} so cross-buffer carry paths get exercised. */
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
