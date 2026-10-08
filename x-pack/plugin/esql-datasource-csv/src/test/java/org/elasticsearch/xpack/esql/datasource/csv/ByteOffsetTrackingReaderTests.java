/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;

public class ByteOffsetTrackingReaderTests extends ESTestCase {

    /**
     * 'a' (1 byte), 'é' U+00E9 (2), '…' U+2026 (3), '😀' U+1F600 (4 bytes / surrogate pair = 2 chars),
     * 'b' (1). Record starts never fall inside a code point, so the low surrogate's offset is not queried.
     */
    public void testByteOffsetsAcrossMultibyte() throws Exception {
        String s = "aé…😀b";
        assertEquals(6, s.length()); // 5 code points, the supplementary one is two chars
        assertEquals(11, s.getBytes(StandardCharsets.UTF_8).length); // 1+2+3+4+1

        long base = 100L;
        ByteOffsetTrackingReader reader = new ByteOffsetTrackingReader(new StringReader(s), base);
        char[] buf = new char[16];
        int produced = 0;
        int n;
        while ((n = reader.read(buf, 0, buf.length)) >= 0) {
            produced += n;
        }
        assertEquals(6, produced);

        assertEquals(base + 0, reader.byteOffsetAtChar(0)); // 'a'
        assertEquals(base + 1, reader.byteOffsetAtChar(1)); // 'é'
        assertEquals(base + 3, reader.byteOffsetAtChar(2)); // '…'
        assertEquals(base + 6, reader.byteOffsetAtChar(3)); // high surrogate of '😀'
        assertEquals(base + 10, reader.byteOffsetAtChar(5)); // 'b' (skip char 4, mid code point)
        assertEquals(base + 11, reader.byteOffsetAtChar(6)); // end of input
    }

    public void testAsciiOffsetsEqualCharOffsets() throws Exception {
        String s = "id,name\n1,alice\n2,bob\n";
        ByteOffsetTrackingReader reader = new ByteOffsetTrackingReader(new StringReader(s), 0L);
        char[] buf = new char[4];
        while (reader.read(buf, 0, buf.length) >= 0) {
            // drain in small reads to exercise the refill path
        }
        // Pure ASCII: byte offset == char offset at every record start.
        assertEquals(0L, reader.byteOffsetAtChar(0));
        assertEquals(8L, reader.byteOffsetAtChar(8)); // start of "1,alice"
        assertEquals(16L, reader.byteOffsetAtChar(16)); // start of "2,bob"
        assertEquals((long) s.length(), reader.byteOffsetAtChar(s.length()));
    }

    /**
     * Many non-ASCII characters interleaved with ASCII, exercising the sparse "extra bytes" event buffer's
     * growth + compaction (well past its initial capacity) and the O(1)-per-ASCII-run advance. Each record is
     * {@code "é\n"} (an 'é' = 2 bytes + a newline = 1 byte = 3 bytes / 2 chars), so record {@code r} starts at
     * char {@code 2r} and byte {@code 3r}.
     */
    public void testManyMultibyteEventsGrowAndCompact() throws Exception {
        int records = 200;
        StringBuilder sb = new StringBuilder();
        for (int r = 0; r < records; r++) {
            sb.append('é').append('\n'); // 'é' then newline
        }
        String s = sb.toString();
        ByteOffsetTrackingReader reader = new ByteOffsetTrackingReader(new StringReader(s), 0L);
        char[] buf = new char[7]; // odd, small buffer -> refills split code points across reads
        while (reader.read(buf, 0, buf.length) >= 0) {
        }
        for (int r = 0; r <= records; r++) {
            assertEquals("record " + r + " byte start", 3L * r, reader.byteOffsetAtChar(2L * r));
        }
    }

    public void testNonDecreasingQueryContractEnforced() throws Exception {
        ByteOffsetTrackingReader reader = new ByteOffsetTrackingReader(new StringReader("abcd"), 0L);
        char[] buf = new char[8];
        while (reader.read(buf, 0, buf.length) >= 0) {
        }
        reader.byteOffsetAtChar(3);
        expectThrows(IllegalArgumentException.class, () -> reader.byteOffsetAtChar(2));
    }

    /**
     * The tripwire condition: a MALFORMED byte sequence (Latin-1 0xE9 in a "UTF-8" stream) is replaced by
     * the decoder with one U+FFFD, which the tracker counts at the replacement char's width (3 bytes) --
     * not the 1 actual malformed byte -- so the inferred end offset diverges from the true byte count.
     * {@code CsvBatchIterator.emitPerStripe} compares inferred vs actual after a full drain and safe-misses
     * stripe capture on mismatch; this pins the divergence the tripwire keys on, and that well-formed input
     * does NOT trip it.
     */
    public void testMalformedUtf8SkewsInferredEndOffset() throws Exception {
        byte[] wellFormed = "a,b\u00e9c\n".getBytes(StandardCharsets.UTF_8); // 'é' properly encoded (2 bytes)
        byte[] malformed = new byte[] { 'a', ',', 'b', (byte) 0xE9, 'c', '\n' }; // bare Latin-1 é (1 byte, invalid UTF-8)

        assertEquals("well-formed input: inferred end == actual bytes", wellFormed.length, drainInferredEnd(wellFormed));
        long inferred = drainInferredEnd(malformed);
        assertNotEquals("malformed input: inferred end diverges from actual bytes (the tripwire condition)", malformed.length, inferred);
        assertEquals("U+FFFD counted at 3 bytes vs 1 actual -> inferred overshoots by 2", malformed.length + 2, inferred);
    }

    /** Drains {@code bytes} through an UTF-8 decoder (REPLACE on malformed) + tracker; returns the inferred end offset (base 0). */
    private static long drainInferredEnd(byte[] bytes) throws Exception {
        try (
            var in = new java.io.InputStreamReader(new java.io.ByteArrayInputStream(bytes), StandardCharsets.UTF_8);
            ByteOffsetTrackingReader reader = new ByteOffsetTrackingReader(in, 0L)
        ) {
            char[] buf = new char[16];
            while (reader.read(buf, 0, buf.length) >= 0) {
            }
            return reader.inferredEndOffset();
        }
    }
}
