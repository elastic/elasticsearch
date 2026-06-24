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

    public void testNonDecreasingQueryContractEnforced() throws Exception {
        ByteOffsetTrackingReader reader = new ByteOffsetTrackingReader(new StringReader("abcd"), 0L);
        char[] buf = new char[8];
        while (reader.read(buf, 0, buf.length) >= 0) {
        }
        reader.byteOffsetAtChar(3);
        expectThrows(IllegalArgumentException.class, () -> reader.byteOffsetAtChar(2));
    }
}
