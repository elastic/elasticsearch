/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.HexFormat;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Murmur3BytesRefUtilsTests extends ESTestCase {

    public void testEmptyArray() {
        BytesRef[] refs = new BytesRef[0];
        verify(refs);

        refs = new BytesRef[] { new BytesRef("".getBytes(UTF_8)) };
        verify(refs);

        refs = new BytesRef[] { new BytesRef("a".getBytes(UTF_8), 1, 0) };
        verify(refs);
    }

    // Tests with the number of bytes and groups less than the number of lanes/bytes in integer - 4.
    public void testSmallElement() {
        BytesRef[] refs = new BytesRef[] { new BytesRef("a".getBytes(UTF_8)) };
        verify(refs);

        refs = new BytesRef[] { new BytesRef("hi".getBytes(UTF_8)) };
        verify(refs);
        refs = new BytesRef[] { new BytesRef("hi".getBytes(UTF_8), 1, 1) };
        verify(refs);

        refs = new BytesRef[] { new BytesRef("hey".getBytes(UTF_8)) };
        verify(refs);
        refs = new BytesRef[] { new BytesRef("hey".getBytes(UTF_8), 2, 1) };
        verify(refs);

        refs = new BytesRef[] {
            new BytesRef("a".getBytes(UTF_8)),
            new BytesRef("hi".getBytes(UTF_8)),
            new BytesRef("hey".getBytes(UTF_8)) };
        verify(refs);
    }

    public void testSingleElement() {
        BytesRef[] refs = new BytesRef[] { new BytesRef("hello".getBytes(UTF_8)) };
        verify(refs);

        // align with number of LANEs
        refs = new BytesRef[] {
            new BytesRef("hello".getBytes(UTF_8)),
            new BytesRef("hello".getBytes(UTF_8)),
            new BytesRef("hello".getBytes(UTF_8)),
            new BytesRef("hello".getBytes(UTF_8)), };
        verify(refs);

        // one LANE plus TAIL of same length
        refs = new BytesRef[] {
            new BytesRef("hello".getBytes(UTF_8)),
            new BytesRef("hello".getBytes(UTF_8)),
            new BytesRef("hello".getBytes(UTF_8)),
            new BytesRef("hello".getBytes(UTF_8)),
            new BytesRef("hello".getBytes(UTF_8)),
            new BytesRef("hello".getBytes(UTF_8)), };
        verify(refs);
    }

    public void testSmallArray() {
        BytesRef[] refs = new BytesRef[] {
            new BytesRef("a".getBytes(UTF_8)),
            new BytesRef("ab".getBytes(UTF_8)),
            new BytesRef("abc".getBytes(UTF_8)),
            new BytesRef("abcd".getBytes(UTF_8)),
            new BytesRef("abcde".getBytes(UTF_8)) };
        verify(refs);
    }

    // more than lane number of each group length size
    public void testSmallishArray() {
        BytesRef[] refs = new BytesRef[] {
            new BytesRef("a".getBytes(UTF_8)),
            new BytesRef("ab".getBytes(UTF_8)),
            new BytesRef("abc".getBytes(UTF_8)),
            new BytesRef("abcd".getBytes(UTF_8)),
            new BytesRef("abcde".getBytes(UTF_8)),
            new BytesRef("a".getBytes(UTF_8)),
            new BytesRef("ab".getBytes(UTF_8)),
            new BytesRef("abc".getBytes(UTF_8)),
            new BytesRef("abcd".getBytes(UTF_8)),
            new BytesRef("abcde".getBytes(UTF_8)),
            new BytesRef("a".getBytes(UTF_8)),
            new BytesRef("ab".getBytes(UTF_8)),
            new BytesRef("abc".getBytes(UTF_8)),
            new BytesRef("abcd".getBytes(UTF_8)),
            new BytesRef("abcde".getBytes(UTF_8)),
            new BytesRef("a".getBytes(UTF_8)),
            new BytesRef("ab".getBytes(UTF_8)),
            new BytesRef("abc".getBytes(UTF_8)),
            new BytesRef("abcd".getBytes(UTF_8)),
            new BytesRef("abcde".getBytes(UTF_8)),
            new BytesRef("a".getBytes(UTF_8)),
            new BytesRef("ab".getBytes(UTF_8)),
            new BytesRef("abc".getBytes(UTF_8)),
            new BytesRef("abcd".getBytes(UTF_8)),
            new BytesRef("abcde".getBytes(UTF_8)) };
        verify(refs);
    }

    // tests fallthrough in switch
    public void testSmallArrayWithTails() {
        BytesRef[] refs = new BytesRef[] {
            br("16 c0 26 fb ff ff ff ff 9b"),
            br("6c 68 82 99 ff ff ff ff"),
            br("4d d1 bd 93 ff ff ff ff"),
            br("c1 33 bd 59 00 00 00 00 0b dd") };
        verify(refs);
    }

    public void testLargeArray() {
        int n = 1000;
        BytesRef[] refs = new BytesRef[n];
        for (int i = 0; i < n; i++) {
            int len = randomIntBetween(1, 20);
            byte[] b = new byte[len];
            random().nextBytes(b);
            refs[i] = new BytesRef(b);
        }
        verify(refs);
    }

    public void testBoundaryLengths() {
        BytesRef[] refs = new BytesRef[12];
        int[] lengths = { 0, 1, 2, 3, 4, 7, 8, 11, 12, 15, 16, 19 };

        for (int i = 0; i < lengths.length; i++) {
            byte[] b = new byte[lengths[i]];
            random().nextBytes(b);
            refs[i] = new BytesRef(b);
        }

        verify(refs);
    }

    private void verify(BytesRef[] refs) {
        int[] expected = new int[refs.length];
        for (int i = 0; i < refs.length; i++) {
            expected[i] = refs[i].hashCode();
        }

        int[] actual = new int[refs.length];
        Murmur3BytesRefUtils.hashAll(refs, actual);
        assertArrayEquals("SIMD hash results must match scalar reference", expected, actual);
    }

    private static final HexFormat HEX = HexFormat.of().withDelimiter(" ");

    private static BytesRef br(String hex) {
        return new BytesRef(HEX.parseHex(hex));
    }
}
