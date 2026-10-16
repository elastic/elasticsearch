/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.operation.hash.murmur3;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class Murmur3HashFunctionTests extends ESTestCase {

    public void testKnownValues() {
        assertHash(0x5a0cb7c3, "hell");
        assertHash(0xd7c31989, "hello");
        assertHash(0x22ab2984, "hello w");
        assertHash(0xdf0ca123, "hello wo");
        assertHash(0xe7744d61, "hello wor");
        assertHash(0xe07db09c, "The quick brown fox jumps over the lazy dog");
        assertHash(0x4e63d2ad, "The quick brown fox jumps over the lazy cog");
    }

    /** The BytesRef overload must match the String one for ASCII and non-ASCII, odd and even lengths, offsets, and over-scratch sizes. */
    public void testBytesRefMatchesString() {
        for (int i = 0; i < 200; i++) {
            final String input = switch (randomIntBetween(0, 2)) {
                case 0 -> randomAlphaOfLengthBetween(0, 40);
                case 1 -> randomAlphaOfLengthBetween(500, 700);
                case 2 -> randomRealisticUnicodeOfLengthBetween(1, 40);
                default -> throw new AssertionError();
            };
            final byte[] utf8 = input.getBytes(StandardCharsets.UTF_8);
            final int pad = randomIntBetween(0, 5);
            final byte[] padded = new byte[pad + utf8.length + pad];
            System.arraycopy(utf8, 0, padded, pad, utf8.length);
            assertEquals(Murmur3HashFunction.hash(input), Murmur3HashFunction.hash(new BytesRef(padded, pad, utf8.length)));
        }
    }

    private static void assertHash(int expected, String stringInput) {
        assertEquals(expected, Murmur3HashFunction.hash(stringInput));
    }
}
