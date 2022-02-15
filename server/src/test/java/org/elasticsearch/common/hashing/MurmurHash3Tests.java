/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.hashing;

import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.test.ESTestCase;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class MurmurHash3Tests extends ESTestCase {
    public void testKnownValues() throws UnsupportedEncodingException {
        assertHash(0x629942693e10f867L, 0x92db0b82baeb5347L, "hell", 0);
        assertHash(0xa78ddff5adae8d10L, 0x128900ef20900135L, "hello", 1);
        assertHash(0x8a486b23f422e826L, 0xf962a2c58947765fL, "hello ", 2);
        assertHash(0x2ea59f466f6bed8cL, 0xc610990acc428a17L, "hello w", 3);
        assertHash(0x79f6305a386c572cL, 0x46305aed3483b94eL, "hello wo", 4);
        assertHash(0xc2219d213ec1f1b5L, 0xa1d8e2e0a52785bdL, "hello wor", 5);
        assertHash(0xe34bbc7bbc071b6cL, 0x7a433ca9c49a9347L, "The quick brown fox jumps over the lazy dog", 0);
        assertHash(0x658ca970ff85269aL, 0x43fee3eaa68e5c3eL, "The quick brown fox jumps over the lazy cog", 0);
    }

    private static void assertHash(long lower, long upper, String inputString, long seed) {
        byte[] bytes = inputString.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 expected = new MurmurHash3.Hash128();
        expected.h1 = lower;
        expected.h2 = upper;
        assertHash(expected, MurmurHash3.hash128(bytes, 0, bytes.length, seed, new MurmurHash3.Hash128()));
    }

    private static void assertHash(MurmurHash3.Hash128 expected, MurmurHash3.Hash128 actual) {
        assertEquals(expected.h1, actual.h1);
        assertEquals(expected.h2, actual.h2);
    }
}
