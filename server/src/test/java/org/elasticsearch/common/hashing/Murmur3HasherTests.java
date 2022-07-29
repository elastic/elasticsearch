/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.hashing;

import org.elasticsearch.common.hash.Murmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;

public class Murmur3HasherTests extends ESTestCase {

    public void testKnownValues() {
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
        MurmurHash3.Hash128 expected = new MurmurHash3.Hash128();
        expected.h1 = lower;
        expected.h2 = upper;

        byte[] bytes = inputString.getBytes(StandardCharsets.UTF_8);
        Murmur3Hasher mh = new Murmur3Hasher(seed);
        mh.update(bytes);
        MurmurHash3.Hash128 actual = Murmur3Hasher.toHash128(mh.digest());
        assertHash(expected, actual);
    }

    private static void assertHash(MurmurHash3.Hash128 expected, MurmurHash3.Hash128 actual) {
        assertEquals(expected.h1, actual.h1);
        assertEquals(expected.h2, actual.h2);
    }

    public void testSingleVsSequentialMurmur3() {
        final String inputString = randomAlphaOfLengthBetween(2000, 3000);
        final int numSplits = randomIntBetween(2, 100); // should produce a good number of byte arrays both longer and shorter than 16
        final int[] splits = new int[numSplits];
        int totalLength = 0;
        for (int k = 0; k < numSplits - 1; k++) {
            splits[k] = randomIntBetween(1, Math.max(2, inputString.length() / numSplits * 2));
            totalLength += splits[k];
        }
        splits[numSplits - 1] = inputString.length() - totalLength;
        totalLength = 0;
        byte[][] splitBytes = new byte[numSplits][];
        for (int k = 0; k < numSplits - 1; k++) {
            int end = Math.min(totalLength + splits[k], inputString.length());
            if (totalLength < end) {
                splitBytes[k] = inputString.substring(totalLength, end).getBytes(StandardCharsets.UTF_8);
            } else {
                splitBytes[k] = new byte[0];
            }
            totalLength += splits[k];
        }
        if (totalLength < inputString.length()) {
            splitBytes[numSplits - 1] = inputString.substring(totalLength).getBytes(StandardCharsets.UTF_8);
        } else {
            splitBytes[numSplits - 1] = new byte[0];
        }

        final long seed = randomLong();
        final byte[] allBytes = inputString.getBytes(StandardCharsets.UTF_8);
        final MurmurHash3.Hash128 singleHash = MurmurHash3.hash128(allBytes, 0, allBytes.length, seed, new MurmurHash3.Hash128());

        Murmur3Hasher mh = new Murmur3Hasher(seed);
        totalLength = 0;
        for (int k = 0; k < numSplits; k++) {
            totalLength += splitBytes[k].length;
            if (totalLength <= inputString.length()) {
                mh.update(splitBytes[k]);
            }
        }
        MurmurHash3.Hash128 sequentialHash = Murmur3Hasher.toHash128(mh.digest());
        assertThat(singleHash, equalTo(sequentialHash));
    }
}
