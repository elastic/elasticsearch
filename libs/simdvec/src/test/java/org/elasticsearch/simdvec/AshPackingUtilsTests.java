/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.test.ESTestCase;

/**
 * Tests for {@link AshPackingUtils}.
 */
public class AshPackingUtilsTests extends ESTestCase {

    public void testPackMultiBitCodesRoundtrip() {
        int bitsPerDim = 2;
        int nDims = 13;
        int numLevels = 1 << bitsPerDim;
        float centerOffset = (numLevels - 1) / 2.0f;

        float[] codes = new float[nDims];
        int[] expectedUnsigned = new int[nDims];
        for (int j = 0; j < nDims; j++) {
            expectedUnsigned[j] = randomIntBetween(0, numLevels - 1);
            codes[j] = expectedUnsigned[j] - centerOffset;
        }

        byte[] packed = AshPackingUtils.pack(codes, bitsPerDim);
        int planeBytes = (nDims + 7) >>> 3;
        assertEquals(bitsPerDim * planeBytes, packed.length);

        for (int j = 0; j < nDims; j++) {
            int byteIdx = j >>> 3;
            int bitIdx = 7 - (j & 7);
            int decoded = 0;
            for (int p = 0; p < bitsPerDim; p++) {
                if ((packed[p * planeBytes + byteIdx] & (1 << bitIdx)) != 0) {
                    decoded |= (1 << p);
                }
            }
            assertEquals("Mismatch at dim " + j, expectedUnsigned[j], decoded);
        }
    }

    public void testPackedByteLength() {
        assertEquals(2, AshPackingUtils.packedLength(8, 2));
        assertEquals(4, AshPackingUtils.packedLength(9, 2));
        assertEquals(36, AshPackingUtils.packedLength(96, 3));
        assertEquals(12, AshPackingUtils.packedLength(96, 1));
    }
}
