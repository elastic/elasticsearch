/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.elasticsearch.test.ESTestCase;

public class BQSpaceUtilsTests extends ESTestCase {

    public void testIntegerTransposeHalfByte() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(15);
        }
        int length = 4 * BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        BQSpaceUtils.transposeHalfByteLegacy(toPack, packedLegacy);
        BQSpaceUtils.transposeHalfByte(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testByteTransposeHalfByte() {
        int dims = randomIntBetween(16, 2048);
        byte[] toPack = new byte[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = (byte) randomInt(15);
        }
        int length = 4 * BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        BQSpaceUtils.transposeHalfByteLegacy(toPack, packedLegacy);
        BQSpaceUtils.transposeHalfByte(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }
}
