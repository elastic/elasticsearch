/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Tests for {@link AshProjectionMatrix}.
 */
public class AshProjectionMatrixTests extends ESTestCase {

    public void testDimAccessors() {
        int originalDim = 768;
        int nDims = 384;
        float[][] w = new float[originalDim][nDims];
        AshProjectionMatrix pm = new AshProjectionMatrix(w);
        assertEquals(originalDim, pm.originalDim());
        assertEquals(nDims, pm.nDims());
    }

    public void testTransposeCorrectness() {
        int originalDim = 4;
        int nDims = 3;
        float[][] w = { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 }, { 10, 11, 12 } };
        AshProjectionMatrix pm = new AshProjectionMatrix(w);
        float[][] wT = pm.wT();

        // wT should be (nDims x originalDim)
        assertEquals(nDims, wT.length);
        assertEquals(originalDim, wT[0].length);

        // Verify transpose: wT[j][i] == w[i][j]
        for (int i = 0; i < originalDim; i++) {
            for (int j = 0; j < nDims; j++) {
                assertEquals(w[i][j], wT[j][i], 0f);
            }
        }
    }

    public void testTransposeLazyAndCached() {
        float[][] w = { { 1, 2 }, { 3, 4 } };
        AshProjectionMatrix pm = new AshProjectionMatrix(w);
        float[][] wT1 = pm.wT();
        float[][] wT2 = pm.wT();
        assertSame(wT1, wT2);
    }

    public void testSerializationRoundtrip() throws IOException {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(2, originalDim);
        float[][] w = randomMatrix(originalDim, nDims);

        AshProjectionMatrix original = new AshProjectionMatrix(w);

        AshProjectionMatrix restored = writeAndRead(original);

        assertEquals(originalDim, restored.originalDim());
        assertEquals(nDims, restored.nDims());
        assertMatrixEquals(w, restored.w());
    }

    public void testByteSizeMatchesActualSerialized() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        float[][] w = randomMatrix(originalDim, nDims);

        AshProjectionMatrix pm = new AshProjectionMatrix(w);

        ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput out = new ByteBuffersIndexOutput(dataOut, "test", "test")) {
            pm.write(out);
        }

        long expectedSize = Integer.BYTES * 2L + (long) originalDim * nDims * Float.BYTES;
        assertEquals(expectedSize, pm.byteSize());
    }

    public void testEmptyMatrix() throws IOException {
        float[][] w = new float[0][0];
        AshProjectionMatrix pm = new AshProjectionMatrix(w);
        assertEquals(0, pm.originalDim());
        assertEquals(0, pm.nDims());

        AshProjectionMatrix restored = writeAndRead(pm);
        assertEquals(0, restored.originalDim());
        assertEquals(0, restored.nDims());
    }

    private AshProjectionMatrix writeAndRead(AshProjectionMatrix pm) throws IOException {
        ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput out = new ByteBuffersIndexOutput(dataOut, "test", "test")) {
            pm.write(out);
        }
        ByteBuffersIndexInput in = new ByteBuffersIndexInput(dataOut.toDataInput(), "test");
        return AshProjectionMatrix.read(in);
    }

    private float[][] randomMatrix(int rows, int cols) {
        float[][] m = new float[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                m[i][j] = (float) random().nextGaussian();
            }
        }
        return m;
    }

    private void assertMatrixEquals(float[][] expected, float[][] actual) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i].length, actual[i].length);
            for (int j = 0; j < expected[i].length; j++) {
                assertEquals(expected[i][j], actual[i][j], 0f);
            }
        }
    }
}
