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
        float[] w = new float[originalDim * nDims];
        AshProjectionMatrix pm = new AshProjectionMatrix(w, originalDim, nDims);
        assertEquals(originalDim, pm.originalDim());
        assertEquals(nDims, pm.nDims());
    }

    public void testTransposeCorrectness() {
        int originalDim = 4;
        int nDims = 3;
        float[] w = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
        AshProjectionMatrix pm = new AshProjectionMatrix(w, originalDim, nDims);
        float[] wT = pm.wT();

        // wT should be (nDims x originalDim)
        assertEquals(nDims * originalDim, wT.length);

        // Verify transpose: wT[j][i] == w[i][j] (i.e. wT[j*originalDim+i] == w[i*nDims+j])
        for (int i = 0; i < originalDim; i++) {
            for (int j = 0; j < nDims; j++) {
                assertEquals(w[i * nDims + j], wT[j * originalDim + i], 0f);
            }
        }
    }

    public void testTransposeLazyAndCached() {
        float[] w = { 1, 2, 3, 4 };
        AshProjectionMatrix pm = new AshProjectionMatrix(w, 2, 2);
        float[] wT1 = pm.wT();
        float[] wT2 = pm.wT();
        assertSame(wT1, wT2);
    }

    public void testSerializationRoundtrip() throws IOException {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(2, originalDim);
        float[] w = randomMatrix(originalDim, nDims);

        AshProjectionMatrix original = new AshProjectionMatrix(w, originalDim, nDims);

        AshProjectionMatrix restored = writeAndRead(original);

        assertEquals(originalDim, restored.originalDim());
        assertEquals(nDims, restored.nDims());
        assertArrayEquals(w, restored.w(), 0f);
    }

    public void testByteSizeMatchesActualSerialized() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        float[] w = randomMatrix(originalDim, nDims);

        AshProjectionMatrix pm = new AshProjectionMatrix(w, originalDim, nDims);

        ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput out = new ByteBuffersIndexOutput(dataOut, "test", "test")) {
            pm.write(out);
        }

        long expectedSize = Integer.BYTES * 2L + (long) originalDim * nDims * Float.BYTES;
        assertEquals(expectedSize, pm.byteSize());
    }

    public void testEmptyMatrix() throws IOException {
        AshProjectionMatrix pm = new AshProjectionMatrix(new float[0], 0, 0);
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

    private float[] randomMatrix(int rows, int cols) {
        float[] m = new float[rows * cols];
        for (int i = 0; i < rows * cols; i++) {
            m[i] = (float) random().nextGaussian();
        }
        return m;
    }
}
