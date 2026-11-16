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
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests for {@link AshProjectionMatrix}.
 */
public class AshProjectionMatrixTests extends ESTestCase {

    public void testDimAccessors() {
        int originalDim = 768;
        int nDims = 384;
        float[] wT = new float[originalDim * nDims];
        AshProjectionMatrix pm = new AshProjectionMatrix(wT, originalDim, nDims, true);
        assertEquals(originalDim, pm.originalDim());
        assertEquals(nDims, pm.nDims());
    }

    public void testSerializationRoundtrip() throws IOException {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(2, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);

        AshProjectionMatrix original = new AshProjectionMatrix(wT, originalDim, nDims, true);

        AshProjectionMatrix restored = writeAndRead(original);

        assertEquals(originalDim, restored.originalDim());
        assertEquals(nDims, restored.nDims());
        assertArrayEquals(wT, restored.wT(), 0f);
    }

    public void testProjectQueryApproximatesFloatProjection() {
        int originalDim = randomIntBetween(64, 1024);
        int nDims = randomIntBetween(32, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);
        AshUtils.qrOrthogonalize(wT, originalDim, nDims);
        AshProjectionMatrix pm = new AshProjectionMatrix(wT, originalDim, nDims, true);
        float[] query = AshUtils.randomGaussians(random(), originalDim);

        float[] expected = ESVectorUtil.matrixVectorMultiply(wT, nDims, originalDim, query);
        float[] projected = pm.projectQuery(query);

        double errorSq = 0;
        double expectedSq = 0;
        for (int j = 0; j < nDims; j++) {
            errorSq += Math.pow(projected[j] - expected[j], 2);
            expectedSq += Math.pow(expected[j], 2);
        }
        // int8 rounding of both the query and W^T, relative to the largest magnitude in each
        assertThat(Math.sqrt(errorSq / expectedSq), lessThan(0.03));
    }

    public void testProjectZeroQuery() {
        int originalDim = randomIntBetween(4, 100);
        int nDims = randomIntBetween(2, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);
        AshProjectionMatrix pm = new AshProjectionMatrix(wT, originalDim, nDims, true);

        assertThat(pm.projectQuery(new float[originalDim]), equalTo(new float[nDims]));
    }

    public void testByteSizeMatchesActualSerialized() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);

        AshProjectionMatrix pm = new AshProjectionMatrix(wT, originalDim, nDims, true);

        ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        try (ByteBuffersIndexOutput out = new ByteBuffersIndexOutput(dataOut, "test", "test")) {
            pm.write(out);
        }

        long expectedSize = Integer.BYTES * 2L + Byte.BYTES + (long) originalDim * nDims * Float.BYTES;
        assertEquals(expectedSize, pm.byteSize());
        assertEquals(pm.byteSize(), dataOut.size());
    }

    public void testLearnedFlagRoundTrips() throws IOException {
        int originalDim = randomIntBetween(4, 50);
        int nDims = randomIntBetween(2, originalDim);
        float[] wT = AshUtils.randomGaussians(random(), originalDim * nDims);

        for (boolean learned : new boolean[] { true, false }) {
            AshProjectionMatrix original = new AshProjectionMatrix(wT, originalDim, nDims, learned);
            assertEquals(learned, original.isLearned());
            AshProjectionMatrix restored = writeAndRead(original);
            assertEquals(learned, restored.isLearned());
            assertArrayEquals(wT, restored.wT(), 0f);
        }
    }

    public void testEmptyMatrix() throws IOException {
        AshProjectionMatrix pm = new AshProjectionMatrix(new float[0], 0, 0, true);
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
}
