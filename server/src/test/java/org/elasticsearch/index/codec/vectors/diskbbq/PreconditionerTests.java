/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PreconditionerTests extends ESTestCase {
    public void testRandomProviderConfigurations() throws IOException {
        int dim = random().nextInt(128, 1024);

        int corpusLen = random().nextInt(100, 200);
        float[][] corpus = new float[corpusLen][];
        for (int i = 0; i < corpusLen; i++) {
            corpus[i] = new float[dim];
            for (int j = 0; j < dim; j++) {
                if (j > 320) {
                    corpus[i][j] = 0f;
                } else {
                    corpus[i][j] = random().nextFloat();
                }
            }
        }

        float[] query = new float[dim];
        for (int i = 0; i < dim; i++) {
            query[i] = random().nextFloat();
        }

        int blockDim = random().nextInt(8, dim);

        Preconditioner preconditioner = Preconditioner.createPreconditioner(dim, blockDim);

        float[] out = new float[dim];
        preconditioner.applyTransform(query, out);

        assertEquals(blockDim, preconditioner.blockDim);
        assertEquals(dim / blockDim + (dim % blockDim == 0 ? 0 : 1), preconditioner.permutationMatrix.length);
        assertEquals(Math.min(blockDim, dim), preconditioner.permutationMatrix[0].length);
        if (dim % blockDim == 0) {
            assertEquals(blockDim, preconditioner.permutationMatrix[preconditioner.permutationMatrix.length - 1].length);
        } else {
            assertEquals(
                dim - (long) (dim / blockDim) * blockDim,
                preconditioner.permutationMatrix[preconditioner.permutationMatrix.length - 1].length
            );
        }
        assertEquals(dim / blockDim + (dim % blockDim == 0 ? 0 : 1), preconditioner.blocks.length);
        assertEquals(Math.min(blockDim, dim), preconditioner.blocks[0].length);
        assertEquals(Math.min(blockDim, dim), preconditioner.blocks[0][0].length);

        // verify can be written and read back
        ByteBuffersDataOutput byteBuffersDataOutput = new ByteBuffersDataOutput();
        IndexOutput output = new ByteBuffersIndexOutput(byteBuffersDataOutput, "test", "test");
        preconditioner.write(output);
        Preconditioner.read(new ByteBuffersIndexInput(byteBuffersDataOutput.toDataInput(), "test"));
    }

    /**
     * Verifies that the byte applyTransform path produces identical results to manually
     * widening bytes to float and calling the float applyTransform path.
     * Exercises both single-block (matrixVectorMultiplyBytes) and multi-block (applyMultiBlock
     * with lambda) paths via randomized blockDim.
     */
    public void testByteFloatEquivalency() {
        int dim = random().nextInt(128, 1024);
        int blockDim = random().nextInt(8, dim);

        Preconditioner preconditioner = Preconditioner.createPreconditioner(dim, blockDim);

        byte[] byteVector = new byte[dim];
        random().nextBytes(byteVector);

        // Path A: byte applyTransform (uses matrixVectorMultiplyBytes or applyMultiBlock with byte lambda)
        float[] byteOut = new float[dim];
        preconditioner.applyTransform(byteVector, byteOut);

        // Path B: manually widen bytes to float, then float applyTransform
        float[] floatVector = new float[dim];
        for (int i = 0; i < dim; i++) {
            floatVector[i] = byteVector[i];
        }
        float[] floatOut = new float[dim];
        preconditioner.applyTransform(floatVector, floatOut);

        // Both paths must produce identical output — the arithmetic is the same,
        // only the source element access differs.
        assertArrayEquals(floatOut, byteOut, 0f);
    }
}
