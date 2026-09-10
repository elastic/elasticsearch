/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    public void testApplyTransformToBytes() {
        int dim = random().nextInt(128, 1024);
        int blockDim = random().nextInt(8, dim);
        Preconditioner preconditioner = Preconditioner.createPreconditioner(dim, blockDim);

        // Generate a random byte vector
        byte[] byteVector = new byte[dim];
        random().nextBytes(byteVector);

        // Apply the byte→byte transform
        byte[] byteOut = new byte[dim];
        float[] scratch = new float[dim];
        preconditioner.applyTransformToBytes(byteVector, byteOut, scratch);

        // Apply the byte→float transform for comparison
        float[] floatOut = new float[dim];
        preconditioner.applyTransform(byteVector, floatOut);

        // The byte output should be the clamped/rounded version of the float output
        for (int i = 0; i < dim; i++) {
            byte expected = (byte) Math.clamp(Math.round(floatOut[i]), -128, 127);
            assertEquals("Mismatch at dimension " + i, expected, byteOut[i]);
        }

        // Verify the float scratch buffer was populated (same as applyTransform output)
        for (int i = 0; i < dim; i++) {
            assertEquals("Scratch mismatch at dimension " + i, floatOut[i], scratch[i], 1e-6f);
        }
    }

    public void testPreconditionFloatVectorsInPlaceMatchesApplyTransform() {
        int dim = 11;
        int blockDim = 4;
        Preconditioner preconditioner = Preconditioner.createPreconditioner(dim, blockDim);

        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            float[] vector = new float[dim];
            for (int j = 0; j < dim; j++) {
                vector[j] = random().nextFloat() * 2 - 1;
            }
            vectors.add(vector);
        }

        float[][] expected = new float[vectors.size()][dim];
        for (int i = 0; i < vectors.size(); i++) {
            preconditioner.applyTransform(vectors.get(i), expected[i]);
        }

        preconditioner.preconditionVectorsInPlace(vectors, VectorEncoding.FLOAT32);

        for (int i = 0; i < vectors.size(); i++) {
            assertArrayEquals("Mismatch for vector " + i, expected[i], vectors.get(i), 1e-6f);
        }
    }

    public void testPreconditionByteVectorsInPlaceMatchesApplyTransformToBytes() {
        int dim = 11;
        int blockDim = 4;
        Preconditioner preconditioner = Preconditioner.createPreconditioner(dim, blockDim);

        List<byte[]> vectors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            byte[] vector = new byte[dim];
            random().nextBytes(vector);
            vectors.add(vector);
        }

        byte[][] expected = new byte[vectors.size()][dim];
        byte[] byteScratch = new byte[dim];
        float[] floatScratch = new float[dim];
        for (int i = 0; i < vectors.size(); i++) {
            preconditioner.applyTransformToBytes(vectors.get(i), byteScratch, floatScratch);
            System.arraycopy(byteScratch, 0, expected[i], 0, dim);
        }

        preconditioner.preconditionVectorsInPlace(vectors, VectorEncoding.BYTE);

        for (int i = 0; i < vectors.size(); i++) {
            assertArrayEquals("Mismatch for vector " + i, expected[i], vectors.get(i));
        }
    }
}
