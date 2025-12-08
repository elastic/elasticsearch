/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

// TODO: apply to other formats
// TODO: instead of manually having to indicate preconditioning add the ability to decide when to use it given the data on the segment

public class PreconditioningProvider {

    final int blockDim;
    final int[][] permutationMatrix;
    final float[][][] blocks;

    public PreconditioningProvider(int blockDim, int vectorDim) {
        this.blockDim = blockDim;
        Random random = new Random(42L);
        blocks = PreconditioningProvider.generateRandomOrthogonalMatrix(vectorDim, blockDim, random);
        int[] dimBlocks = new int[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            dimBlocks[i] = blocks[i].length;
        }
        permutationMatrix = PreconditioningProvider.createPermutationMatrixRandomly(vectorDim, dimBlocks, random);
    }

    private PreconditioningProvider(int blockDim, float[][][] blocks, int[][] permutationMatrix) {
        this.blockDim = blockDim;
        this.permutationMatrix = permutationMatrix;
        this.blocks = blocks;
    }

    public float[] applyPreconditioningTransform(float[] vector) {
        assert vector != null;

        float[] out = new float[vector.length];

        if (blocks.length == 1) {
            matrixVectorMultiply(blocks[0], vector, out);
            return out;
        }

        int blockIdx = 0;
        float[] x = new float[blockDim];
        float[] blockOut = new float[blockDim];
        for (int j = 0; j < blocks.length; j++) {
            float[][] block = blocks[j];
            int blockDim = blocks[j].length;
            // blockDim is only ever smaller for the tail
            if (blockDim != this.blockDim) {
                x = new float[blockDim];
                blockOut = new float[blockDim];
            }
            for (int k = 0; k < permutationMatrix[j].length; k++) {
                int idx = permutationMatrix[j][k];
                x[k] = vector[idx];
            }
            matrixVectorMultiply(block, x, blockOut);
            System.arraycopy(blockOut, 0, out, blockIdx, blockDim);
            blockIdx += blockDim;
        }

        return out;
    }

    public void write(IndexOutput out) throws IOException {
        int rem = blockDim;
        if (blocks[blocks.length - 1].length != blockDim) {
            rem = blocks[blocks.length - 1].length;
        }

        out.writeInt(blocks.length);
        out.writeInt(blockDim);
        out.writeInt(rem);
        out.writeInt(permutationMatrix.length);

        final ByteBuffer blockBuffer = ByteBuffer.allocate(
            (blocks.length - 1) * blockDim * blockDim * Float.BYTES + rem * rem * Float.BYTES
        ).order(ByteOrder.LITTLE_ENDIAN);
        FloatBuffer floatBuffer = blockBuffer.asFloatBuffer();
        for (int i = 0; i < blocks.length; i++) {
            for (int j = 0; j < blocks[i].length; j++) {
                floatBuffer.put(blocks[i][j]);
            }
        }
        out.writeBytes(blockBuffer.array(), blockBuffer.array().length);

        for (int i = 0; i < permutationMatrix.length; i++) {
            out.writeInt(permutationMatrix[i].length);
            final ByteBuffer permBuffer = ByteBuffer.allocate(permutationMatrix[i].length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            permBuffer.asIntBuffer().put(permutationMatrix[i]);
            out.writeBytes(permBuffer.array(), permBuffer.array().length);
        }
    }

    public static PreconditioningProvider read(IndexInput input) throws IOException {
        int blocksLen = input.readInt();
        int blockDim = input.readInt();
        int rem = input.readInt();
        int permutationMatrixLen = input.readInt();

        float[][][] blocks = new float[blocksLen][][];
        int[][] permutationMatrix = new int[permutationMatrixLen][];

        for (int i = 0; i < blocksLen; i++) {
            int blockLen = blocksLen - 1 == i ? rem : blockDim;
            blocks[i] = new float[blockLen][blockLen];
            for (int j = 0; j < blockLen; j++) {
                input.readFloats(blocks[i][j], 0, blockLen);
            }
        }

        for (int i = 0; i < permutationMatrix.length; i++) {
            int permutationMatrixSubLen = input.readInt();
            permutationMatrix[i] = new int[permutationMatrixSubLen];
            input.readInts(permutationMatrix[i], 0, permutationMatrixSubLen);
        }

        return new PreconditioningProvider(blockDim, blocks, permutationMatrix);
    }

    // TODO: write Panama version of this
    static void modifiedGramSchmidt(float[][] m) {
        assert m.length == m[0].length;
        int dim = m.length;
        for (int i = 0; i < dim; i++) {
            double norm = 0.0;
            for (float v : m[i]) {
                norm += v * v;
            }
            norm = Math.sqrt(norm);
            if (norm == 0.0f) {
                continue;
            }
            for (int j = 0; j < dim; j++) {
                m[i][j] /= (float) norm;
            }
            for (int k = i + 1; k < dim; k++) {
                double dotik = 0.0;
                for (int j = 0; j < dim; j++) {
                    dotik += m[i][j] * m[k][j];
                }
                for (int j = 0; j < dim; j++) {
                    m[k][j] -= (float) (dotik * m[i][j]);
                }
            }
        }
    }

    private static void randomFill(Random random, float[][] m) {
        for (int i = 0; i < m.length; ++i) {
            for (int j = 0; j < m[i].length; ++j) {
                m[i][j] = (float) random.nextGaussian();
            }
        }
    }

    private static float[][][] generateRandomOrthogonalMatrix(int dim, int blockDim, Random random) {
        blockDim = Math.min(dim, blockDim);
        int nBlocks = dim / blockDim;
        int rem = dim % blockDim;

        float[][][] blocks = new float[nBlocks + (rem > 0 ? 1 : 0)][][];

        for (int i = 0; i < nBlocks; i++) {
            float[][] m = new float[blockDim][blockDim];
            randomFill(random, m);
            modifiedGramSchmidt(m);
            blocks[i] = m;
        }

        if (rem != 0) {
            float[][] m = new float[rem][rem];
            randomFill(random, m);
            modifiedGramSchmidt(m);
            blocks[nBlocks] = m;
        }

        return blocks;
    }

    private static void matrixVectorMultiply(float[][] m, float[] x, float[] out) {
        assert m.length == x.length;
        assert m.length == out.length;
        int dim = out.length;
        for (int i = 0; i < dim; i++) {
            out[i] = VectorUtil.dotProduct(m[i], x);
        }
    }

    private static int minElementIndex(float[] array) {
        int minIndex = 0;
        float minValue = array[0];
        for (int i = 1; i < array.length; i++) {
            if (array[i] < minValue) {
                minValue = array[i];
                minIndex = i;
            }
        }
        return minIndex;
    }

    private static int[][] createPermutationMatrixRandomly(int dim, int[] dimBlocks, Random random) {
        // Randomly assign dimensions to blocks.
        List<Integer> indices = new ArrayList<>(dim);
        for (int i = 0; i < dim; i++) {
            indices.add(i);
        }
        Collections.shuffle(indices, random);

        int[][] permutationMatrix = new int[dimBlocks.length][];
        int pos = 0;
        for (int i = 0; i < dimBlocks.length; i++) {
            permutationMatrix[i] = new int[dimBlocks[i]];
            for (int j = 0; j < dimBlocks[i]; j++) {
                permutationMatrix[i][j] = indices.get(pos++);
            }
            Arrays.sort(permutationMatrix[i]);
        }

        return permutationMatrix;
    }

    private static int[][] createPermutationMatrixWEqualVariance(int[] dimBlocks, FloatVectorValues vectors) throws IOException {
        int dim = vectors.dimension();

        if (dimBlocks.length == 1) {
            int[] indices = new int[dim];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = i;
            }
            return new int[][] { indices };
        }

        // Use a greedy approach to pick assignments to blocks that equalizes their variance.
        float[] means = new float[dim];
        float[] variances = new float[dim];
        int[] n = new int[dim];

        // TODO: write Panama version of this
        for (int i = 0; i < vectors.size(); ++i) {
            float[] vector = vectors.vectorValue(i);
            for (int j = 0; j < dim; j++) {
                float value = vector[j];
                n[j]++;
                double delta = value - means[j];
                means[j] += (float) (delta / n[j]);
                variances[j] += (float) (delta * (value - means[j]));
            }
        }

        int[] indices = new int[dim];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        new IntSorter(indices, i -> NumericUtils.floatToSortableInt(variances[i])).sort(0, indices.length);

        int[][] permutationMatrix = new int[dimBlocks.length][];
        for (int i = 0; i < permutationMatrix.length; i++) {
            permutationMatrix[i] = new int[dimBlocks[i]];
        }
        float[] cumulativeVariances = new float[dimBlocks.length];
        int[] jthIdx = new int[permutationMatrix.length];
        for (int i : indices) {
            int j = minElementIndex(cumulativeVariances);
            permutationMatrix[j][jthIdx[j]++] = i;
            cumulativeVariances[j] = (jthIdx[j] == dimBlocks[j] ? Float.MAX_VALUE : cumulativeVariances[j] + variances[i]);
        }
        for (int[] matrix : permutationMatrix) {
            Arrays.sort(matrix);
        }

        return permutationMatrix;
    }

}
