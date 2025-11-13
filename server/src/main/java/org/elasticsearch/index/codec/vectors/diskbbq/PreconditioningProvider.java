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
import java.util.Arrays;
import java.util.Random;

// FIXME: recall drops to 0; fix this
// TODO: add in random permutation matrix instead of variance based and compare
// TODO: apply to other formats

public class PreconditioningProvider {

    final int blockDim;
    final int[][] permutationMatrix;
    final float[][][] blocks;

    public PreconditioningProvider(int blockDim, FloatVectorValues vectors) throws IOException {
        this.blockDim = blockDim;
        int dim = vectors.dimension();
        blocks = PreconditioningProvider.generateRandomOrthogonalMatrix(dim, blockDim);
        int[] dimBlocks = new int[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            dimBlocks[i] = blocks[i].length;
        }
        permutationMatrix = PreconditioningProvider.createPermutationMatrix(dimBlocks, vectors);
    }

    private PreconditioningProvider(int blockDim, float[][][] blocks, int[][] permutationMatrix) {
        this.blockDim = blockDim;
        this.permutationMatrix = permutationMatrix;
        this.blocks = blocks;
    }

    public void applyPreconditioningTransform(float[] vector) {
        assert vector != null;

        int dim = vector.length;

        if (blocks.length == 1) {
            int blockDim = blocks[0].length;
            float[] tmp = new float[blockDim];
            matrixVectorMultiply(blocks[0], vector, tmp);
            System.arraycopy(tmp, 0, vector, 0, dim);
            return;
        }

        int blockIdx = 0;
        float[] x = new float[blockDim];
        float[] out = new float[blockDim];
        for (int j = 0; j < blocks.length; j++) {
            float[][] block = blocks[j];
            int blockDim = blocks[j].length;
            // blockDim is only ever smaller for the tail
            if (blockDim != this.blockDim) {
                x = new float[blockDim];
                out = new float[blockDim];
            }
            for (int k = 0; k < permutationMatrix[j].length; k++) {
                int idx = permutationMatrix[j][k];
                x[k] = vector[idx];
            }
            matrixVectorMultiply(block, x, out);
            System.arraycopy(out, 0, vector, blockIdx, out.length);
            blockIdx += blockDim;
        }
    }

    public void write(IndexOutput out) throws IOException {
        out.writeInt(blockDim);
        out.writeInt(blocks.length);
        out.writeInt(permutationMatrix.length);

        for (float[][] block : blocks) {
            out.writeInt(block.length);
            for (int j = 0; j < block.length; j++) {
                out.writeInt(block[j].length);
                for (int k = 0; k < block[j].length; k++) {
                    out.writeInt(Float.floatToIntBits(block[k][j]));
                }
            }
        }

        for (int[] matrix : permutationMatrix) {
            out.writeInt(matrix.length);
            for (int i : matrix) {
                out.writeInt(i);
            }
        }
    }

    public static PreconditioningProvider read(IndexInput input) throws IOException {
        int blockDim = input.readInt();
        int blocksLen = input.readInt();
        int permutationMatrixLen = input.readInt();

        float[][][] blocks = new float[blocksLen][][];
        int[][] permutationMatrix = new int[permutationMatrixLen][];

        for (int i = 0; i < blocks.length; i++) {
            int blockLen = input.readInt();
            blocks[i] = new float[blockLen][];
            for (int j = 0; j < blockLen; j++) {
                int subBlockLen = input.readInt();
                blocks[i][j] = new float[subBlockLen];
                for (int k = 0; k < subBlockLen; k++) {
                    blocks[i][j][k] = Float.intBitsToFloat(input.readInt());
                }
            }
        }

        for (int i = 0; i < permutationMatrix.length; i++) {
            int permutationMatrixSubLen = input.readInt();
            permutationMatrix[i] = new int[permutationMatrixSubLen];
            for (int j = 0; j < permutationMatrix[i].length; j++) {
                permutationMatrix[i][j] = input.readInt();
            }
        }

        return new PreconditioningProvider(blockDim, blocks, permutationMatrix);
    }

    private static void modifiedGramSchmidt(float[][] m) {
        for (int i = 0; i < m.length; i++) {
            double norm = 0.0;
            for (float v : m[i]) {
                norm += v * v;
            }
            norm = Math.sqrt(norm);
            if (norm == 0.0f) {
                continue;
            }
            for (int j = 0; j < m[i].length; j++) {
                m[i][j] /= (float) norm;
            }
            for (int k = i + 1; k < m[i].length; k++) {
                double dotik = 0.0;
                for (int j = 0; j < m[k].length; j++) {
                    dotik += m[i][j] * m[k][j];
                }
                for (int j = 0; j < m[k].length; j++) {
                    m[k][j] -= (float) (dotik * m[i][j]);
                }
            }
        }
    }

    private static void randomFill(Random gen, float[][] m) {
        for (int i = 0; i < m.length; ++i) {
            for (int j = 0; j < m[i].length; ++j) {
                m[i][j] = (float) gen.nextGaussian();
            }
        }
    }

    private static float[][][] generateRandomOrthogonalMatrix(int dim, int blockDim) {
        blockDim = Math.min(dim, blockDim);
        int nBlocks = dim / blockDim;
        int rem = dim % blockDim;

        float[][][] blocks = new float[nBlocks + (rem > 0 ? 1 : 0)][][];
        Random random = new Random(42L);

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

    private static int[][] createPermutationMatrix(int[] dimBlocks, FloatVectorValues vectors) throws IOException {
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
