/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

// TODO: apply to formats other than ESNextDiskBBQVectorsFormat
// TODO: instead of manually having to indicate preconditioning add the ability to decide when to use it given the data on the segment
public class Preconditioner {
    final int blockDim;
    final int[][] permutationMatrix;
    final float[][][] blocks;

    private Preconditioner(int blockDim, int[][] permutationMatrix, float[][][] blocks) {
        this.blockDim = blockDim;
        this.permutationMatrix = permutationMatrix;
        this.blocks = blocks;
    }

    public void applyTransform(float[] vector, float[] out) {
        assert vector != null;
        assert vector.length == blockDim * (blocks.length - 1) + (blocks[blocks.length - 1].length);

        if (blocks.length == 1) {
            matrixVectorMultiply(blocks[0], vector, out);
        } else {
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
                // TODO: can be optimized to do all blocks in one pass?
                matrixVectorMultiply(block, x, blockOut);
                System.arraycopy(blockOut, 0, out, blockIdx, blockDim);
                blockIdx += blockDim;
            }
        }
    }

    // TODO: write Panama version of this
    private static void modifiedGramSchmidt(float[][] m) {
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
        assert blockDim <= dim;
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
        assert m.length == out.length;
        assert m.length > 0 && m[0].length == x.length;
        int dim = out.length;
        // TODO: write Panama version of this to do all multiplications in one pass
        for (int i = 0; i < dim; i++) {
            out[i] = ESVectorUtil.dotProduct(m[i], x);
        }
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

    public void write(IndexOutput out) throws IOException {
        int rem = this.blockDim;
        float[][][] blocks = this.blocks;
        int[][] permutationMatrix = this.permutationMatrix;
        int blockDim = this.blockDim;
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

    // TODO: cache these preconditioners based on vectorDimension and blockDimension
    // need something thread safe and a way to clear the cache when done indexing (after flush or merge ... but that defeats the point)
    // maybe not possible or we limit it to a fixed number of cached preconditioners
    // maybe use setExpireAfterAccess in CacheBuilder; to be fair this code is not a hot path though
    public static Preconditioner createPreconditioner(int vectorDimension, int blockDimension) {
        if (blockDimension <= 0) {
            throw new IllegalArgumentException("block dimension must be positive but was [" + blockDimension + "]");
        }
        if (vectorDimension <= 0) {
            throw new IllegalArgumentException("vector dimension must be positive but was [" + vectorDimension + "]");
        }
        Random random = new Random(42L);
        blockDimension = Math.min(vectorDimension, blockDimension);
        float[][][] blocks = Preconditioner.generateRandomOrthogonalMatrix(vectorDimension, blockDimension, random);
        int[] dimBlocks = new int[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            dimBlocks[i] = blocks[i].length;
        }
        int[][] permutationMatrix = Preconditioner.createPermutationMatrixRandomly(vectorDimension, dimBlocks, random);
        return new Preconditioner(blockDimension, permutationMatrix, blocks);
    }

    public static Preconditioner read(IndexInput input) throws IOException {
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

        return new Preconditioner(blockDim, permutationMatrix, blocks);
    }
}
