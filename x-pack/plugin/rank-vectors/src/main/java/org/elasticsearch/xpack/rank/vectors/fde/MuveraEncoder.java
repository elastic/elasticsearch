/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.fde;

import java.util.Arrays;

/**
 * MUVERA fixed-dimensional encoding (Dhulipala et al., NeurIPS 2024, arXiv:2405.19504).
 * <p>
 * For each of {@code rReps} repetitions, {@code kSim} random hyperplanes split token space into
 * {@code 2^kSim} SimHash cells. Each cell contributes one block: the mean of the document's tokens
 * in that cell (empty cells take the token whose cell id is Hamming-nearest), or the sum of the
 * query's tokens in that cell (empty cells stay zero). Each block is reduced from the token dimension
 * to {@code dProj} by a random +-1 projection; when {@code dProj} equals the token dimension the
 * projection is skipped. Blocks are concatenated across cells and repetitions.
 * <p>
 * All randomness comes from a SplitMix64 stream seeded with {@code seed}, drawn in a fixed order
 * (hyperplanes, then projections). The draw order is mirrored by the Python
 * measurement implementation. Strict transcendental functions and fixed-order scalar accumulation
 * make an encoding version bit-exact across supported hardware. A future change to any numerical
 * operation or draw order therefore requires a new encoding version.
 *
 * @see <a href="https://arxiv.org/abs/2405.19504">MUVERA: Multi-Vector Retrieval via Fixed Dimensional Encodings</a>
 */
public final class MuveraEncoder implements FdeEncoder {

    private static final Runnable NO_CANCELLATION = () -> {};
    static final int MAX_TOKEN_DIMENSIONS = 4096;
    static final int MAX_TOKEN_VECTORS = 8192;
    static final int MAX_REPETITIONS = 256;
    static final int MAX_RAW_DIMENSIONS = 4096;
    static final long MAX_RANDOM_MATRIX_ELEMENTS = 1L << 24;
    static final long MAX_CELL_ELEMENTS = 1L << 22;
    static final long MAX_ENCODE_OPERATIONS = 1_000_000_000L;
    static final double UNIT_MAGNITUDE_TOLERANCE = 1e-3;
    static final int MAX_K_SIM = 10;

    private final int tokenDims;
    private final int kSim;
    private final int numCells;
    private final int dProj;
    private final int rReps;
    private final int rawDims;
    private final boolean identityProjection;
    private final float projectionScale;

    /** [rep][bit] -> hyperplane of length tokenDims */
    private final float[][][] hyperplanes;
    /** [rep][row] -> +-1 projection row of length tokenDims; null when the projection is the identity */
    private final float[][][] projections;

    /**
     * @param tokenDims dimension of each token vector
     * @param kSim      SimHash bits per repetition; {@code 2^kSim} cells
     * @param dProj     per-cell projected dimension, at most {@code tokenDims}
     * @param rReps     independent repetitions
     * @param seed      seed for all random draws
     */
    public MuveraEncoder(int tokenDims, int kSim, int dProj, int rReps, long seed) {
        if (tokenDims <= 0 || tokenDims > MAX_TOKEN_DIMENSIONS) {
            throw new IllegalArgumentException("token dims must be in [1, " + MAX_TOKEN_DIMENSIONS + "], got [" + tokenDims + "]");
        }
        if (kSim < 0 || kSim > MAX_K_SIM) {
            throw new IllegalArgumentException("k_sim must be in [0, " + MAX_K_SIM + "], got [" + kSim + "]");
        }
        if (dProj <= 0 || dProj > tokenDims) {
            throw new IllegalArgumentException("d_proj must be in [1, " + tokenDims + "], got [" + dProj + "]");
        }
        if (rReps <= 0 || rReps > MAX_REPETITIONS) {
            throw new IllegalArgumentException("r_reps must be in [1, " + MAX_REPETITIONS + "], got [" + rReps + "]");
        }
        long raw = (long) rReps * (1L << kSim) * dProj;
        if (raw > MAX_RAW_DIMENSIONS) {
            throw new IllegalArgumentException(
                "r_reps * 2^k_sim * d_proj = [" + raw + "] exceeds the maximum of [" + MAX_RAW_DIMENSIONS + "]"
            );
        }
        long randomMatrixElements = (long) rReps * kSim * tokenDims;
        if (dProj != tokenDims) {
            randomMatrixElements += (long) rReps * dProj * tokenDims;
        }
        if (randomMatrixElements > MAX_RANDOM_MATRIX_ELEMENTS) {
            throw new IllegalArgumentException(
                "random matrices require ["
                    + randomMatrixElements
                    + "] elements, exceeding the maximum of ["
                    + MAX_RANDOM_MATRIX_ELEMENTS
                    + "]"
            );
        }
        long cellElements = (long) (1 << kSim) * tokenDims;
        if (cellElements > MAX_CELL_ELEMENTS) {
            throw new IllegalArgumentException(
                "cell workspace requires [" + cellElements + "] elements, exceeding the maximum of [" + MAX_CELL_ELEMENTS + "]"
            );
        }
        this.tokenDims = tokenDims;
        this.kSim = kSim;
        this.numCells = 1 << kSim;
        this.dProj = dProj;
        this.rReps = rReps;
        this.rawDims = (int) raw;
        this.identityProjection = dProj == tokenDims;
        this.projectionScale = (float) (1.0 / StrictMath.sqrt(dProj));

        SplitMix64 rng = new SplitMix64(seed);
        hyperplanes = new float[rReps][kSim][tokenDims];
        for (int r = 0; r < rReps; r++) {
            for (int k = 0; k < kSim; k++) {
                for (int d = 0; d < tokenDims; d++) {
                    hyperplanes[r][k][d] = (float) rng.nextGaussian();
                }
            }
        }
        if (identityProjection) {
            projections = null;
        } else {
            projections = new float[rReps][dProj][tokenDims];
            for (int r = 0; r < rReps; r++) {
                for (int j = 0; j < dProj; j++) {
                    for (int d = 0; d < tokenDims; d++) {
                        projections[r][j][d] = rng.nextSign();
                    }
                }
            }
        }
    }

    @Override
    public int dimensions() {
        return rawDims;
    }

    @Override
    public float[] encodeDocument(float[][] tokens) {
        return encode(tokens, true, NO_CANCELLATION);
    }

    @Override
    public float[] encodeQuery(float[][] tokens) {
        return encode(tokens, false, NO_CANCELLATION);
    }

    /**
     * Encodes query tokens while periodically checking whether the enclosing
     * search has been cancelled.
     */
    public float[] encodeQuery(float[][] tokens, Runnable checkCancelled) {
        if (checkCancelled == null) {
            throw new IllegalArgumentException("cancellation check must not be null");
        }
        return encode(tokens, false, checkCancelled);
    }

    /** Validates query vectors against this immutable encoding profile without encoding them. */
    public void validateQueryVectors(float[][] tokens) {
        validate(tokens, false, NO_CANCELLATION);
    }

    private void validate(float[][] tokens, boolean document, Runnable checkCancelled) {
        if (tokens == null) {
            throw new IllegalArgumentException("token vectors must not be null");
        }
        if (tokens.length == 0) {
            throw new IllegalArgumentException("at least one token vector is required");
        }
        if (tokens.length > MAX_TOKEN_VECTORS) {
            throw new IllegalArgumentException(
                "at most [" + MAX_TOKEN_VECTORS + "] token vectors are supported, got [" + tokens.length + "]"
            );
        }
        long operationsPerRepetition = (long) tokens.length * (kSim + 1) * tokenDims;
        operationsPerRepetition += (long) numCells * tokenDims;
        operationsPerRepetition += identityProjection ? (long) numCells * tokenDims : (long) numCells * dProj * tokenDims;
        if (document) {
            operationsPerRepetition += (long) numCells * numCells;
        }
        long estimatedOperations = operationsPerRepetition * rReps;
        if (estimatedOperations > MAX_ENCODE_OPERATIONS) {
            throw new IllegalArgumentException(
                "encoding requires approximately ["
                    + estimatedOperations
                    + "] operations, exceeding the maximum of ["
                    + MAX_ENCODE_OPERATIONS
                    + "]"
            );
        }
        for (int t = 0; t < tokens.length; t++) {
            if ((t & 15) == 0) {
                checkCancelled.run();
            }
            float[] token = tokens[t];
            if (token == null) {
                throw new IllegalArgumentException("token vector [" + t + "] must not be null");
            }
            if (token.length != tokenDims) {
                throw new IllegalArgumentException("expected token dims [" + tokenDims + "], got [" + token.length + "]");
            }
            double squaredMagnitude = 0d;
            for (int d = 0; d < tokenDims; d++) {
                float value = token[d];
                if (Float.isFinite(value) == false) {
                    throw new IllegalArgumentException("token vector [" + t + "] contains a non-finite value at dimension [" + d + "]");
                }
                squaredMagnitude += (double) value * value;
            }
            if (Math.abs(squaredMagnitude - 1d) > UNIT_MAGNITUDE_TOLERANCE) {
                throw new IllegalArgumentException(
                    "token vector [" + t + "] must have unit magnitude, got squared magnitude [" + squaredMagnitude + "]"
                );
            }
        }
    }

    private float[] encode(float[][] tokens, boolean document, Runnable checkCancelled) {
        validate(tokens, document, checkCancelled);
        float[] raw = new float[rawDims];
        float[][] cells = new float[numCells][tokenDims];
        int[] counts = new int[numCells];
        int[] firstToken = new int[numCells];
        for (int r = 0; r < rReps; r++) {
            checkCancelled.run();
            for (float[] cell : cells) {
                Arrays.fill(cell, 0f);
            }
            Arrays.fill(counts, 0);
            Arrays.fill(firstToken, -1);
            for (int t = 0; t < tokens.length; t++) {
                if ((t & 15) == 0) {
                    checkCancelled.run();
                }
                int c = cellId(tokens[t], r);
                if (counts[c]++ == 0) {
                    firstToken[c] = t;
                }
                float[] cell = cells[c];
                float[] token = tokens[t];
                for (int d = 0; d < tokenDims; d++) {
                    cell[d] += token[d];
                }
            }
            if (document) {
                for (int c = 0; c < numCells; c++) {
                    if (counts[c] > 1) {
                        float inv = 1f / counts[c];
                        float[] cell = cells[c];
                        for (int d = 0; d < tokenDims; d++) {
                            cell[d] *= inv;
                        }
                    } else if (counts[c] == 0) {
                        System.arraycopy(tokens[firstToken[nearestNonEmpty(c, counts)]], 0, cells[c], 0, tokenDims);
                    }
                }
            }
            int base = r * numCells * dProj;
            for (int c = 0; c < numCells; c++) {
                checkCancelled.run();
                if (document == false && counts[c] == 0) {
                    continue;
                }
                int off = base + c * dProj;
                if (identityProjection) {
                    System.arraycopy(cells[c], 0, raw, off, tokenDims);
                } else {
                    for (int j = 0; j < dProj; j++) {
                        raw[off + j] = projectionScale * scalarDotProduct(cells[c], projections[r][j]);
                    }
                }
            }
        }
        return raw;
    }

    private int cellId(float[] token, int rep) {
        int id = 0;
        float[][] planes = hyperplanes[rep];
        for (int k = 0; k < kSim; k++) {
            if (scalarDotProduct(token, planes[k]) > 0f) {
                id |= 1 << k;
            }
        }
        return id;
    }

    /**
     * Uses a fixed scalar accumulation order. Lucene's vectorized dot product may reassociate operations depending on
     * the available SIMD instructions, which would make the persisted FDE differ across nodes.
     */
    private static float scalarDotProduct(float[] left, float[] right) {
        assert left.length == right.length;
        float sum = 0f;
        for (int i = 0; i < left.length; i++) {
            sum += left[i] * right[i];
        }
        return sum;
    }

    /** Lowest-id non-empty cell at minimum Hamming distance from {@code cell}. */
    private int nearestNonEmpty(int cell, int[] counts) {
        int best = -1;
        int bestDist = Integer.MAX_VALUE;
        for (int other = 0; other < numCells; other++) {
            if (counts[other] > 0) {
                int dist = Integer.bitCount(cell ^ other);
                if (dist < bestDist) {
                    bestDist = dist;
                    best = other;
                }
            }
        }
        return best;
    }

    /** SplitMix64 (Steele, Lea, Flood 2014); chosen because it is trivially reproducible outside Java. */
    static final class SplitMix64 {
        private long state;

        SplitMix64(long seed) {
            this.state = seed;
        }

        long next() {
            long z = (state += 0x9E3779B97F4A7C15L);
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        /** Uniform in [0, 1). */
        double nextDouble() {
            return (next() >>> 11) * 0x1.0p-53;
        }

        float nextSign() {
            return next() < 0 ? -1f : 1f;
        }

        /** Box-Muller, one variate per call; StrictMath provides reproducible transcendental operations. */
        double nextGaussian() {
            double u1 = 1.0 - nextDouble();
            double u2 = nextDouble();
            return StrictMath.sqrt(-2.0 * StrictMath.log(u1)) * StrictMath.cos(2.0 * StrictMath.PI * u2);
        }
    }
}
