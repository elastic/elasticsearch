/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.search.TaskExecutor;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.util.Arrays;
import java.util.Random;

final class AshUtils {

    /**
     * The most multiplications by the second-moment matrix that {@link #dominantSubspace} performs when it finds the
     * subspace starting from a random basis, as opposed to refining an existing basis with {@link #refineSubspace}.
     * It normally stops earlier, once the energy the subspace captures stops growing (see
     * {@link #ENERGY_GAIN_TOLERANCE}); this bound is for the slowest-converging spectra. Only the span of the result
     * matters, because the basis is rotated afterwards.
     */
    static final int MAX_MULTIPLIES = 20;

    /**
     * Multiplications by the second-moment matrix between orthonormalizations of the basis. An empirical choice, not
     * derived from a bound: orthonormalizing every second multiplication captured about the same energy as after every
     * one, at half the orthonormalization cost, while after every fourth the basis lost precision in float early on.
     */
    static final int MULTIPLIES_PER_ORTHONORMALIZATION = 2;

    /**
     * {@link #refineSubspace} stops when one cycle, {@link #MULTIPLIES_PER_ORTHONORMALIZATION} multiplications followed
     * by an orthonormalization, raises the energy captured by the subspace by less than this fraction of the total
     * energy. This bounds the gain of a cycle, not the energy still to be gained: when the spectrum decays slowly near
     * the edge of the subspace the shortfall can be several times larger. That edge carries little energy, and only
     * the span matters; in the cases tried the shortfall was small next to the sampling error of the default training
     * sample, ten times as many vectors as dimensions.
     */
    static final double ENERGY_GAIN_TOLERANCE = 1e-4;

    /**
     * {@link #procrustes} stops iterating once the largest absolute entry of X^T X - I is below this, and returns the
     * iterate that first crosses it, not the one after. It is well above the rounding noise floor in float, about 6e-7 for
     * k from 128 to 1536 in the measurements made, so it is reached before the floor. Waiting instead for the error to
     * stop decreasing costs several further steps, 3 to 4 of the 20 to 28 in the cases measured, spent detecting the floor.
     * <p>
     * This bounds the largest entry, not the distance in the spectral norm. A singular value that is still lagging, whose
     * direction is spread over many coordinates, shows in each entry only as about (1 - sigma^2) / k. So for large k the
     * returned matrix can have a singular value well away from 1 while every entry is below the tolerance: about 0.97 at
     * k = 1024 and 0.87 at k = 3072, for a matrix with one singular value of 0.0001 along a flat direction and all the
     * others 1. The rows of the result are not bounded at all, and can be several times worse than the columns.
     */
    static final float ORTHOGONALITY_TOLERANCE = 1e-4f;

    /**
     * A row of {@link #orthonormalizeRows} whose squared norm, after the rows before it are projected off, is at most this
     * fraction of what it was before is taken to lie exactly in their span. Rounding leaves a row that is only
     * numerically dependent with a squared norm around 1e-14 of the original or more, and the projections still give
     * such a row a valid direction; this is far below that, for rows that cancel to zero, as duplicates and exactly
     * representable data do.
     */
    static final float EXACT_DEPENDENCE = 1e-18f;

    /**
     * Number of rows {@link #orthonormalizeRows} orthonormalizes together. A larger block means fewer copies of the
     * rows already done but more serial work inside the block. 128 is a tuning choice: the fastest of 64 to 512 in one
     * benchmark of 1536 rows of length 3072, on a single machine.
     */
    static final int ORTHONORMALIZE_BLOCK = 128;

    private AshUtils() {}

    /**
     * Returns an array of floats each with a Gaussian distribution around 0.0
     */
    public static float[] randomGaussians(Random random, int dims) {
        float[] v = new float[dims];
        randomGaussians(random, v, 0, dims);
        return v;
    }

    private static final float INT8_MAX = Byte.MAX_VALUE;

    /**
     * Quantizes {@code v[offset..offset+length)} to symmetric int8 in {@code out[outOffset..]}, so that the
     * largest magnitude maps to ±127. -128 is never written, so negating a value cannot overflow.
     *
     * @return the scale that maps the int8 values back to {@code v}, or 0 if every value is 0
     */
    static float quantizeInt8(float[] v, int offset, int length, byte[] out, int outOffset) {
        float max = 0;
        for (int i = 0; i < length; i++) {
            max = Math.max(max, Math.abs(v[offset + i]));
        }
        if (max == 0) {
            Arrays.fill(out, outOffset, outOffset + length, (byte) 0);
            return 0;
        }
        float inverseScale = INT8_MAX / max;
        for (int i = 0; i < length; i++) {
            // map floats to [-127, 127] so it's symmetric
            out[outOffset + i] = (byte) Math.clamp(Math.round(v[offset + i] * inverseScale), -INT8_MAX, INT8_MAX);
        }
        return max / INT8_MAX;
    }

    /**
     * Populates an array of floats each with a Gaussian distribution around 0.0
     */
    public static void randomGaussians(Random random, float[] v, int offset, int length) {
        for (int i = 0; i < length; i++) {
            v[offset + i] = (float) random.nextGaussian();
        }
    }

    /**
     * Returns a random orthogonal {@code (n x n)} matrix, row-major, with rows (and so columns) orthonormal.
     */
    public static float[] randomOrthogonal(Random random, int n, TaskExecutor executor) {
        float[] q = randomGaussians(random, n * n);
        orthonormalizeRows(q, n, n, executor);
        return q;
    }

    /**
     * Computes the nearest orthogonal matrix to M (k x k) using Newton-Schulz iteration
     * for the polar decomposition. Computes U @ Vt from the exact SVD of M
     * (the polar factor that minimizes ||M - R||_F over orthogonal R).
     * <p>
     * Uses Newton-Schulz iteration: X_{k+1} = X_k * (3I - X_k^T X_k) / 2
     * <p>
     * In float precision, X^T X only approaches I down to a rounding noise floor that grows with k. The iteration stops
     * once the largest entry of X^T X - I is below {@link #ORTHOGONALITY_TOLERANCE}, or after 100 iterations, which bounds
     * the cost if the floor is ever above the tolerance. It does not stop when that error merely stops decreasing: while
     * a small singular value is being brought up the largest entry stays almost flat, which looks the same as the floor.
     *
     * @param m the input matrix in row-major order, length k*k
     * @param k the matrix dimension
     * @param r the output matrix in row-major order, length k*k
     * @param executor parallelization executor
     */
    public static void procrustes(float[] m, int k, float[] r, TaskExecutor executor) {
        final int len = k * k;
        assert m.length == len;
        assert r.length == len;

        // Scale M so that all singular values are in (0, sqrt(3)) for Newton-Schulz convergence.
        float spectralNorm = estimateSpectralNorm(m, k, 50);
        float scale = 1f / Math.max(spectralNorm, 1e-10f);

        float[] x = new float[len];
        ESVectorUtil.linearCombination(scale, m, x);

        final int maxIter = 100;
        float[] xT = new float[len];
        float[] xtx = new float[len];
        float[] xNew = new float[len];

        for (int iter = 0; iter < maxIter; iter++) {
            ESVectorUtil.transposeMatrix(x, k, k, xT);
            ESVectorUtil.matrixMultiply(xT, x, k, k, k, xtx, executor);

            // Distance from orthogonality: X^T X should be close to I
            float maxOff = 0;
            for (int i = 0; i < k; i++) {
                int base = i * k;
                for (int j = 0; j < k; j++) {
                    float expected = (i == j) ? 1f : 0f;
                    maxOff = Math.max(maxOff, Math.abs(xtx[base + j] - expected));
                }
            }
            if (maxOff < ORTHOGONALITY_TOLERANCE) {
                break; // x is orthogonal enough
            }

            // X_new = X @ (3I - X^T X) / 2 = 1.5 * X - 0.5 * X @ (X^T X)
            ESVectorUtil.matrixMultiply(x, xtx, k, k, k, xNew, executor);
            ESVectorUtil.linearCombination(1.5f, x, -0.5f, xNew);

            // swap the arrays round for the next iteration
            float[] xOld = x;
            x = xNew;
            xNew = xOld;
        }

        System.arraycopy(x, 0, r, 0, len);
    }

    /**
     * Estimates the spectral norm (largest singular value) of a k x k matrix using power iteration on M^T M.
     */
    private static float estimateSpectralNorm(float[] m, int k, int iterations) {
        // Power iteration on M^T M to find largest eigenvalue (= sigma_max^2)
        float[] v = new float[k];
        // Initialize with uniform vector
        Arrays.fill(v, (float) (1.0 / Math.sqrt(k)));
        float[] mv = new float[k];
        float[] mtmv = new float[k];

        for (int iter = 0; iter < iterations; iter++) {
            // mv = M @ v
            ESVectorUtil.matrixVectorMultiply(m, k, k, v, mv);
            // mtmv = M^T @ mv: row-broadcast so M is read contiguously
            for (int i = 0; i < k; i++) {
                ESVectorUtil.linearCombination(mv[i], m, i * k, mtmv, 0, k);
            }
            // Normalize mtmv - this becomes the new v for the next iteration
            float normSq = ESVectorUtil.l2Normalize(mtmv);
            if (normSq == 0f || !Float.isFinite(normSq)) return 0f;

            float[] nextV = mtmv;
            // re-use v for the next mtmv (less allocations woo!) and zero it out for re-use
            Arrays.fill(v, 0f);
            mtmv = v;
            v = nextV;
        }

        // Compute ||M @ v|| which approximates sigma_max
        ESVectorUtil.matrixVectorMultiply(m, k, k, v, mv);
        return (float) Math.sqrt(ESVectorUtil.dotProduct(mv, mv, k));
    }

    /**
     * Finds an orthonormal basis for the dominant {@code k}-dimensional invariant subspace of a symmetric
     * positive semi-definite matrix, starting from a random subspace. See {@link #refineSubspace}.
     *
     * @param c    the symmetric matrix, row-major {@code (n x n)}; typically a second-moment matrix {@code X^T X}
     * @param n    the matrix dimension
     * @param k    the size of the subspace to extract
     * @param seed random seed for the initial subspace
     * @return the basis transposed, row-major {@code (k x n)}, one orthonormal vector per row. The vectors span the
     *         subspace but are not eigenvectors, and are in no particular order
     */
    public static float[] dominantSubspace(float[] c, int n, int k, long seed, TaskExecutor executor) {
        float[] bT = randomGaussians(new Random(seed), n * k);
        orthonormalizeRows(bT, n, k, executor);
        return refineSubspace(c, n, k, bT, MAX_MULTIPLIES, executor);
    }

    /**
     * Extends {@code have} orthonormal rows to {@code want} orthonormal rows: the extra rows are random directions
     * orthogonal to the first ones, which keep their span.
     *
     * @param bT   the {@code have} orthonormal vectors, one per row, row-major {@code (have x n)}
     * @param n    the length of each vector
     * @param have the number of vectors supplied
     * @param want the number of vectors required, at most {@code n}
     * @return the {@code want} orthonormal vectors, row-major {@code (want x n)}
     */
    public static float[] completeBasis(float[] bT, int n, int have, int want, Random random, TaskExecutor executor) {
        assert bT.length == have * n;
        assert have <= want && want <= n;
        float[] full = Arrays.copyOf(bT, want * n);
        randomGaussians(random, full, have * n, (want - have) * n);
        orthonormalizeRows(full, n, want, executor);
        return full;
    }

    /**
     * Subspace (block power) iteration: {@code B <- orth(C^s B)}. All k vectors advance at once, so the cost is
     * O(multiplies * n^2 * k), without the per-vector deflation a one-at-a-time method would need.
     * <p>
     * The basis is orthonormalized every {@link #MULTIPLIES_PER_ORTHONORMALIZATION} multiplications. Each cycle
     * starts from an orthonormal {@code B}, so its first product {@code C B} also gives the energy
     * {@code tr(B^T C B)} that {@code B} captures, for the cost of a dot product. The iteration stops once a cycle
     * gains less than {@link #ENERGY_GAIN_TOLERANCE} of {@code tr(C)}: only the span matters, and the directions that
     * converge slowest are those at the edge of the subspace, which carry the least energy.
     * <p>
     * The block is transposed either side of each orthonormalization because {@link #orthonormalizeRows} needs
     * the vectors in rows while the product needs them in columns.
     *
     * @param c             the symmetric matrix, row-major {@code (n x n)}
     * @param n             the matrix dimension
     * @param k             the size of the subspace
     * @param bT            the starting orthonormal basis, row-major {@code (k x n)}. Overwritten with the result
     * @param maxMultiplies the most multiplications by {@code c} to perform
     * @return {@code bT}, refined, and orthonormal; or {@code bT} as it was given if the matrix has no energy
     */
    public static float[] refineSubspace(float[] c, int n, int k, float[] bT, int maxMultiplies, TaskExecutor executor) {
        assert c.length == n * n;
        assert bT.length == n * k;
        double trace = 0;
        for (int i = 0; i < n; i++) {
            trace += c[i * n + i];
        }
        if (trace <= 0) {
            return bT; // the matrix is zero, so every subspace captures the same (no) energy and there is nothing to refine
        }
        float[] b = new float[n * k];
        float[] cb = new float[n * k];
        double previousEnergy = Double.NEGATIVE_INFINITY;
        int multiplies = 0;
        while (multiplies < maxMultiplies) {
            ESVectorUtil.transposeMatrix(bT, k, n, b);                  // B (n x k), orthonormal columns
            ESVectorUtil.matrixMultiply(c, b, n, n, k, cb, executor);   // C @ B (n x k)
            multiplies++;

            // tr(B^T C B), as a fraction of tr(C): the dot product of B and C B, a row at a time. Each row is a dot
            // product on the vectorized primitive, in float; the rows are summed in double. A single float dot product
            // over all n * k terms is not accurate enough, since the stopping tolerance is finer than its error
            double energy = 0;
            for (int i = 0; i < n; i++) {
                energy += ESVectorUtil.dotProduct(b, i * k, cb, i * k, k);
            }
            energy /= trace;
            if (energy - previousEnergy < ENERGY_GAIN_TOLERANCE) {
                break; // bT is orthonormal, and the last cycle did not improve it enough to matter
            }
            previousEnergy = energy;

            for (int step = 1; step < MULTIPLIES_PER_ORTHONORMALIZATION && multiplies < maxMultiplies; step++) {
                ESVectorUtil.matrixMultiply(c, cb, n, n, k, b, executor);
                multiplies++;
                float[] swap = b;
                b = cb;
                cb = swap;
            }
            ESVectorUtil.transposeMatrix(cb, n, k, bT);
            orthonormalizeRows(bT, n, k, executor);
        }
        return bT;
    }

    /**
     * Orthonormalizes the rows of {@code vT} (k x n), in place, keeping the span of every leading set of rows.
     * <p>
     * Modified Gram-Schmidt does this with one dot product and one update per pair of rows: serial, and bound by memory
     * traffic. Here rows are taken in blocks. Each block is projected off all the rows before it with two matrix
     * multiplications, twice over ("twice is enough" for a stable result even when the rows are close to parallel, as
     * they are after several multiplications by a second-moment matrix), and then orthonormalized within itself.
     * This is about twice the arithmetic, but it runs as large multiplications that use the executor.
     * <p>
     * A row that lies exactly in the span of the rows before it, such as a duplicate or a zero row, cannot be given a
     * direction by rounding. It is replaced by a random direction orthogonal to all the rows before it, so the result
     * is orthonormal whenever {@code k <= n}.
     *
     * @param vT       the k vectors, each of length n, row-major (k x n)
     * @param n        the length of each vector
     * @param k        the number of vectors
     * @param executor parallelization executor
     */
    static void orthonormalizeRows(float[] vT, int n, int k, TaskExecutor executor) {
        orthonormalizeRows(vT, n, k, ORTHONORMALIZE_BLOCK, executor);
    }

    static void orthonormalizeRows(float[] vT, int n, int k, int blockSize, TaskExecutor executor) {
        int iterationSize = Math.min(blockSize, k);
        float[] block = new float[iterationSize * n];
        float[] originalNormSq = new float[iterationSize];
        float[] projection = new float[iterationSize * n];

        for (int start = 0; start < k; start += iterationSize) {
            int rows = Math.min(iterationSize, k - start);
            if (rows < iterationSize) {
                // the multiplications need exactly-sized operands, so resize for the last iteration
                block = Arrays.copyOfRange(vT, start * n, (start + rows) * n);
                originalNormSq = new float[rows];
                projection = new float[rows * n];
            } else {
                System.arraycopy(vT, start * n, block, 0, rows * n);
            }

            for (int i = 0; i < rows; i++) {
                originalNormSq[i] = ESVectorUtil.dotProduct(block, i * n, block, i * n, n);
            }
            float[] previous = start > 0 ? Arrays.copyOf(vT, start * n) : null;   // (start x n), orthonormal rows
            if (previous != null) {
                float[] previousT = ESVectorUtil.transposeMatrix(previous, start, n);     // (n x start)
                float[] coefficients = new float[rows * start];
                for (int pass = 0; pass < 2; pass++) {
                    ESVectorUtil.matrixMultiply(block, previousT, rows, n, start, coefficients, executor);
                    ESVectorUtil.matrixMultiply(coefficients, previous, rows, start, n, projection, executor);
                    ESVectorUtil.linearCombination(-1f, projection, 1f, block);
                }
            }
            orthonormalizeBlock(block, rows, n, originalNormSq, previous, start);
            System.arraycopy(block, 0, vT, start * n, rows * n);
        }
    }

    /**
     * Orthonormalizes the rows of a block, already projected off the {@code previousRows} rows of {@code previous},
     * in place: each row is projected off the rows of the block before it, twice since the rows of a block can be close
     * to parallel too. A row that is then exactly in the span of the rows before it, judged against its norm before any
     * projection ({@code originalNormSq}), is replaced by a random direction orthogonal to all of them.
     */
    private static void orthonormalizeBlock(float[] block, int rows, int n, float[] originalNormSq, float[] previous, int previousRows) {
        for (int i = 0; i < rows; i++) {
            int row = i * n;
            for (int pass = 0; pass < 2; pass++) {
                projectOff(block, row, n, block, i);
            }
            float remainingNormSq = ESVectorUtil.dotProduct(block, row, block, row, n);
            if (remainingNormSq <= EXACT_DEPENDENCE * originalNormSq[i]) {
                // seeded by the position of the row in the whole matrix, so that two such rows never get the same direction
                randomGaussians(new Random(31L * (previousRows + i) + n), block, row, n);
                for (int pass = 0; pass < 2; pass++) {
                    if (previous != null) {
                        projectOff(block, row, n, previous, previousRows);
                    }
                    projectOff(block, row, n, block, i);
                }
            }
            ESVectorUtil.l2Normalize(block, row, n);
        }
    }

    /** Subtracts from the vector at {@code vectors[row..row+n)} its projection onto the
     * first {@code basisRows} orthonormal rows of {@code basis}. */
    private static void projectOff(float[] vectors, int row, int n, float[] basis, int basisRows) {
        for (int b = 0; b < basisRows; b++) {
            float dot = ESVectorUtil.dotProduct(vectors, row, basis, b * n, n);
            ESVectorUtil.linearCombination(-dot, basis, b * n, vectors, row, n);
        }
    }

    /**
     * Modified Gram-Schmidt QR orthogonalization in-place on the rows of V^T (k x n), row-major.
     * <p>
     * Each vector is stored in a row. This allows each step to run on contiguous blocks of data.
     * For matrices storing vectors as columns, you need to transpose before/after, but that
     * is cheaper than this operation having to access strided data across many cache lines.
     *
     * @param vT the k vectors, each of length n, row-major (k x n)
     * @param n  the length of each vector
     * @param k  the number of vectors
     */
    static void qrOrthogonalize(float[] vT, int n, int k) {
        for (int j = 0; j < k; j++) {
            int row = j * n;
            // Subtract the projections onto the already orthonormalized vectors
            for (int prev = 0; prev < j; prev++) {
                float dot = ESVectorUtil.dotProduct(vT, row, vT, prev * n, n);
                ESVectorUtil.linearCombination(-dot, vT, prev * n, vT, row, n);
            }
            ESVectorUtil.l2Normalize(vT, row, n);
        }
    }
}
