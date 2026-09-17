/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.elasticsearch.simdvec.ESVectorUtil;

import java.util.Arrays;
import java.util.Random;

final class AshUtils {

    private AshUtils() {}

    /**
     * Returns an array of floats each with a Gaussian distribution around 0.0
     */
    public static float[] randomGaussians(Random random, int dims) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = (float) random.nextGaussian();
        }
        return v;
    }

    /**
     * Computes the nearest orthogonal matrix to M (k x k) using Newton-Schulz iteration
     * for the polar decomposition. Computes U @ Vt from the exact SVD of M
     * (the polar factor that minimizes ||M - R||_F over orthogonal R).
     * <p>
     * Uses Newton-Schulz iteration in double precision for guaranteed convergence:
     * X_{k+1} = X_k * (3I - X_k^T X_k) / 2
     *
     * @param m the input matrix in row-major order, length k*k
     * @param k the matrix dimension
     * @param r the output matrix in row-major order, length k*k
     */
    public static void procrustes(float[] m, int k, float[] r) {
        // Scale M so that all singular values are in (0, sqrt(3)) for Newton-Schulz convergence.
        float spectralNorm = estimateSpectralNorm(m, k, 50);
        double scale = 1.0 / Math.max(spectralNorm, 1e-10);

        // Work in double precision to avoid float32 accumulation errors at 352x352
        double[] x = new double[k * k];
        for (int i = 0; i < k * k; i++) {
            x[i] = m[i] * scale;
        }

        // Newton-Schulz iteration: X <- X * (3I - X^T X) / 2
        int maxIter = 100;
        // pre-allocate the arrays first
        double[] xtx = new double[k * k];
        double[] b = new double[k * k];
        double[] xNew = new double[k * k];

        for (int iter = 0; iter < maxIter; iter++) {
            // Compute X^T X (k x k) using row-broadcast for cache efficiency
            for (int l = 0; l < k; l++) {
                int xBase = l * k;
                for (int i = 0; i < k; i++) {
                    double xli = x[xBase + i];
                    int xtxBase = i * k;
                    for (int j = i; j < k; j++) {
                        xtx[xtxBase + j] = Math.fma(xli, x[xBase + j], xtx[xtxBase + j]);
                    }
                }
            }
            // Symmetrize
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < i; j++) {
                    xtx[i * k + j] = xtx[j * k + i];
                }
            }

            // Check convergence: X^T X should be close to I
            double maxOff = 0;
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    double expected = (i == j) ? 1.0 : 0.0;
                    maxOff = Math.max(maxOff, Math.abs(xtx[i * k + j] - expected));
                }
            }
            if (maxOff < 1e-12) {
                break;
            }

            // B = (3I - X^T X) / 2
            // don't need to clear b here, it's all overwritten anyway
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    b[i * k + j] = -xtx[i * k + j] / 2.0;
                }
                b[i * k + i] += 1.5;
            }

            // X_new = X @ B (row-broadcast for JIT vectorization)
            // this uses doubles, so can't use matrixMultiply nor ESVectorUtil methods
            Arrays.fill(xNew, 0);
            for (int i = 0; i < k; i++) {
                int xBase = i * k;
                int xNewBase = i * k;
                for (int l = 0; l < k; l++) {
                    double xVal = x[xBase + l];
                    int bBase = l * k;
                    for (int j = 0; j < k; j++) {
                        xNew[xNewBase + j] = Math.fma(xVal, b[bBase + j], xNew[xNewBase + j]);
                    }
                }
            }

            // swap the arrays round for the next iteration
            double[] xOld = x;
            x = xNew;
            xNew = xOld;

            Arrays.fill(xtx, 0);
        }

        // Convert back to float
        for (int i = 0; i < k * k; i++) {
            r[i] = (float) x[i];
        }
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
     * Computes the top-k right singular vectors of matrix A (m x n) using power iteration
     * on the Gram matrix A^T A. Much faster than full SVD when k is small.
     *
     * @param a   matrix in row-major order, length m*n
     * @param m   number of rows
     * @param n   number of columns
     * @param k   number of top singular vectors to extract
     * @param seed random seed for initialization
     * @return top-k right singular vectors as columns, row-major (n x k)
     */
    public static float[] topKRightSingularVectors(float[] a, int m, int n, int k, long seed) {
        // Compute C = A^T A (n x n) -- this is symmetric positive semi-definite
        // For m >> n this is cheaper than full SVD
        // For m < n, we use A A^T (m x m) and transform back
        if (m >= n) {
            return topKEigenvectorsGram(a, m, n, k, seed);
        } else {
            // Compute A A^T (m x m), find eigenvectors, transform back to right singular vectors
            return topKEigenvectorsGramTranspose(a, m, n, k, seed);
        }
    }

    private static float[] topKEigenvectorsGram(float[] a, int m, int n, int k, long seed) {
        // Eigenvectors of A^T A are the right singular vectors, so iterate with X = A. A^T is
        // materialized so that the A^T @ W product reads sequentially.
        float[] vT = blockPowerIteration(a, ESVectorUtil.transposeMatrix(a, m, n), m, n, k, seed);
        return ESVectorUtil.transposeMatrix(vT, k, n);
    }

    private static float[] topKEigenvectorsGramTranspose(float[] a, int m, int n, int k, long seed) {
        // A is (m x n) with m < n, so A A^T (m x m) is the smaller Gram matrix: iterate with
        // X = A^T to get the left singular vectors U, then recover the right singular vectors.
        float[] uT = blockPowerIteration(ESVectorUtil.transposeMatrix(a, m, n), a, n, m, k, seed);

        // V = A^T U, computed transposed as V^T = U^T A (k x n) so that each vector occupies a
        // row and the normalization runs over contiguous data.
        float[] vT = ESVectorUtil.matrixMultiply(uT, a, k, m, n);
        for (int j = 0; j < k; j++) {
            ESVectorUtil.l2Normalize(vT, j * n, n);
        }
        return ESVectorUtil.transposeMatrix(vT, k, n);
    }

    /**
     * Block (subspace) power iteration for the dominant k-dimensional invariant subspace of
     * {@code X^T X}: iterates {@code B <- X^T (X B)}, QR-orthogonalizing after each step. All k
     * vectors advance at once, which is O(iterations * p * q * k) in total, much cheaper than
     * deflating one vector at a time for large k.
     * <p>
     * The caller supplies both X and its transpose so that each product keeps the (q x k) block as
     * its right operand, which {@code multiplyAccumulate} streams once per four rows of output.
     * The block is transposed either side of each orthogonalization because
     * {@link #qrOrthogonalize} needs the vectors in rows while the products need them in columns.
     *
     * @param x    the matrix, row-major (p x q)
     * @param xT   the transpose of {@code x}, row-major (q x p)
     * @param p    number of rows in {@code x}
     * @param q    number of columns in {@code x}
     * @param k    the size of the subspace to extract
     * @param seed random seed for initialization
     * @return the converged block transposed, row-major (k x q), one orthonormal vector per row
     */
    private static float[] blockPowerIteration(float[] x, float[] xT, int p, int q, int k, long seed) {
        int iters = 20; // sufficient for PCA init that gets refined by Procrustes

        float[] bT = randomGaussians(new Random(seed), q * k);
        qrOrthogonalize(bT, q, k);

        float[] b = new float[q * k];
        float[] w = new float[p * k];
        for (int iter = 0; iter < iters; iter++) {
            ESVectorUtil.transposeMatrix(bT, k, q, b);       // B (q x k)
            ESVectorUtil.matrixMultiply(x, b, p, q, k, w);   // W = X @ B (p x k)
            ESVectorUtil.matrixMultiply(xT, w, q, p, k, b);  // B <- X^T @ W (q x k)

            ESVectorUtil.transposeMatrix(b, q, k, bT);
            qrOrthogonalize(bT, q, k);
        }

        return bT;
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
