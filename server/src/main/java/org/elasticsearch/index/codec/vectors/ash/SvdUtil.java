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

import static org.elasticsearch.simdvec.ESVectorUtil.transposeMatrix;

/**
 * Utility class providing thin SVD decomposition via one-sided Jacobi rotations.
 * <p>
 * This implementation is designed for small matrices (e.g. 28x28 or 768x28) used
 * during ASH training. It computes the thin SVD: A = U * S * Vt, where U has
 * orthonormal columns, S is diagonal, and Vt has orthonormal rows.
 * <p>
 * All matrices are represented in row-major flat {@code float[]} of length rows*cols.
 */
final class SvdUtil {

    private SvdUtil() {}

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
     * Result of a thin SVD decomposition.
     *
     * @param u  left singular vectors, row-major (m x k)
     * @param s  singular values (k)
     * @param vt right singular vectors transposed, row-major (k x n)
     */
    public record SvdResult(float[] u, float[] s, float[] vt) {}

    /**
     * Computes the thin SVD of matrix A (m x n) where m >= n.
     * Returns U (m x n), S (n), Vt (n x n).
     * <p>
     * For m &lt; n, returns U (m x m), S (m), Vt (m x n)
     * <p>
     * Uses one-sided Jacobi SVD which is simple and numerically stable for small matrices.
     *
     * @param a the input matrix in row-major order, length m*n
     * @param m number of rows
     * @param n number of columns
     * @return the thin SVD decomposition
     */
    public static SvdResult thinSvd(float[] a, int m, int n) {
        if (m < n) {
            // For wide matrices, compute SVD of transpose and swap U/V.
            // at is (n x m); thinSvd(at, n, m) gives result.u of shape (n x m) and result.vt of shape (m x m).
            // A = U * S * Vt => At = V * S * Ut, so U = result.vt^T and Vt = result.u^T.
            float[] at = transposeMatrix(a, m, n);
            SvdResult result = thinSvd(at, n, m);
            return new SvdResult(transposeMatrix(result.vt(), m, m), result.s(), transposeMatrix(result.u(), n, m));
        }

        // Copy A into working matrix; Jacobi rotations are applied in-place and after
        // column normalization this array becomes U (m x n).
        float[] u = a.clone();

        // V starts as identity (n x n)
        float[] v = new float[n * n];
        for (int i = 0; i < n; i++) {
            v[i * n + i] = 1.0f;
        }

        // One-sided Jacobi: apply rotations to columns of u until convergence
        int maxIterations = 100 * n;
        for (int iter = 0; iter < maxIterations; iter++) {
            boolean converged = true;
            for (int p = 0; p < n - 1; p++) {
                for (int q = p + 1; q < n; q++) {
                    // Compute 2x2 Gram matrix elements for columns p, q
                    double app = 0, aqq = 0, apq = 0;
                    for (int i = 0; i < m; i++) {
                        app = Math.fma(u[i * n + p], u[i * n + p], app);
                        aqq = Math.fma(u[i * n + q], u[i * n + q], aqq);
                        apq = Math.fma(u[i * n + p], u[i * n + q], apq);
                    }

                    if (Math.abs(apq) < 1e-10 * Math.sqrt(app * aqq)) {
                        continue; // columns already orthogonal
                    }
                    converged = false;

                    // Compute Jacobi rotation angle
                    double tau = (aqq - app) / (2.0 * apq);
                    double t;
                    if (tau >= 0) {
                        t = 1.0 / (tau + Math.sqrt(1.0 + tau * tau));
                    } else {
                        t = -1.0 / (-tau + Math.sqrt(1.0 + tau * tau));
                    }
                    double cos = 1.0 / Math.sqrt(1.0 + t * t);
                    double sin = t * cos;

                    // Apply rotation to columns p, q of u
                    for (int i = 0; i < m; i++) {
                        double wp = u[i * n + p];
                        double wq = u[i * n + q];
                        u[i * n + p] = (float) (cos * wp - sin * wq);
                        u[i * n + q] = (float) (sin * wp + cos * wq);
                    }

                    // Apply rotation to columns p, q of V
                    for (int i = 0; i < n; i++) {
                        double vp = v[i * n + p];
                        double vq = v[i * n + q];
                        v[i * n + p] = (float) (cos * vp - sin * vq);
                        v[i * n + q] = (float) (sin * vp + cos * vq);
                    }
                }
            }
            if (converged) {
                break;
            }
        }

        // normalize columns of u in-place, and use the norms as singular values
        float[] s = new float[n];
        for (int j = 0; j < n; j++) {
            s[j] = normalizeColumn(u, j, n, m);
        }

        // Sort by descending singular value
        // TODO: can we transpose the matrix now so the column swaps become row swaps -> arraycopy?
        sortDescending(u, s, v, m, n);

        // Vt = transpose of V
        float[] vt = transposeMatrix(v, n, n);

        return new SvdResult(u, s, vt);
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
     * @return the nearest orthogonal matrix in row-major order, length k*k
     */
    public static float[] procrustes(float[] m, int k) {
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
        for (int iter = 0; iter < maxIter; iter++) {
            // Compute X^T X (k x k) using row-broadcast for cache efficiency
            double[] xtx = new double[k * k];
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
            double[] b = new double[k * k];
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    b[i * k + j] = -xtx[i * k + j] / 2.0;
                }
                b[i * k + i] += 1.5;
            }

            // X_new = X @ B (row-broadcast for JIT vectorization)
            double[] xNew = new double[k * k];
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
            x = xNew;
        }

        // Convert back to float
        float[] result = new float[k * k];
        for (int i = 0; i < k * k; i++) {
            result[i] = (float) x[i];
        }
        return result;
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
            for (int i = 0; i < k; i++) {
                mv[i] = ESVectorUtil.dotProduct(m, i * k, v, 0, k);
            }
            // mtmv = M^T @ mv: row-broadcast so M is read contiguously
            Arrays.fill(mtmv, 0f);
            for (int i = 0; i < k; i++) {
                ESVectorUtil.linearCombination(mv[i], m, i * k, mtmv, 0, k);
            }
            // Normalize
            double normSq = ESVectorUtil.dotProduct(mtmv, 0, mtmv, 0, k);
            double norm = Math.sqrt(normSq);
            if (norm < 1e-30) return 0f;
            for (int j = 0; j < k; j++) {
                v[j] = (float) (mtmv[j] / norm);
            }
        }
        // Compute ||M @ v|| which approximates sigma_max
        double mvNormSq = 0;
        for (int i = 0; i < k; i++) {
            double sum = ESVectorUtil.dotProduct(m, i * k, v, 0, k);
            mvNormSq += sum * sum;
        }
        return (float) Math.sqrt(mvNormSq);
    }

    private static void sortDescending(float[] u, float[] s, float[] v, int m, int n) {
        // Simple insertion sort (n is small)
        // TODO: swap out with IntroSorter, which uses insertion sort for small arrays?
        for (int i = 0; i < n - 1; i++) {
            int maxIdx = i;
            for (int j = i + 1; j < n; j++) {
                if (s[j] > s[maxIdx]) {
                    maxIdx = j;
                }
            }
            if (maxIdx != i) {
                // Swap singular values
                float tmp = s[i];
                s[i] = s[maxIdx];
                s[maxIdx] = tmp;
                // Swap columns i and maxIdx of U
                for (int r = 0; r < m; r++) {
                    tmp = u[r * n + i];
                    u[r * n + i] = u[r * n + maxIdx];
                    u[r * n + maxIdx] = tmp;
                }
                // Swap columns i and maxIdx of V
                for (int r = 0; r < n; r++) {
                    tmp = v[r * n + i];
                    v[r * n + i] = v[r * n + maxIdx];
                    v[r * n + maxIdx] = tmp;
                }
            }
        }
    }

    /**
     * Computes the top-k right singular vectors of matrix A (m x n) using power iteration
     * on the Gram matrix A^T A with deflation. Much faster than full SVD when k is small.
     *
     * @param a   matrix in row-major order, length m*n
     * @param m   number of rows
     * @param n   number of columns
     * @param k   number of top singular vectors to extract
     * @param seed random seed for initialization
     * @return top-k right singular vectors as rows, row-major (k x n)
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
        // Block (subspace) power iteration: process all k vectors simultaneously.
        // V = random (n x k), iterate: V <- A^T (A V), then QR-orthogonalize.
        // This is O(iterations * m * n * k) total -- much faster than deflation for large k.
        //
        // We store V in column-major form (n x k) for QR, but use a transposed (k x n) copy
        // for the matmul inner loops to enable row-contiguous access and JIT vectorization.
        int iters = 20; // sufficient for PCA init that gets refined by Procrustes

        // V: (n x k) row-major -- each column is a candidate eigenvector
        float[] v = randomGaussians(new Random(seed), n * k);
        qrOrthogonalize(v, n, k);

        for (int iter = 0; iter < iters; iter++) {
            // Build transposed view vT (k x n) for cache-friendly row access in matmul
            float[] vT = transposeMatrix(v, n, k);

            // W = A @ V (m x k): w[i*k + j] = dot(a[i*n..], vT[j*n..])
            float[] w = new float[m * k];
            for (int i = 0; i < m; i++) {
                int aBase = i * n;
                int wBase = i * k;
                for (int j = 0; j < k; j++) {
                    w[wBase + j] = ESVectorUtil.dotProduct(a, aBase, vT, j * n, n);
                }
            }

            // V_new = A^T @ W (n x k): use row-broadcast accumulation
            float[] vNew = new float[n * k];
            for (int i = 0; i < m; i++) {
                int aBase = i * n;
                int wBase = i * k;
                for (int d = 0; d < n; d++) {
                    ESVectorUtil.linearCombination(a[aBase + d], w, wBase, vNew, d * k, k);
                }
            }
            qrOrthogonalize(vNew, n, k);
            v = vNew;
        }

        // Convert columns of V to rows for return format (k x n)
        return transposeMatrix(v, n, k);
    }

    /**
     * Normalizes column {@code col} of a row-major matrix in-place and returns the column norm.
     * Elements are at indices {@code col}, {@code col + stride}, ..., {@code col + (length-1)*stride}.
     * No-ops (but still returns the norm) if the norm is zero or non-finite.
     *
     * @param matrix flat row-major array
     * @param col    column index within a row (starting offset of the column)
     * @param stride number of columns in the matrix
     * @param length number of rows to normalize
     * @return the column norm before normalization
     */
    static float normalizeColumn(float[] matrix, int col, int stride, int length) {
        double normSq = 0;
        for (int i = 0; i < length; i++) {
            normSq = Math.fma(matrix[i * stride + col], matrix[i * stride + col], normSq);
        }
        float norm = (float) Math.sqrt(normSq);
        float invNorm = 1.0f / norm;
        if (Float.isFinite(invNorm)) {
            for (int i = 0; i < length; i++) {
                matrix[i * stride + col] *= invNorm;
            }
        }
        return norm;
    }

    /**
     * Modified Gram-Schmidt QR orthogonalization in-place on columns of V (n x k), row-major.
     */
    private static void qrOrthogonalize(float[] v, int n, int k) {
        for (int j = 0; j < k; j++) {
            // Subtract projections of previous columns
            for (int prev = 0; prev < j; prev++) {
                double dot = 0;
                for (int i = 0; i < n; i++) {
                    dot = Math.fma(v[i * k + j], v[i * k + prev], dot);
                }
                for (int i = 0; i < n; i++) {
                    v[i * k + j] = (float) Math.fma(-dot, v[i * k + prev], v[i * k + j]);
                }
            }
            normalizeColumn(v, j, k, n);
        }
    }

    private static float[] topKEigenvectorsGramTranspose(float[] a, int m, int n, int k, long seed) {
        // A is (m x n) with m < n. Find top-k eigenvectors of A A^T (m x m), then transform.
        // u_i = eigenvector of A A^T => v_i = A^T u_i / sigma_i (right singular vector)
        float[] result = new float[k * n];
        Random rng = new Random(seed);
        float[][] deflated = new float[k][];
        int found = 0;

        for (int vec = 0; vec < k; vec++) {
            // Random initial vector (m-dimensional)
            float[] u = randomGaussians(rng, m);
            ESVectorUtil.l2Normalize(u);

            // Power iteration on A A^T: u <- A (A^T u) / ||...||
            for (int iter = 0; iter < 100; iter++) {
                // w = A^T u (n-dimensional): row-broadcast so A is read contiguously
                float[] w = new float[n];
                for (int i = 0; i < m; i++) {
                    ESVectorUtil.linearCombination(u[i], a, i * n, w, 0, n);
                }
                // u_new = A w (m-dimensional)
                float[] uNew = new float[m];
                for (int i = 0; i < m; i++) {
                    uNew[i] = ESVectorUtil.dotProduct(a, i * n, w, 0, n);
                }
                // Deflate
                for (int d = 0; d < found; d++) {
                    float dot = ESVectorUtil.dotProduct(uNew, deflated[d]);
                    ESVectorUtil.linearCombination(-dot, deflated[d], uNew);
                }
                ESVectorUtil.l2Normalize(uNew);
                u = uNew;
            }
            deflated[found] = u;
            found++;

            // Recover right singular vector: v = A^T u, then normalize; row-broadcast so A is read contiguously
            float[] sv = new float[n];
            for (int i = 0; i < m; i++) {
                ESVectorUtil.linearCombination(u[i], a, i * n, sv, 0, n);
            }
            ESVectorUtil.l2Normalize(sv);
            System.arraycopy(sv, 0, result, vec * n, n);
        }
        return result;
    }
}
