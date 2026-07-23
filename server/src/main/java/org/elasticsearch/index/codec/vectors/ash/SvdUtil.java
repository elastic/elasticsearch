/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

/**
 * Utility class providing thin SVD decomposition via one-sided Jacobi rotations.
 * <p>
 * This implementation is designed for small matrices (e.g. 28x28 or 768x28) used
 * during ASH training. It computes the thin SVD: A = U * S * Vt, where U has
 * orthonormal columns, S is diagonal, and Vt has orthonormal rows.
 */
public final class SvdUtil {

    private SvdUtil() {}

    /**
     * Result of a thin SVD decomposition.
     *
     * @param u  left singular vectors (m x k)
     * @param s  singular values (k)
     * @param vt right singular vectors transposed (k x n)
     */
    public record SvdResult(float[][] u, float[] s, float[][] vt) {}

    /**
     * Computes the thin SVD of matrix A (m x n) where m >= n.
     * Returns U (m x n), S (n), Vt (n x n).
     * <p>
     * Uses one-sided Jacobi SVD which is simple and numerically stable for small matrices.
     *
     * @param a the input matrix of shape (m x n)
     * @param m number of rows
     * @param n number of columns
     * @return the thin SVD decomposition
     */
    public static SvdResult thinSvd(float[][] a, int m, int n) {
        if (m < n) {
            // For wide matrices, compute SVD of transpose and swap U/V
            float[][] at = transpose(a, m, n);
            SvdResult result = thinSvd(at, n, m);
            // A = U * S * Vt => At = V * S * Ut
            return new SvdResult(result.vt() != null ? transposeSquare(result.vt(), m) : null, result.s(), transposeToVt(result.u(), n, m));
        }

        // Copy A into working matrix (m x n)
        float[][] work = new float[m][n];
        for (int i = 0; i < m; i++) {
            System.arraycopy(a[i], 0, work[i], 0, n);
        }

        // V starts as identity (n x n)
        float[][] v = new float[n][n];
        for (int i = 0; i < n; i++) {
            v[i][i] = 1.0f;
        }

        // One-sided Jacobi: apply rotations to columns of work until convergence
        int maxIterations = 100 * n;
        for (int iter = 0; iter < maxIterations; iter++) {
            boolean converged = true;
            for (int p = 0; p < n - 1; p++) {
                for (int q = p + 1; q < n; q++) {
                    // Compute 2x2 Gram matrix elements for columns p, q
                    double app = 0, aqq = 0, apq = 0;
                    for (int i = 0; i < m; i++) {
                        app += (double) work[i][p] * work[i][p];
                        aqq += (double) work[i][q] * work[i][q];
                        apq += (double) work[i][p] * work[i][q];
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

                    // Apply rotation to columns of work
                    for (int i = 0; i < m; i++) {
                        double wp = work[i][p];
                        double wq = work[i][q];
                        work[i][p] = (float) (cos * wp - sin * wq);
                        work[i][q] = (float) (sin * wp + cos * wq);
                    }

                    // Apply rotation to columns of V
                    for (int i = 0; i < n; i++) {
                        double vp = v[i][p];
                        double vq = v[i][q];
                        v[i][p] = (float) (cos * vp - sin * vq);
                        v[i][q] = (float) (sin * vp + cos * vq);
                    }
                }
            }
            if (converged) {
                break;
            }
        }

        // Extract singular values and normalize columns of work to get U
        float[] s = new float[n];
        float[][] u = new float[m][n];
        for (int j = 0; j < n; j++) {
            double norm = 0;
            for (int i = 0; i < m; i++) {
                norm += (double) work[i][j] * work[i][j];
            }
            s[j] = (float) Math.sqrt(norm);
            if (s[j] > 1e-10f) {
                float invNorm = 1.0f / s[j];
                for (int i = 0; i < m; i++) {
                    u[i][j] = work[i][j] * invNorm;
                }
            }
        }

        // Sort by descending singular value
        sortDescending(u, s, v, m, n);

        // Vt = transpose of V
        float[][] vt = new float[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                vt[i][j] = v[j][i];
            }
        }

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
     * @param m the input matrix of shape (k x k)
     * @param k the matrix dimension
     * @return the nearest orthogonal matrix of shape (k x k)
     */
    public static float[][] procrustes(float[][] m, int k) {
        // Scale M so that all singular values are in (0, sqrt(3)) for Newton-Schulz convergence.
        float spectralNorm = estimateSpectralNorm(m, k, 50);
        double scale = 1.0 / Math.max(spectralNorm, 1e-10);

        // Work in double precision to avoid float32 accumulation errors at 352x352
        double[][] x = new double[k][k];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                x[i][j] = m[i][j] * scale;
            }
        }

        // Newton-Schulz iteration: X <- X * (3I - X^T X) / 2
        int maxIter = 100;
        for (int iter = 0; iter < maxIter; iter++) {
            // Compute X^T X (k x k) using row-broadcast for cache efficiency
            double[][] xtx = new double[k][k];
            for (int l = 0; l < k; l++) {
                double[] xRow = x[l];
                for (int i = 0; i < k; i++) {
                    double xli = xRow[i];
                    double[] xtxRow = xtx[i];
                    for (int j = i; j < k; j++) {
                        xtxRow[j] += xli * xRow[j];
                    }
                }
            }
            // Symmetrize
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < i; j++) {
                    xtx[i][j] = xtx[j][i];
                }
            }

            // Check convergence: X^T X should be close to I
            double maxOff = 0;
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    double expected = (i == j) ? 1.0 : 0.0;
                    maxOff = Math.max(maxOff, Math.abs(xtx[i][j] - expected));
                }
            }
            if (maxOff < 1e-12) {
                break;
            }

            // B = (3I - X^T X) / 2
            double[][] b = new double[k][k];
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < k; j++) {
                    b[i][j] = -xtx[i][j] / 2.0;
                }
                b[i][i] += 1.5;
            }

            // X_new = X @ B (row-broadcast for JIT vectorization)
            double[][] xNew = new double[k][k];
            for (int i = 0; i < k; i++) {
                double[] xRow = x[i];
                double[] xNewRow = xNew[i];
                for (int l = 0; l < k; l++) {
                    double xVal = xRow[l];
                    double[] bRow = b[l];
                    for (int j = 0; j < k; j++) {
                        xNewRow[j] += xVal * bRow[j];
                    }
                }
            }
            x = xNew;
        }

        // Convert back to float
        float[][] result = new float[k][k];
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                result[i][j] = (float) x[i][j];
            }
        }
        return result;
    }

    /**
     * Estimates the spectral norm (largest singular value) of a k x k matrix using power iteration on M^T M.
     */
    private static float estimateSpectralNorm(float[][] m, int k, int iterations) {
        // Power iteration on M^T M to find largest eigenvalue (= sigma_max^2)
        float[] v = new float[k];
        // Initialize with uniform vector
        float initVal = (float) (1.0 / Math.sqrt(k));
        for (int i = 0; i < k; i++) {
            v[i] = initVal;
        }
        float[] mv = new float[k];
        float[] mtmv = new float[k];
        for (int iter = 0; iter < iterations; iter++) {
            // mv = M @ v
            for (int i = 0; i < k; i++) {
                double sum = 0;
                for (int j = 0; j < k; j++) {
                    sum += (double) m[i][j] * v[j];
                }
                mv[i] = (float) sum;
            }
            // mtmv = M^T @ mv
            double normSq = 0;
            for (int j = 0; j < k; j++) {
                double sum = 0;
                for (int i = 0; i < k; i++) {
                    sum += (double) m[i][j] * mv[i];
                }
                mtmv[j] = (float) sum;
                normSq += sum * sum;
            }
            // Normalize
            double norm = Math.sqrt(normSq);
            if (norm < 1e-30) return 0f;
            for (int j = 0; j < k; j++) {
                v[j] = (float) (mtmv[j] / norm);
            }
        }
        // Compute ||M @ v|| which approximates sigma_max
        double mvNormSq = 0;
        for (int i = 0; i < k; i++) {
            double sum = 0;
            for (int j = 0; j < k; j++) {
                sum += (double) m[i][j] * v[j];
            }
            mvNormSq += sum * sum;
        }
        return (float) Math.sqrt(mvNormSq);
    }

    private static void sortDescending(float[][] u, float[] s, float[][] v, int m, int n) {
        // Simple insertion sort (n is small)
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
                // Swap columns of U
                for (int r = 0; r < m; r++) {
                    tmp = u[r][i];
                    u[r][i] = u[r][maxIdx];
                    u[r][maxIdx] = tmp;
                }
                // Swap columns of V
                for (int r = 0; r < n; r++) {
                    tmp = v[r][i];
                    v[r][i] = v[r][maxIdx];
                    v[r][maxIdx] = tmp;
                }
            }
        }
    }

    private static float[][] transpose(float[][] a, int m, int n) {
        float[][] at = new float[n][m];
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                at[j][i] = a[i][j];
            }
        }
        return at;
    }

    private static float[][] transposeSquare(float[][] a, int n) {
        float[][] at = new float[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                at[i][j] = a[j][i];
            }
        }
        return at;
    }

    private static float[][] transposeToVt(float[][] u, int k, int m) {
        // u is (m x k), we want (k x m)
        float[][] vt = new float[k][m];
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < k; j++) {
                vt[j][i] = u[i][j];
            }
        }
        return vt;
    }

    /**
     * Computes the top-k right singular vectors of matrix A (m x n) using power iteration
     * on the Gram matrix A^T A with deflation. Much faster than full SVD when k is small.
     *
     * @param a matrix of shape (m x n)
     * @param m number of rows
     * @param n number of columns
     * @param k number of top singular vectors to extract
     * @param seed random seed for initialization
     * @return top-k right singular vectors as rows (k x n)
     */
    public static float[][] topKRightSingularVectors(float[][] a, int m, int n, int k, long seed) {
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

    private static float[][] topKEigenvectorsGram(float[][] a, int m, int n, int k, long seed) {
        // Block (subspace) power iteration: process all k vectors simultaneously.
        // V = random (n x k), iterate: V <- A^T (A V), then QR-orthogonalize.
        // This is O(iterations * m * n * k) total -- much faster than deflation for large k.
        //
        // We store V in column-major form (n x k) for QR, but use a transposed (k x n) copy
        // for the matmul inner loops to enable row-contiguous access and JIT vectorization.
        java.util.Random rng = new java.util.Random(seed);
        int iters = 20; // sufficient for PCA init that gets refined by Procrustes

        // V: (n x k) -- each column is a candidate eigenvector
        float[][] v = new float[n][k];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < k; j++) {
                v[i][j] = (float) rng.nextGaussian();
            }
        }
        qrOrthogonalize(v, n, k);

        for (int iter = 0; iter < iters; iter++) {
            // Build transposed view vT (k x n) for cache-friendly row access in matmul
            float[][] vT = new float[k][n];
            for (int d = 0; d < n; d++) {
                for (int j = 0; j < k; j++) {
                    vT[j][d] = v[d][j];
                }
            }

            // W = A @ V (m x k): w[i][j] = dot(a[i], vT[j])
            float[][] w = new float[m][k];
            for (int i = 0; i < m; i++) {
                float[] aRow = a[i];
                float[] wRow = w[i];
                for (int j = 0; j < k; j++) {
                    float[] vtRow = vT[j];
                    float sum = 0;
                    for (int d = 0; d < n; d++) {
                        sum += aRow[d] * vtRow[d];
                    }
                    wRow[j] = sum;
                }
            }

            // V_new = A^T @ W (n x k): use row-broadcast accumulation
            float[][] vNew = new float[n][k];
            for (int i = 0; i < m; i++) {
                float[] aRow = a[i];
                float[] wRow = w[i];
                for (int d = 0; d < n; d++) {
                    float aVal = aRow[d];
                    float[] vNewRow = vNew[d];
                    for (int j = 0; j < k; j++) {
                        vNewRow[j] += aVal * wRow[j];
                    }
                }
            }
            qrOrthogonalize(vNew, n, k);
            v = vNew;
        }

        // Convert columns of V to rows for return format (k x n)
        float[][] result = new float[k][n];
        for (int j = 0; j < k; j++) {
            for (int d = 0; d < n; d++) {
                result[j][d] = v[d][j];
            }
        }
        return result;
    }

    /**
     * Modified Gram-Schmidt QR orthogonalization in-place on columns of V (n x k).
     */
    private static void qrOrthogonalize(float[][] v, int n, int k) {
        for (int j = 0; j < k; j++) {
            // Subtract projections of previous columns
            for (int prev = 0; prev < j; prev++) {
                double dot = 0;
                for (int i = 0; i < n; i++) {
                    dot += (double) v[i][j] * v[i][prev];
                }
                for (int i = 0; i < n; i++) {
                    v[i][j] -= (float) (dot * v[i][prev]);
                }
            }
            // Normalize column j
            double normSq = 0;
            for (int i = 0; i < n; i++) {
                normSq += (double) v[i][j] * v[i][j];
            }
            float invNorm = (float) (1.0 / Math.sqrt(normSq));
            if (Float.isFinite(invNorm)) {
                for (int i = 0; i < n; i++) {
                    v[i][j] *= invNorm;
                }
            }
        }
    }

    private static float[][] topKEigenvectorsGramTranspose(float[][] a, int m, int n, int k, long seed) {
        // A is (m x n) with m < n. Find top-k eigenvectors of A A^T (m x m), then transform.
        // u_i = eigenvector of A A^T => v_i = A^T u_i / sigma_i (right singular vector)
        float[][] result = new float[k][n];
        java.util.Random rng = new java.util.Random(seed);
        float[][] deflated = new float[k][];
        int found = 0;

        for (int vec = 0; vec < k; vec++) {
            // Random initial vector (m-dimensional)
            float[] u = new float[m];
            for (int i = 0; i < m; i++) {
                u[i] = (float) rng.nextGaussian();
            }
            normalize(u);

            // Power iteration on A A^T: u <- A (A^T u) / ||...||
            for (int iter = 0; iter < 100; iter++) {
                // w = A^T u (n-dimensional)
                float[] w = new float[n];
                for (int j = 0; j < n; j++) {
                    double sum = 0;
                    for (int i = 0; i < m; i++) {
                        sum += (double) a[i][j] * u[i];
                    }
                    w[j] = (float) sum;
                }
                // u_new = A w (m-dimensional)
                float[] uNew = new float[m];
                for (int i = 0; i < m; i++) {
                    double sum = 0;
                    for (int j = 0; j < n; j++) {
                        sum += (double) a[i][j] * w[j];
                    }
                    uNew[i] = (float) sum;
                }
                // Deflate
                for (int d = 0; d < found; d++) {
                    double dot = 0;
                    for (int i = 0; i < m; i++) {
                        dot += (double) uNew[i] * deflated[d][i];
                    }
                    for (int i = 0; i < m; i++) {
                        uNew[i] -= (float) (dot * deflated[d][i]);
                    }
                }
                normalize(uNew);
                u = uNew;
            }
            deflated[found] = u;
            found++;

            // Recover right singular vector: v = A^T u, then normalize
            float[] sv = new float[n];
            for (int j = 0; j < n; j++) {
                double sum = 0;
                for (int i = 0; i < m; i++) {
                    sum += (double) a[i][j] * u[i];
                }
                sv[j] = (float) sum;
            }
            normalize(sv);
            result[vec] = sv;
        }
        return result;
    }

    private static void normalize(float[] v) {
        double norm = 0;
        for (float f : v) {
            norm += (double) f * f;
        }
        norm = Math.sqrt(norm);
        if (norm > 0) {
            float invNorm = (float) (1.0 / norm);
            for (int i = 0; i < v.length; i++) {
                v[i] *= invNorm;
            }
        }
    }
}
