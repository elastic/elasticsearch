/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Manifold model for distance/similarity as a function of rank and corpus size.
 * Fits a log-linear model: log(distance at rank k) ~ alpha + invDim * (log(k) - log(N)).
 * Used in calibration to predict expected distances and compute expected recall@k.
 * <p>
 * Average rank-distance estimation:
 * per-query {@code TopK} heaps of capacity {@code 6 * k} are fed successive corpus slices
 * without reset, so each sweep step uses the cumulative corpus prefix (not disjoint chunks).
 * Dot, cosine, and maximum-inner-product metrics use negated float dot product in the heap
 * (no extra L2 normalization).
 */
public final class ManifoldModel {
    private static final Logger logger = LogManager.getLogger(ManifoldModel.class);

    /**
     * multipliers for the manifold rank sweep:
     * rank = floor(multiplier * k / 5), multipliers descending 29..5.
     */
    private static final int[] RANK_MULTIPLIERS = {
        29,
        28,
        27,
        26,
        25,
        24,
        23,
        22,
        21,
        20,
        19,
        18,
        17,
        16,
        15,
        14,
        13,
        12,
        11,
        10,
        9,
        8,
        7,
        6,
        5 };

    private static final int[] SAMPLE_SIZES = {
        4096,
        4608,
        5120,
        5632,
        6144,
        6656,
        7168,
        7680,
        8192,
        8704,
        9216,
        9728,
        10240,
        10752,
        11264,
        11776,
        12288,
        12800,
        13312,
        13824,
        14336,
        14848,
        15360,
        15872,
        16384 };

    static final int[] RANKS_FOR_K = { 29, 25, 21, 17, 13, 9, 7, 5 };

    static final int[] SAMPLE_SIZES_FAST = { 4096, 5632, 7168, 8704, 10240, 11776, 13312, 16384 };

    private static final int PARALLEL_QUERY_THRESHOLD = Integer.MAX_VALUE;

    private ManifoldModel() {}

    /**
     * Estimate manifold parameters (alpha, invDim) using default (full) sample sizes.
     */
    public static double[] estimateManifoldParameters(
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int k
    ) throws IOException {
        return estimateManifoldParameters(similarityFunction, dim, queries, fvv, corpusOrdinals, k, ranksForK(k), SAMPLE_SIZES);
    }

    /**
     * Estimate manifold parameters (alpha, invDim) using reduced sample sizes for faster execution.
     */
    public static double[] estimateManifoldParametersFast(
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int k
    ) throws IOException {
        return estimateManifoldParameters(similarityFunction, dim, queries, fvv, corpusOrdinals, k, ranksForKFast(k), SAMPLE_SIZES_FAST);
    }

    static int[] ranksForK(int k) {
        return ranksFromMultipliers(RANK_MULTIPLIERS, k);
    }

    static int[] ranksForKFast(int k) {
        return ranksFromMultipliers(RANKS_FOR_K, k);
    }

    private static int[] ranksFromMultipliers(int[] multipliers, int k) {
        int[] ranks = new int[multipliers.length];
        for (int i = 0; i < multipliers.length; i++) {
            ranks[i] = Math.max(1, (multipliers[i] * k) / 5);
        }
        return ranks;
    }

    /**
     * Estimate manifold parameters (log(alpha), invDim) from query-corpus distances at various
     * ranks and sample sizes. Corpus vectors are accessed lazily via {@code fvv} and
     * {@code corpusOrdinals}.
     *
     * @param fvv the underlying vector values source
     * @param corpusOrdinals ordinal indices into {@code fvv} for the corpus subset
     * @param ranksForK the rank values to sweep
     * @param sampleSizes the corpus sample sizes to sweep (must be same length as ranksForK)
     * @return double[2] containing {log(alpha), invDim}
     */
    static double[] estimateManifoldParameters(
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int k,
        int[] ranksForK,
        int[] sampleSizes
    ) throws IOException {
        long startNanos = System.nanoTime();
        int nQueries = queries.size();
        int nDocsTotal = corpusOrdinals.length;
        int m = Math.min(ranksForK.length, sampleSizes.length);

        int logCount = 0;
        double[] logRanks = new double[m];
        double[] logSampleSizes = new double[m];
        double[] logDistances = new double[m];

        float[] queryScratch = new float[dim];
        ManifoldTopK[] topKs = new ManifoldTopK[nQueries];
        for (int qi = 0; qi < nQueries; qi++) {
            topKs[qi] = new ManifoldTopK(similarityFunction, 6 * k);
        }

        int sampleStart = 0;
        for (int i = 0; i < m; i++) {
            int rank = ranksForK[i];
            int sampleEnd = sampleSizes[i];
            if (sampleEnd > nDocsTotal) {
                break;
            }
            double avgDist;
            // compute average rank-th distance across sampled queries
            if (nQueries >= PARALLEL_QUERY_THRESHOLD && Runtime.getRuntime().availableProcessors() > 1) {
                avgDist = averageIthDistanceParallel(dim, rank, queries, fvv, corpusOrdinals, sampleStart, sampleEnd, topKs, nQueries);
            } else {
                double sum = 0;
                for (int qi = 0; qi < nQueries; qi++) {
                    queries.copyQuery(qi, false, queryScratch);
                    topKs[qi].add(dim, queryScratch, fvv, corpusOrdinals, sampleStart, sampleEnd);
                    sum += topKs[qi].ithDistance(rank);
                }
                avgDist = sum / nQueries;
            }
            logRanks[logCount] = Math.log(rank);
            logSampleSizes[logCount] = Math.log(sampleSizes[i]);
            logDistances[logCount] = Math.log(avgDist);
            logCount++;
            sampleStart = sampleEnd;
        }
        if (logCount < 2) {
            return new double[] { 0, 0 };
        }
        // build regression variables
        // x = log(rank) - log(sampleSize) = log(k/N)
        // y = log(distance)
        double[] x = new double[logCount];
        for (int i = 0; i < logCount; i++) {
            x[i] = logRanks[i] - logSampleSizes[i];
        }
        double[] y = new double[logCount];
        System.arraycopy(logDistances, 0, y, 0, logCount);

        // for different sample sizes, the typical distance to the rank-th nearest neighbor is avgDist.
        // log(alpha) + (1/d) * (log(rank) - log(sampleSize)) = log(distance)
        // fit regression model (log(alpha) and 1/d) and compute R²
        Regression.OLSResult res = Regression.fitOls(x, y);
        double r2 = Regression.rSquared(x, y, res); // coefficient of determination for the fitted model
        double elapsed = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        logger.info(
            "------------------------------------------\nEstimated manifold parameters in [{}]s\ndist(k) = [{}] * (k/N)^[{}] (R² = [{}])",
            String.format(Locale.ROOT, "%.4f", elapsed),
            String.format(Locale.ROOT, "%.4f", Math.exp(res.beta0())),
            String.format(Locale.ROOT, "%.4f", res.beta1()),
            String.format(Locale.ROOT, "%.4f", r2)
        );
        return new double[] { res.beta0(), res.beta1() };
    }

    private static double averageIthDistanceParallel(
        int dim,
        int rank,
        CalibrationQueries queries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int start,
        int end,
        ManifoldTopK[] topKs,
        int nQueries
    ) throws IOException {
        // TODO : revisit usage of workers / thread pool
        int workers = Math.min(nQueries, Runtime.getRuntime().availableProcessors());
        double[] partialSums = new double[nQueries];
        int[] queryStarts = new int[workers + 1];
        for (int w = 0; w <= workers; w++) {
            queryStarts[w] = w * nQueries / workers;
        }
        try (ExecutorService pool = Executors.newFixedThreadPool(workers, r -> {
            Thread t = new Thread(r, "manifold-calibration");
            t.setDaemon(true);
            return t;
        });) {

            List<Future<?>> futures = new ArrayList<>(workers);
            for (int w = 0; w < workers; w++) {
                int w0 = w;
                FloatVectorValues fvvCopy = fvv.copy();
                futures.add(pool.submit(() -> {
                    float[] localQueryScratch = new float[dim];
                    for (int qi = queryStarts[w0]; qi < queryStarts[w0 + 1]; qi++) {
                        try {
                            queries.copyQuery(qi, false, localQueryScratch);
                            topKs[qi].add(dim, localQueryScratch, fvvCopy, corpusOrdinals, start, end);
                            partialSums[qi] = topKs[qi].ithDistance(rank);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Throwable t) {
            throw new IOException(t);
        }
        double sum = 0;
        for (int qi = 0; qi < nQueries; qi++) {
            sum += partialSums[qi];
        }
        return sum / nQueries;
    }

    /**
     * Max-heap of up to {@code k} smallest float distances (float dot / float squared Euclidean).
     */
    static final class ManifoldTopK {
        private final VectorSimilarityFunction similarityFunction;
        private final int capacity;
        private final PriorityQueue<Float> pq;

        ManifoldTopK(VectorSimilarityFunction similarityFunction, int capacity) {
            this.similarityFunction = similarityFunction;
            this.capacity = capacity;
            this.pq = new PriorityQueue<>(capacity, Comparator.reverseOrder());
        }

        void add(int dim, float[] query, FloatVectorValues fvv, int[] corpusOrdinals, int startDoc, int endDoc) throws IOException {
            boolean dotLike = isDotLike(similarityFunction);
            for (int d = startDoc; d < endDoc; d++) {
                float[] doc = fvv.vectorValue(corpusOrdinals[d]);
                float dist = dotLike ? -dotFloat(dim, query, doc) : euclideanSqFloat(dim, query, doc);
                if (pq.size() < capacity) {
                    pq.offer(dist);
                } else if (dist < pq.peek()) {
                    pq.poll();
                    pq.offer(dist);
                }
            }
        }

        /**
         * {@code rank}-th smallest stored distance (1-based).
         */
        float ithDistance(int rank) {
            PriorityQueue<Float> pqCopy = new PriorityQueue<>(capacity, Comparator.reverseOrder());
            pqCopy.addAll(pq);
            for (int j = 0; j < capacity - rank; j++) {
                pqCopy.poll();
            }
            Float top = pqCopy.peek();
            if (top == null) {
                return 0f;
            }
            float val = top;
            return isDotLike(similarityFunction) ? -val : val;
        }
    }

    /** Float dot product. */
    static float dotFloat(int dim, float[] x, float[] y) {
        float sum = 0f;
        for (int i = 0; i < dim; i++) {
            sum += x[i] * y[i];
        }
        return sum;
    }

    /** Float squared Euclidean distance. */
    static float euclideanSqFloat(int dim, float[] x, float[] y) {
        float sum = 0f;
        for (int i = 0; i < dim; i++) {
            float d = x[i] - y[i];
            sum += d * d;
        }
        return sum;
    }

    /**
     * Expected distance/similarity value at rank k in a corpus of size N from the manifold model.
     */
    public static double expectedRankDistance(VectorSimilarityFunction similarityFunction, double alpha, double invDim, int N, int k) {
        double logK = Math.log(k);
        double logN = Math.log(N);
        if (isDotLike(similarityFunction)) {
            return -Math.exp(alpha + (logK - logN) * invDim);
        }
        return Math.exp(alpha + (logK - logN) * invDim);
    }

    static boolean isDotLike(VectorSimilarityFunction similarityFunction) {
        return similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
            || similarityFunction == VectorSimilarityFunction.COSINE
            || similarityFunction == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
    }
}
