/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.core.Strings.format;

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
    static final int[] RANK_MULTIPLIERS = { 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5 };

    /**
     * min to max corpus slice (with fixed 512 steps) for a stable k-NN statistic (empirically determined).
     */
    static final int[] SAMPLE_SIZES = {
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

    private ManifoldModel() {}

    /**
     * Estimate manifold parameters (alpha, invDim) using default sample sizes.
     * Query buffers are sized from {@link CalibrationUtils#calibrationQueryDimension(int, boolean)}
     * so cosine normalization and Neyshabur lift are supported.
     */
    public static double[] estimateManifoldParameters(CalibrationSource source) throws IOException {
        return estimateManifoldParameters(source, ranksFromMultipliers(source.k()));
    }

    static int[] ranksFromMultipliers(int k) {
        int[] ranks = new int[ManifoldModel.RANK_MULTIPLIERS.length];
        for (int i = 0; i < ManifoldModel.RANK_MULTIPLIERS.length; i++) {
            ranks[i] = Math.max(1, (ManifoldModel.RANK_MULTIPLIERS[i] * k) / 5);
        }
        return ranks;
    }

    /**
     * Estimate manifold parameters (log(alpha), invDim) from query-corpus distances at various
     * ranks and sample sizes. Corpus vectors are accessed lazily via {@link CalibrationSource#vectors()}
     * and {@link CalibrationSource#corpusOrdinals()}.
     *
     * @param source    calibration context (similarity function, vectors, query set, target k)
     * @param ranksForK the rank values to sweep
     * @return double[2] containing {log(alpha), invDim}
     */
    static double[] estimateManifoldParameters(CalibrationSource source, int[] ranksForK) throws IOException {
        int nQueries = source.queryOrdinals().length;
        int nDocsTotal = source.corpusOrdinals().length;
        int m = Math.min(ranksForK.length, ManifoldModel.SAMPLE_SIZES.length);
        int dimWork = CalibrationUtils.calibrationQueryDimension(source.baseDim(), source.neyshabur());

        int logCount = 0;
        double[] logRanks = new double[m];
        double[] logSampleSizes = new double[m];
        double[] logDistances = new double[m];
        boolean dotLike = isDotLike(source.similarityFunction());

        // materialize the query sample once, on-heap (nQueries <= MAX_QUERY_SAMPLE). Corpus vectors
        // are then streamed once per sweep step and scored against every query, so each corpus read happens once
        // per step rather than once per (step, query).
        FloatVectorValues vectors = source.vectors();
        int[] corpusOrdinals = source.corpusOrdinals();
        float[][] queries = new float[nQueries][dimWork];
        for (int qi = 0; qi < nQueries; qi++) {
            CalibrationUtils.materializeCalibrationQuery(
                vectors,
                source.queryOrdinals()[qi],
                source.baseDim(),
                dimWork,
                source.cosine(),
                source.neyshabur(),
                null,
                false,
                queries[qi],
                null
            );
        }

        ManifoldTopK[] topKs = new ManifoldTopK[nQueries];
        for (int qi = 0; qi < nQueries; qi++) {
            topKs[qi] = new ManifoldTopK(source.similarityFunction(), 6 * source.k());
        }
        float[] bulkDistances = new float[4];

        int sampleStart = 0;
        for (int i = 0; i < m; i++) {
            int rank = ranksForK[i];
            int sampleEnd = ManifoldModel.SAMPLE_SIZES[i];
            if (sampleEnd > nDocsTotal) {
                break;
            }
            // feed corpus slice [sampleStart, sampleEnd) to every query's heap, reading each corpus vector once
            // and bulk-scoring it against four on-heap queries at a time (dot product / square distance are symmetric).
            int bulkLimit = nQueries - 3;
            for (int d = sampleStart; d < sampleEnd; d++) {
                float[] cv = vectors.vectorValue(corpusOrdinals[d]);
                int qi = 0;
                for (; qi < bulkLimit; qi += 4) {
                    if (dotLike) {
                        ESVectorUtil.dotProductBulk(cv, queries[qi], queries[qi + 1], queries[qi + 2], queries[qi + 3], 0, bulkDistances);
                        topKs[qi].considerCandidate(-bulkDistances[0]);
                        topKs[qi + 1].considerCandidate(-bulkDistances[1]);
                        topKs[qi + 2].considerCandidate(-bulkDistances[2]);
                        topKs[qi + 3].considerCandidate(-bulkDistances[3]);
                    } else {
                        ESVectorUtil.squareDistanceBulk(
                            cv,
                            0,
                            dimWork,
                            queries[qi],
                            queries[qi + 1],
                            queries[qi + 2],
                            queries[qi + 3],
                            bulkDistances
                        );
                        topKs[qi].considerCandidate(bulkDistances[0]);
                        topKs[qi + 1].considerCandidate(bulkDistances[1]);
                        topKs[qi + 2].considerCandidate(bulkDistances[2]);
                        topKs[qi + 3].considerCandidate(bulkDistances[3]);
                    }
                }
                for (; qi < nQueries; qi++) {
                    float dist = dotLike ? -ESVectorUtil.dotProduct(cv, queries[qi]) : ESVectorUtil.squareDistance(cv, queries[qi]);
                    topKs[qi].considerCandidate(dist);
                }
            }
            double sum = 0;
            for (int qi = 0; qi < nQueries; qi++) {
                sum += topKs[qi].ithDistance(rank);
            }
            double avgDist = sum / nQueries;
            logRanks[logCount] = Math.log(rank);
            logSampleSizes[logCount] = Math.log(ManifoldModel.SAMPLE_SIZES[i]);
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
        logger.debug(
            () -> format(
                "Estimated manifold parameters: dist(k) = [%.4f] * (k/N)^[%.4f] (R² = [%.4f])",
                Math.exp(res.beta0()),
                res.beta1(),
                r2
            )
        );
        return new double[] { res.beta0(), res.beta1() };
    }

    /**
     * Tracks up to {@code capacity} smallest distances (negated dot product for dot-like metrics).
     * {@link #ithDistance} sorts a reusable scratch buffer instead of cloning and draining a heap.
     */
    static final class ManifoldTopK {
        private final VectorSimilarityFunction similarityFunction;
        private final int capacity;
        private final float[] buffer;
        private final float[] scratch;
        private int size;
        private int maxIndex;

        ManifoldTopK(VectorSimilarityFunction similarityFunction, int capacity) {
            this.similarityFunction = similarityFunction;
            this.capacity = capacity;
            this.buffer = new float[capacity];
            this.scratch = new float[capacity];
        }

        void considerCandidate(float dist) {
            if (size < capacity) {
                buffer[size++] = dist;
                if (size == capacity) {
                    updateMaxIndex();
                }
            } else if (dist < buffer[maxIndex]) {
                buffer[maxIndex] = dist;
                updateMaxIndex();
            }
        }

        private void updateMaxIndex() {
            maxIndex = 0;
            float max = buffer[0];
            for (int i = 1; i < size; i++) {
                if (buffer[i] > max) {
                    max = buffer[i];
                    maxIndex = i;
                }
            }
        }

        /**
         * {@code rank}-th smallest stored distance (1-based).
         */
        float ithDistance(int rank) {
            if (size == 0 || rank <= 0) {
                return 0f;
            }
            System.arraycopy(buffer, 0, scratch, 0, size);
            Arrays.sort(scratch, 0, size);
            float val = scratch[Math.min(rank, size) - 1];
            return isDotLike(similarityFunction) ? -val : val;
        }
    }

    /**
     * Expected distance/similarity value at rank k in a corpus of size N from the manifold model.
     */
    public static double expectedRankDistance(
        VectorSimilarityFunction similarityFunction,
        double alpha,
        double invDim,
        int numDocs,
        int k
    ) {
        double logK = Math.log(k);
        double logN = Math.log(numDocs);
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
