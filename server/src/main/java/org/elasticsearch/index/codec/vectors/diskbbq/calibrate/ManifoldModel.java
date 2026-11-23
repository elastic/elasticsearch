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
 * Manifold model for distance as a function of rank and corpus size.
 * Fits a log-linear model: log(distance at rank k) ~ alpha + invDim * (log(k) - log(N)).
 * Used in calibration to predict expected distances and compute expected recall@k.
 * <p>
 * The model is fit in <em>distance</em> units for every metric: the squared distance {@code ||q - x||^2} for
 * Euclidean and the half squared distance {@code r = 0.5 (||q||^2 + ||x||^2) - q.x} ({@code 1 - cos} for unit
 * vectors) for dot, cosine and maximum-inner-product. The power law {@code dist(k) ~ (k/N)^(1/d)} describes
 * distances in a locally uniform neighbourhood; fitting it to the similarity level instead (as this code used
 * to for dot-like metrics) gives an exponent near zero for typical embeddings, whose 10th-neighbour similarity
 * sits in a narrow band (0.75-0.9 for E5) whatever the neighbourhood, and an exponent near zero tells the recall
 * model that gaps do not shrink with corpus size at all (measured on FiQA/E5 the rank-10-to-30 gap shrinks 4%
 * per doubling of N; the similarity-space fit said 0.6%, the distance-space fit says 3.3%). {@code r} is affine
 * in {@code -similarity} for a fixed query, so rank-to-rank gaps -- all the recall integral uses -- are the
 * similarity gaps, and the quantization error measured in similarity units needs no conversion. {@code invDim}
 * is therefore positive for every metric and {@link #expectedRankDistance} increases with rank for every metric.
 * <p>
 * Average rank-distance estimation:
 * per-query {@code TopK} heaps of capacity {@code 6 * k} are fed successive corpus slices
 * without reset, so each sweep step uses the cumulative corpus prefix (not disjoint chunks).
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
     * Parameters of the log-linear manifold model: {@code log(dist at rank k) = alpha + invDim * (log(k) - log(N))}.
     *
     * @param alpha  OLS intercept (log-scale)
     * @param invDim OLS slope (approximates 1/intrinsic-dimension of the corpus)
     */
    public record ManifoldParams(double alpha, double invDim) {}

    /**
     * Estimate manifold parameters (alpha, invDim) using default sample sizes.
     * Query buffers are sized to {@link CalibrationSource#workingDim()}, which already incorporates
     * any Neyshabur lift or cosine normalization applied when the source was built.
     */
    public static ManifoldParams estimateManifoldParameters(CalibrationSource source) throws IOException {
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
     * @return {@link ManifoldParams} containing {log(alpha), invDim}
     */
    static ManifoldParams estimateManifoldParameters(CalibrationSource source, int[] ranksForK) throws IOException {
        int nQueries = source.queryOrdinals().length;
        int nDocsTotal = source.corpusOrdinals().length;
        int m = Math.min(ranksForK.length, ManifoldModel.SAMPLE_SIZES.length);
        int dimWork = source.workingDim();

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
            topKs[qi] = new ManifoldTopK(dotLike, 6 * source.k(), ESVectorUtil.dotProduct(queries[qi], queries[qi]));
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
                // For dot-like metrics the heap key is r = 0.5 ||x||^2 - q.x (plus the per-query constant
                // 0.5 ||q||^2 on output): the half squared distance. One extra dot product per document per step.
                float docNormSq = dotLike ? ESVectorUtil.dotProduct(cv, cv) : 0f;
                int qi = 0;
                for (; qi < bulkLimit; qi += 4) {
                    if (dotLike) {
                        ESVectorUtil.dotProductBulk(cv, queries[qi], queries[qi + 1], queries[qi + 2], queries[qi + 3], 0, bulkDistances);
                        topKs[qi].considerCandidate(-bulkDistances[0], docNormSq);
                        topKs[qi + 1].considerCandidate(-bulkDistances[1], docNormSq);
                        topKs[qi + 2].considerCandidate(-bulkDistances[2], docNormSq);
                        topKs[qi + 3].considerCandidate(-bulkDistances[3], docNormSq);
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
                    topKs[qi].considerCandidate(dist, docNormSq);
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
            return new ManifoldParams(0, 0);
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
        return new ManifoldParams(res.beta0(), res.beta1());
    }

    /**
     * Tracks up to {@code capacity} smallest distances: squared distances for Euclidean, and for dot-like metrics
     * the half squared distance {@code r = 0.5 ||x||^2 - q.x + 0.5 ||q||^2}, of which the heap stores the
     * document-dependent part {@code 0.5 ||x||^2 - q.x} (the caller supplies {@code -q.x} and {@code ||x||^2}) and
     * {@link #ithDistance} adds the per-query constant. For unit vectors this orders exactly like {@code -q.x}.
     * {@link #ithDistance} sorts a reusable scratch buffer instead of cloning and draining a heap.
     */
    static final class ManifoldTopK {
        private final boolean isDotLike;
        private final int capacity;
        private final float[] buffer;
        private final float[] scratch;
        private float queryNormSq;
        private int size;
        private int maxIndex;

        ManifoldTopK(boolean isDotLike, int capacity) {
            this(isDotLike, capacity, 0f);
        }

        /** @param queryNormSq {@code ||q||^2}, needed for dot-like metrics to report {@code r} rather than {@code r - 0.5 ||q||^2} */
        ManifoldTopK(boolean isDotLike, int capacity, float queryNormSq) {
            this.isDotLike = isDotLike;
            this.capacity = capacity;
            this.buffer = new float[capacity];
            this.scratch = new float[capacity];
            this.queryNormSq = queryNormSq;
        }

        void setQueryNormSq(float queryNormSq) {
            this.queryNormSq = queryNormSq;
        }

        /** Euclidean: the squared distance. Dot-like callers must use {@link #considerCandidate(float, float)}. */
        void considerCandidate(float dist) {
            considerCandidate(dist, 0f);
        }

        /**
         * @param dist   squared distance for Euclidean; {@code -q.x} for dot-like metrics
         * @param normSq {@code ||x||^2} of the candidate (ignored for Euclidean)
         */
        void considerCandidate(float dist, float normSq) {
            float key = isDotLike ? dist + 0.5f * normSq : dist;
            if (size < capacity) {
                buffer[size++] = key;
                if (size == capacity) {
                    updateMaxIndex();
                }
            } else if (key < buffer[maxIndex]) {
                buffer[maxIndex] = key;
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
         * {@code rank}-th smallest stored distance (1-based), in the units the model is fit in.
         */
        float ithDistance(int rank) {
            if (size == 0 || rank <= 0) {
                return 0f;
            }
            System.arraycopy(buffer, 0, scratch, 0, size);
            Arrays.sort(scratch, 0, size);
            float val = scratch[Math.min(rank, size) - 1];
            return isDotLike ? val + 0.5f * queryNormSq : val;
        }
    }

    /**
     * Expected distance at rank k in a corpus of size N from the manifold model: positive and increasing in k for
     * every metric (squared distance for Euclidean, the half squared distance {@code r} for dot-like, see the class
     * Javadoc). {@code similarityFunction} is kept for signature compatibility; the units differ, the convention does not.
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
        return Math.exp(alpha + (logK - logN) * invDim);
    }

    public static boolean isDotLike(VectorSimilarityFunction similarityFunction) {
        return similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
            || similarityFunction == VectorSimilarityFunction.COSINE
            || similarityFunction == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
    }
}
