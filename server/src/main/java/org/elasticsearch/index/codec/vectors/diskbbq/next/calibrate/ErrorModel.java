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
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Error model for quantization error in scalar quantization.
 * Estimates the standard deviation of the error in distance/similarity after quantizing
 * queries and documents. Used by calibration to predict recall.
 */
public final class ErrorModel {

    private static final Logger logger = LogManager.getLogger(ErrorModel.class);

    static final int N_QUERY_CLUSTERS = 32;

    static final int[] SAMPLE_SIZES_SCALING = { 4096, 5120, 6144, 7168, 8192, 9216, 10240, 11264, 12288, 13312, 14336, 15360 };

    static final int[] SAMPLE_SIZES_MAGNITUDE = { 2048, 3072, 4096 };

    /** Scaling fit always uses (qbits=4, dbits=1), matching {@code ONE_BIT_4BIT_QUERY}. */
    private static final int SCALING_QBITS = 4;
    private static final int SCALING_DBITS = 1;

    private ErrorModel() {}

    private static HierarchicalKMeans calibrationKMeans(int dim) {
        return HierarchicalKMeans.ofSerial(dim);
    }

    /**
     * Exact similarity between two vectors, consistent with the metric convention
     * where higher values indicate greater similarity.
     */
    static double simExact(VectorSimilarityFunction sim, int dim, float[] y, float[] x) {
        if (sim == VectorSimilarityFunction.EUCLIDEAN) {
            return 2.0 * CalibrationUtils.dot(dim, y, x) - CalibrationUtils.dot(dim, x, x);
        }
        return CalibrationUtils.dot(dim, y, x);
    }

    static long dotInt(int dim, int[] x, int xOff, int[] y, int yOff) {
        long sum = 0;
        for (int i = 0; i < dim; i++) {
            sum += (long) x[xOff + i] * y[yOff + i];
        }
        return sum;
    }

    /**
     * Quantized representation error standard deviation. Quantizes doc residuals and
     * query residuals using OSQ, estimates dot products, and compares to exact
     * similarities for the top-5k ranked documents per query.
     */
    static double quantizedRepErrorStd(
        VectorSimilarityFunction sim,
        int dim,
        CalibrationQueries queries,
        boolean usePreconditioned,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int corpusLength,
        boolean cosine,
        int[] docAssignments,
        float[][] docCentroids,
        int nQueryClusters,
        int qbits,
        int dbits,
        int k
    ) throws IOException {
        int nDocs = corpusLength;
        int nDocClusters = docCentroids.length;
        if (nDocClusters == 0 || nDocs == 0) {
            return 1.0;
        }

        int effectiveQueryClusters = Math.min(nQueryClusters, nDocClusters);
        float[][] queryCentroids;
        int[] docCentroidAssignments;
        if (effectiveQueryClusters <= 1) {
            queryCentroids = new float[][] { docCentroids[0].clone() };
            docCentroidAssignments = new int[nDocClusters];
        } else {
            int targetSize = Math.max(1, nDocClusters / effectiveQueryClusters);
            KMeansFloatVectorValues centroidVectors = KMeansFloatVectorValues.build(Arrays.asList(docCentroids), null, dim);
            KMeansResult queryClustering = HierarchicalKMeans.ofSerial(dim).cluster(centroidVectors, targetSize);
            queryCentroids = queryClustering.centroids();
            docCentroidAssignments = queryClustering.assignments();
        }
        int actualQueryClusters = queryCentroids.length;

        double[] centroidDotCentroid = new double[nDocClusters];
        for (int i = 0; i < nDocClusters; i++) {
            centroidDotCentroid[i] = CalibrationUtils.dot(dim, queryCentroids[docCentroidAssignments[i]], docCentroids[i]);
        }

        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(VectorSimilarityFunction.EUCLIDEAN);
        float[] residualScratch = new float[dim];
        float[] normScratch = cosine ? new float[dim] : null;

        float[] docLower = new float[nDocs];
        float[] docUpper = new float[nDocs];
        int[] docL1 = new int[nDocs];
        int[][] docQuantized = new int[nDocs][dim];
        double[] corpusDotCentroid = new double[nDocs];
        double[] docDotDoc = sim == VectorSimilarityFunction.EUCLIDEAN ? new double[nDocs] : null;

        for (int i = 0; i < nDocs; i++) {
            float[] doc = fvv.vectorValue(corpusOrdinals[i]);
            if (cosine) {
                doc = CalibrationUtils.copyAndNormalize(doc, normScratch);
            }
            int qc = docCentroidAssignments[docAssignments[i]];
            corpusDotCentroid[i] = CalibrationUtils.dot(dim, queryCentroids[qc], doc);
            var qr = quantizer.scalarQuantize(doc, residualScratch, docQuantized[i], (byte) dbits, docCentroids[docAssignments[i]]);
            docLower[i] = qr.lowerInterval();
            docUpper[i] = qr.upperInterval();
            docL1[i] = qr.quantizedComponentSum();
            if (docDotDoc != null) {
                docDotDoc[i] = CalibrationUtils.dot(dim, doc, doc);
            }
        }

        OnlineMeanAndVariance moments = new OnlineMeanAndVariance();
        double dScale = 1.0 / ((1 << dbits) - 1);
        double qScale = 1.0 / ((1 << qbits) - 1);

        float[] queryLower = new float[actualQueryClusters];
        float[] queryUpper = new float[actualQueryClusters];
        int[] queryL1 = new int[actualQueryClusters];
        int[][] queryQuantized = new int[actualQueryClusters][dim];

        float[] queryScratch = new float[dim];
        for (int qi = 0; qi < queries.size(); qi++) {
            queries.copyQuery(qi, usePreconditioned, queryScratch);
            for (int qc = 0; qc < actualQueryClusters; qc++) {
                var qr = quantizer.scalarQuantize(queryScratch, residualScratch, queryQuantized[qc], (byte) qbits, queryCentroids[qc]);
                queryLower[qc] = qr.lowerInterval();
                queryUpper[qc] = qr.upperInterval();
                queryL1[qc] = qr.quantizedComponentSum();
            }

            double[] queryDotCentroid = new double[nDocClusters];
            for (int i = 0; i < nDocClusters; i++) {
                queryDotCentroid[i] = CalibrationUtils.dot(dim, queryScratch, docCentroids[i]);
            }

            double[] simOsq = new double[nDocs];
            for (int i = 0; i < nDocs; i++) {
                int dc = docAssignments[i];
                int qc = docCentroidAssignments[dc];

                double ad = docLower[i];
                double ld = dScale * (docUpper[i] - docLower[i]);
                double aq = queryLower[qc];
                double lq = qScale * (queryUpper[qc] - queryLower[qc]);

                long intDot = dotInt(dim, docQuantized[i], 0, queryQuantized[qc], 0);
                double dotEst = ad * aq * dim + aq * ld * docL1[i] + ad * lq * queryL1[qc] + ld * lq * intDot;

                dotEst += corpusDotCentroid[i] + queryDotCentroid[dc] - centroidDotCentroid[dc];

                if (sim == VectorSimilarityFunction.EUCLIDEAN) {
                    dotEst = 2.0 * dotEst - docDotDoc[i];
                }
                simOsq[i] = dotEst;
            }

            int[] order = new int[nDocs];
            mergeSortIndicesByKeysDescending(simOsq, order, nDocs);

            int topN = Math.min(5 * k, nDocs);
            for (int i = 0; i < topN; i++) {
                int docIdx = order[i];
                float[] doc = fvv.vectorValue(corpusOrdinals[docIdx]);
                if (cosine) {
                    doc = CalibrationUtils.copyAndNormalize(doc, normScratch);
                }
                double exact = simExact(sim, dim, queryScratch, doc);
                moments.add(exact - simOsq[docIdx]);
            }
        }

        return Math.sqrt(3.0 * moments.var());
    }

    /**
     * Clusters the corpus prefix and measures quantized representation error standard deviation.
     */
    static QuantizedErrorComputeResult quantizedRepErrorStdWithCentroids(
        VectorSimilarityFunction sim,
        int dim,
        CalibrationQueries queries,
        boolean usePreconditioned,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int corpusLength,
        boolean cosine,
        int nQueryClusters,
        int nDocsPerCluster,
        int qbits,
        int dbits,
        int k,
        float[][] warmStartCentroids
    ) throws IOException {
        KMeansFloatVectorValues corpusVectors = KMeansFloatVectorValues.wrap(fvv, corpusOrdinals, corpusLength);
        int expectedClusters = HierarchicalKMeans.numClustersForTargetSize(corpusLength, nDocsPerCluster);
        float[][] initialCentroids = warmStartCentroids != null && warmStartCentroids.length == expectedClusters
            ? warmStartCentroids
            : null;
        KMeansResult docClusters = calibrationKMeans(dim).cluster(corpusVectors, nDocsPerCluster, initialCentroids);

        float[][] centroids = docClusters.centroids();
        int[] flatAssignments = docClusters.assignments();
        if (centroids.length == 0) {
            return new QuantizedErrorComputeResult(1.0, centroids);
        }

        double qStd = quantizedRepErrorStd(
            sim,
            dim,
            queries,
            usePreconditioned,
            fvv,
            corpusOrdinals,
            corpusLength,
            cosine,
            flatAssignments,
            centroids,
            nQueryClusters,
            qbits,
            dbits,
            k
        );

        return new QuantizedErrorComputeResult(qStd, centroids);
    }

    record QuantizedErrorComputeResult(double std, float[][] centroids) {}

    /**
     * Estimate the scaling of quantization error by sweeping sample sizes at fixed
     * {@code nDocsPerCluster}, fitting {@code log(error_std) ~ beta0 + beta1 * (log(L) - log(N))}.
     */
    public static QuantizationErrorStdModel estimateQuantizationErrorStdModel(
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        boolean cosine,
        int k,
        int nDocsPerCluster
    ) {
        logger.debug("Fitting error scaling model");
        long scalingStartNanos = System.nanoTime();

        double logNDocsPerCluster = Math.log(nDocsPerCluster);
        Regression.OLSAccumulator state = new Regression.OLSAccumulator();
        float[][] warmStartCentroids = null;
        int corpusLength = 0;

        for (int i = 0; i < SAMPLE_SIZES_SCALING.length; i++) {
            int sampleSize = SAMPLE_SIZES_SCALING[i];
            if (sampleSize > corpusOrdinals.length) {
                break;
            }
            corpusLength = sampleSize;
            try {
                QuantizedErrorComputeResult computed = quantizedRepErrorStdWithCentroids(
                    similarityFunction,
                    dim,
                    queries,
                    true,
                    fvv,
                    corpusOrdinals,
                    corpusLength,
                    cosine,
                    N_QUERY_CLUSTERS,
                    nDocsPerCluster,
                    SCALING_QBITS,
                    SCALING_DBITS,
                    k,
                    warmStartCentroids
                );
                warmStartCentroids = computed.centroids();
                double x = logNDocsPerCluster - Math.log(sampleSize);
                double y = Math.log(Math.max(computed.std(), 1e-38));
                state.update(new double[] { x }, new double[] { y });
                if ((i + 1) % 4 == 0) {
                    logger.debug("Processed {}/{} scaling samples", i + 1, SAMPLE_SIZES_SCALING.length);
                }
            } catch (IOException e) {
                logger.warn("failed to compute quantization error std for sample size [{}]", sampleSize, e);
            }
        }

        Regression.OLSResult params = state.fit();
        if (params == Regression.OLSResult.ZERO) {
            return new QuantizationErrorStdModel(Regression.OLSResult.ZERO);
        }

        double scalingSeconds = (System.nanoTime() - scalingStartNanos) / 1_000_000_000.0;
        logger.debug(
            "Fit error scaling model in {}s\nquantization error ∝ (L/N)^{} (R² = {})",
            String.format(Locale.ROOT, "%.5f", scalingSeconds),
            String.format(Locale.ROOT, "%.4f", params.beta1()),
            String.format(Locale.ROOT, "%.4f", state.r2(params))
        );

        return new QuantizationErrorStdModel(params);
    }

    /**
     * Estimate the magnitude of quantization error for a specific (qbits, dbits) pair.
     * Sweeps sample sizes at fixed {@code nDocsPerCluster} and fits a plug-in regression
     * that reuses the slope from the scaling model.
     */
    public static QuantizationErrorStdModel estimateQuantizationErrorStdMagnitudeParameter(
        QuantizationErrorStdModel scalingModel,
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        boolean usePreconditionedQueries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        boolean cosine,
        int k,
        int qbits,
        int dbits,
        int nDocsPerCluster
    ) {
        long magnitudeStartNanos = System.nanoTime();

        double logNDocsPerCluster = Math.log(nDocsPerCluster);
        Regression.OLSAccumulator state = new Regression.OLSAccumulator();
        float[][] warmStartCentroids = null;

        for (int i = 0; i < SAMPLE_SIZES_MAGNITUDE.length; i++) {
            int sampleSize = SAMPLE_SIZES_MAGNITUDE[i];
            if (sampleSize > corpusOrdinals.length) {
                break;
            }
            try {
                QuantizedErrorComputeResult computed = quantizedRepErrorStdWithCentroids(
                    similarityFunction,
                    dim,
                    queries,
                    usePreconditionedQueries,
                    fvv,
                    corpusOrdinals,
                    sampleSize,
                    cosine,
                    N_QUERY_CLUSTERS,
                    nDocsPerCluster,
                    qbits,
                    dbits,
                    k,
                    warmStartCentroids
                );
                warmStartCentroids = computed.centroids();
                double x = logNDocsPerCluster - Math.log(sampleSize);
                double y = Math.log(Math.max(computed.std(), 1e-38));
                state.update(new double[] { x }, new double[] { y });
            } catch (IOException e) {
                logger.warn("failed to compute quantization error std for magnitude sample size [{}]", sampleSize, e);
            }
        }

        Regression.OLSResult params = state.fitPlugin(scalingModel.params());
        if (params == Regression.OLSResult.ZERO) {
            return scalingModel;
        }

        double magnitudeSeconds = (System.nanoTime() - magnitudeStartNanos) / 1_000_000_000.0;
        logger.debug(
            "Fit error magnitude model in {}s\nquantization error {} (L/N)^{} (R² = {})",
            String.format(Locale.ROOT, "%.2f", magnitudeSeconds),
            String.format(Locale.ROOT, "%.4f", Math.exp(params.beta0())),
            String.format(Locale.ROOT, "%.4f", params.beta1()),
            String.format(Locale.ROOT, "%.4f", state.r2(params))
        );

        return new QuantizationErrorStdModel(params);
    }

    /**
     * Sorts {@code idx[0..len)} into a permutation of {@code 0..len-1} such that
     * {@code keys[idx[i]]} is non-increasing (descending).
     */
    private static void mergeSortIndicesByKeysDescending(double[] keys, int[] idx, int len) {
        if (len < 2) {
            if (len == 1) {
                idx[0] = 0;
            }
            return;
        }
        for (int i = 0; i < len; i++) {
            idx[i] = i;
        }
        int[] aux = new int[len];
        mergeSortDescending(keys, idx, aux, 0, len - 1);
    }

    private static void mergeSortDescending(double[] keys, int[] idx, int[] aux, int lo, int hi) {
        if (lo >= hi) {
            return;
        }
        int mid = (lo + hi) >>> 1;
        mergeSortDescending(keys, idx, aux, lo, mid);
        mergeSortDescending(keys, idx, aux, mid + 1, hi);
        mergeDescending(keys, idx, aux, lo, mid, hi);
    }

    private static void mergeDescending(double[] keys, int[] idx, int[] aux, int lo, int mid, int hi) {
        System.arraycopy(idx, lo, aux, lo, hi - lo + 1);
        int i = lo;
        int j = mid + 1;
        int k = lo;
        while (i <= mid && j <= hi) {
            if (keys[aux[i]] >= keys[aux[j]]) {
                idx[k++] = aux[i++];
            } else {
                idx[k++] = aux[j++];
            }
        }
        while (i <= mid) {
            idx[k++] = aux[i++];
        }
        while (j <= hi) {
            idx[k++] = aux[j++];
        }
    }
}
