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
import org.elasticsearch.core.WelfordVariance;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Error model for error in scalar quantization.
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

    private static HierarchicalKMeans<float[]> calibrationKMeans(int dim) {
        return HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, dim);
    }

    /**
     * Exact similarity between two vectors, consistent with the metric convention
     * where higher values indicate greater similarity.
     */
    static double simExact(VectorSimilarityFunction sim, float[] y, float[] x) {
        if (sim == VectorSimilarityFunction.EUCLIDEAN) {
            return 2.0 * ESVectorUtil.dotProduct(y, x) - ESVectorUtil.dotProduct(x, x);
        }
        return ESVectorUtil.dotProduct(y, x);
    }

    private static void packQuantizedAsBytes(int[] quantized, byte[] destination, int dim) {
        for (int d = 0; d < dim; d++) {
            destination[d] = (byte) quantized[d];
        }
    }

    /**
     * Quantized representation error standard deviation. Quantizes doc residuals and
     * query residuals using OSQ, estimates dot products, and compares to exact
     * similarities for the top-5k ranked documents per query.
     */
    static double quantizedRepErrorStd(
        VectorSimilarityFunction sim,
        int dim,
        FloatVectorValues querySource,
        int[] queryOrdinals,
        int baseDim,
        boolean cosine,
        boolean neyshabur,
        Preconditioner preconditioner,
        boolean usePreconditioned,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int corpusLength,
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
            KMeansResult<float[]> queryClustering = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, dim)
                .cluster(centroidVectors, targetSize);
            queryCentroids = queryClustering.centroids();
            docCentroidAssignments = queryClustering.assignments();
        }
        int actualQueryClusters = queryCentroids.length;

        double[] centroidDotCentroid = new double[nDocClusters];
        for (int i = 0; i < nDocClusters; i++) {
            centroidDotCentroid[i] = ESVectorUtil.dotProduct(queryCentroids[docCentroidAssignments[i]], docCentroids[i]);
        }

        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(VectorSimilarityFunction.EUCLIDEAN);
        float[] residualScratch = new float[dim];
        float[] normScratch = cosine ? new float[dim] : null;
        int[] quantizeScratch = new int[dim];

        float[] docLower = new float[nDocs];
        float[] docUpper = new float[nDocs];
        int[] docL1 = new int[nDocs];
        byte[][] docQuantized = new byte[nDocs][dim];
        double[] corpusDotCentroid = new double[nDocs];
        double[] docDotDoc = sim == VectorSimilarityFunction.EUCLIDEAN ? new double[nDocs] : null;

        for (int i = 0; i < nDocs; i++) {
            float[] doc = fvv.vectorValue(corpusOrdinals[i]);
            if (cosine) {
                doc = CalibrationUtils.copyAndNormalize(doc, normScratch);
            }
            int qc = docCentroidAssignments[docAssignments[i]];
            corpusDotCentroid[i] = ESVectorUtil.dotProduct(queryCentroids[qc], doc);
            var qr = quantizer.scalarQuantize(doc, residualScratch, quantizeScratch, (byte) dbits, docCentroids[docAssignments[i]]);
            packQuantizedAsBytes(quantizeScratch, docQuantized[i], dim);
            docLower[i] = qr.lowerInterval();
            docUpper[i] = qr.upperInterval();
            docL1[i] = qr.quantizedComponentSum();
            if (docDotDoc != null) {
                docDotDoc[i] = ESVectorUtil.dotProduct(doc, doc);
            }
        }

        WelfordVariance moments = new WelfordVariance();
        double dScale = 1.0 / ((1 << dbits) - 1);
        double qScale = 1.0 / ((1 << qbits) - 1);

        float[] queryLower = new float[actualQueryClusters];
        float[] queryUpper = new float[actualQueryClusters];
        int[] queryL1 = new int[actualQueryClusters];
        byte[][] queryQuantized = new byte[actualQueryClusters][dim];

        float[] queryScratch = new float[dim];
        float[] preconditionScratch = preconditioner != null ? new float[dim] : null;
        for (int qi = 0; qi < queryOrdinals.length; qi++) {
            CalibrationUtils.materializeCalibrationQuery(
                querySource,
                queryOrdinals[qi],
                baseDim,
                dim,
                cosine,
                neyshabur,
                preconditioner,
                usePreconditioned,
                queryScratch,
                preconditionScratch
            );
            for (int qc = 0; qc < actualQueryClusters; qc++) {
                var qr = quantizer.scalarQuantize(queryScratch, residualScratch, quantizeScratch, (byte) qbits, queryCentroids[qc]);
                packQuantizedAsBytes(quantizeScratch, queryQuantized[qc], dim);
                queryLower[qc] = qr.lowerInterval();
                queryUpper[qc] = qr.upperInterval();
                queryL1[qc] = qr.quantizedComponentSum();
            }

            double[] queryDotCentroid = new double[nDocClusters];
            for (int i = 0; i < nDocClusters; i++) {
                queryDotCentroid[i] = ESVectorUtil.dotProduct(queryScratch, docCentroids[i]);
            }

            double[] simOsq = new double[nDocs];
            for (int i = 0; i < nDocs; i++) {
                int dc = docAssignments[i];
                int qc = docCentroidAssignments[dc];

                double ad = docLower[i];
                double ld = dScale * (docUpper[i] - docLower[i]);
                double aq = queryLower[qc];
                double lq = qScale * (queryUpper[qc] - queryLower[qc]);

                long intDot = (long) ESVectorUtil.dotProduct(docQuantized[i], queryQuantized[qc]);
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
                double exact = simExact(sim, queryScratch, doc);
                moments.add(exact - simOsq[docIdx]);
            }
        }

        return Math.sqrt(3.0 * moments.sampleVariance());
    }

    /**
     * Clusters the corpus prefix and measures quantized representation error standard deviation.
     */
    static QuantizedErrorComputeResult quantizedRepErrorStdWithCentroids(
        VectorSimilarityFunction sim,
        int dim,
        FloatVectorValues querySource,
        int[] queryOrdinals,
        int baseDim,
        boolean cosine,
        boolean neyshabur,
        Preconditioner preconditioner,
        boolean usePreconditioned,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        int corpusLength,
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
        KMeansResult<float[]> docClusters = calibrationKMeans(dim).cluster(corpusVectors, nDocsPerCluster, initialCentroids);

        float[][] centroids = docClusters.centroids();
        int[] flatAssignments = docClusters.assignments();
        if (centroids.length == 0) {
            return new QuantizedErrorComputeResult(1.0, centroids);
        }

        double qStd = quantizedRepErrorStd(
            sim,
            dim,
            querySource,
            queryOrdinals,
            baseDim,
            cosine,
            neyshabur,
            preconditioner,
            usePreconditioned,
            fvv,
            corpusOrdinals,
            corpusLength,
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
        FloatVectorValues querySource,
        int[] queryOrdinals,
        int baseDim,
        boolean cosine,
        boolean neyshabur,
        Preconditioner preconditioner,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
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
                    querySource,
                    queryOrdinals,
                    baseDim,
                    cosine,
                    neyshabur,
                    preconditioner,
                    true,
                    fvv,
                    corpusOrdinals,
                    corpusLength,
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
        FloatVectorValues querySource,
        int[] queryOrdinals,
        int baseDim,
        boolean cosine,
        boolean neyshabur,
        Preconditioner preconditioner,
        boolean usePreconditionedQueries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
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
                    querySource,
                    queryOrdinals,
                    baseDim,
                    cosine,
                    neyshabur,
                    preconditioner,
                    usePreconditionedQueries,
                    fvv,
                    corpusOrdinals,
                    sampleSize,
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
