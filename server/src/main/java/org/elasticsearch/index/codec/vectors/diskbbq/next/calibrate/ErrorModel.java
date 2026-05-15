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
 * Error model for representation (quantization) error in scalar quantization.
 * Estimates the standard deviation of the error in distance/similarity after quantizing
 * queries and documents. Used by calibration to predict recall.
 * <p>
 * Corpus vectors are accessed lazily via {@link FloatVectorValues} and ordinal arrays
 * to avoid materializing a large {@code float[][]}. Queries are provided via
 * {@link CalibrationQueries} (late materialization, ~1024 vectors max).
 * <p>
 * Fits two OLS regressions:
 * <ul>
 *   <li><b>Scaling model</b>: sweeps (nDocsPerCluster, sampleSize) pairs to fit
 *       {@code log(error_std) ~ beta0 + beta1 * (log(L) - log(N))}.</li>
 *   <li><b>Magnitude model</b>: for a specific (qbits, dbits), sweeps cluster sizes at
 *       a fixed sample size, fitting with a plug-in regression that reuses the slope
 *       from the scaling model.</li>
 * </ul>
 */
public final class ErrorModel {

    private static final Logger logger = LogManager.getLogger(ErrorModel.class);

    static final int N_QUERY_CLUSTERS = 32;

    static final int[] N_DOCS_PER_CLUSTER_SCALING = { 256, 240, 224, 216, 200, 184, 176, 160, 144, 136, 120, 104, 96, 80, 64 };
    static final int[] SAMPLE_SIZES_SCALING = {
        8192,
        8872,
        9552,
        9892,
        10572,
        11252,
        11592,
        12272,
        12952,
        13292,
        13972,
        14652,
        14992,
        15672,
        16352 };

    static final int[] N_DOCS_PER_CLUSTER_SCALING_FAST = { 256, 216, 176, 136, 104, 80, 64 };
    static final int[] SAMPLE_SIZES_SCALING_FAST = { 8192, 9892, 11592, 13292, 14652, 15672, 16352 };

    static final int[] N_DOCS_PER_CLUSTER_MAGNITUDE = { 64, 72, 80, 88, 96, 104, 112, 120, 128 };
    static final int[] N_DOCS_PER_CLUSTER_MAGNITUDE_FAST = { 64, 80, 96, 112, 128 };
    static final int SAMPLE_SIZE_MAGNITUDE = 4096;

    private ErrorModel() {}

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
     * Centroid representation error standard deviation. For each query, finds the top 5%
     * of clusters by similarity and measures the error between the exact similarity to
     * each document and the similarity to its assigned centroid.
     *
     * @param fvv the vector values source for lazy access
     * @param corpusOrdinals ordinal indices into {@code fvv}
     * @param cosine if true, normalize corpus vectors on-the-fly
     */
    static double centroidRepErrorStd(
        VectorSimilarityFunction sim,
        int dim,
        CalibrationQueries queries,
        boolean usePreconditioned,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        boolean cosine,
        int[][] perClusterAssignments,
        float[][] centroids
    ) throws IOException {
        int k = perClusterAssignments.length;
        int visit = Math.max(1, (5 * k + 99) / 100);
        OnlineMeanAndVariance moments = new OnlineMeanAndVariance();

        int[] order = new int[k];
        double[] simToCentroid = new double[k];
        float[] scratch = cosine ? new float[dim] : null;
        float[] queryScratch = new float[dim];

        for (int qi = 0; qi < queries.size(); qi++) {
            queries.copyQuery(qi, usePreconditioned, queryScratch);
            for (int i = 0; i < k; i++) {
                simToCentroid[i] = simExact(sim, dim, queryScratch, centroids[i]);
            }
            mergeSortIndicesByKeysDescending(simToCentroid, order, k);
            for (int idx = 0; idx < Math.min(visit, k); idx++) {
                int ci = order[idx];
                float[] cent = centroids[ci];
                for (int j : perClusterAssignments[ci]) {
                    float[] doc = fvv.vectorValue(corpusOrdinals[j]);
                    if (cosine) {
                        doc = CalibrationUtils.copyAndNormalize(doc, scratch);
                    }
                    double err = simExact(sim, dim, queryScratch, doc) - simExact(sim, dim, queryScratch, cent);
                    moments.add(err);
                }
            }
        }
        return Math.sqrt(moments.var());
    }

    /**
     * Quantized representation error standard deviation. Quantizes doc residuals and
     * query residuals using OSQ, estimates dot products, and compares to exact
     * similarities for the top-5k ranked documents per query.
     *
     * @param fvv the vector values source for lazy access
     * @param corpusOrdinals ordinal indices into {@code fvv}
     * @param cosine if true, normalize corpus vectors on-the-fly
     */
    static double quantizedRepErrorStd(
        VectorSimilarityFunction sim,
        int dim,
        CalibrationQueries queries,
        boolean usePreconditioned,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        boolean cosine,
        int[] docAssignments,
        float[][] docCentroids,
        int nQueryClusters,
        int nDocsPerCluster,
        int qbits,
        int dbits,
        int k
    ) throws IOException {
        int nDocs = docAssignments.length;
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
     * Clusters the corpus and measures both centroid and quantized representation error
     * standard deviations for the given configuration.
     *
     * @param fvv the vector values source for lazy access
     * @param corpusOrdinals ordinal indices into {@code fvv}
     * @param cosine if true, normalize corpus vectors on-the-fly
     * @return {@code double[]{centroidStd, quantizedStd}}
     */
    /**
     * @param corpusLength number of corpus vectors to use (prefix of {@code corpusOrdinals}).
     */
    static double[] repErrorStds(
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
        int k
    ) throws IOException {
        KMeansFloatVectorValues corpusVectors = KMeansFloatVectorValues.wrap(fvv, corpusOrdinals, corpusLength);
        KMeansResult docClusters = HierarchicalKMeans.ofSerial(dim).cluster(corpusVectors, nDocsPerCluster);

        float[][] centroids = docClusters.centroids();
        int[] flatAssignments = docClusters.assignments();
        if (centroids.length == 0) {
            return new double[] { 1.0, 1.0 };
        }

        int nClusters = centroids.length;
        int[] clusterSizes = new int[nClusters];
        for (int flatAssignment : flatAssignments) {
            clusterSizes[flatAssignment]++;
        }
        int[][] perClusterAssignments = new int[nClusters][];
        for (int i = 0; i < nClusters; i++) {
            perClusterAssignments[i] = new int[clusterSizes[i]];
        }
        int[] clusterOffsets = new int[nClusters];
        for (int i = 0; i < flatAssignments.length; i++) {
            int cluster = flatAssignments[i];
            perClusterAssignments[cluster][clusterOffsets[cluster]++] = i;
        }

        double cStd = centroidRepErrorStd(
            sim,
            dim,
            queries,
            usePreconditioned,
            fvv,
            corpusOrdinals,
            cosine,
            perClusterAssignments,
            centroids
        );
        double qStd = quantizedRepErrorStd(
            sim,
            dim,
            queries,
            usePreconditioned,
            fvv,
            corpusOrdinals,
            cosine,
            flatAssignments,
            centroids,
            nQueryClusters,
            nDocsPerCluster,
            qbits,
            dbits,
            k
        );

        return new double[] { cStd, qStd };
    }

    /**
     * Plug-in regression that reuses the slope from the scaling model and only fits the
     * intercept. Used by the magnitude model to avoid overfitting with few data points.
     */
    static Regression.OLSResult fitRepErrorStdPlugin(
        RepErrorStdModel scalingModel,
        double[] logNDocsPerCluster,
        double[] logSampleSizes,
        double[] logErrorStd
    ) {
        int m = logErrorStd.length;
        double beta1 = scalingModel.qparams().beta1();
        double var1 = scalingModel.qparams().var1();

        double sumRes = 0.0;
        double sumX = 0.0;
        for (int i = 0; i < m; i++) {
            double x = logNDocsPerCluster[i] - logSampleSizes[i];
            sumRes += logErrorStd[i] - beta1 * x;
            sumX += x;
        }
        double beta0 = sumRes / m;
        double xBar = sumX / m;

        double rss = 0.0;
        for (int i = 0; i < m; i++) {
            double x = logNDocsPerCluster[i] - logSampleSizes[i];
            double err = logErrorStd[i] - (beta0 + beta1 * x);
            rss += err * err;
        }
        double sigmaSq = m > 1 ? rss / (m - 1) : 0.0;

        return new Regression.OLSResult(beta0, beta1, (sigmaSq / m) + (xBar * xBar * var1), var1, -xBar * var1, sigmaSq);
    }

    /**
     * Estimate the scaling of representation error using default (full) sweep parameters.
     */
    public static RepErrorStdModel estimateRepErrorStdScalingParameter(
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        boolean cosine,
        int k
    ) {
        return estimateRepErrorStdScalingParameter(
            similarityFunction,
            dim,
            queries,
            fvv,
            corpusOrdinals,
            cosine,
            k,
            N_DOCS_PER_CLUSTER_SCALING,
            SAMPLE_SIZES_SCALING
        );
    }

    /**
     * Estimate the scaling of representation error using reduced sweep parameters for faster execution.
     */
    public static RepErrorStdModel estimateRepErrorStdScalingParameterFast(
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        boolean cosine,
        int k
    ) {
        return estimateRepErrorStdScalingParameter(
            similarityFunction,
            dim,
            queries,
            fvv,
            corpusOrdinals,
            cosine,
            k,
            N_DOCS_PER_CLUSTER_SCALING_FAST,
            SAMPLE_SIZES_SCALING_FAST
        );
    }

    /**
     * Estimate the scaling of representation error by sweeping (nDocsPerCluster, sampleSize) pairs,
     * clustering the corpus at each, measuring centroid and quantized error, and fitting OLS on
     * {@code log(error_std) ~ beta0 + beta1 * (log(L) - log(N))}.
     */
    static RepErrorStdModel estimateRepErrorStdScalingParameter(
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        boolean cosine,
        int k,
        int[] nDocsPerClusterArray,
        int[] sampleSizesArray
    ) {
        int m = nDocsPerClusterArray.length;
        int nDocsTotal = corpusOrdinals.length;

        logger.info("Fitting error scaling models");
        long scalingStartNanos = System.nanoTime();

        double[] x = new double[m];
        double[] logCStd = new double[m];
        double[] logQStd = new double[m];
        double[] logNDocsRow = new double[m];
        double[] logSampleRow = new double[m];
        int mActual = 0;

        for (int i = 0; i < m; i++) {
            int ss = sampleSizesArray[i];
            if (ss > nDocsTotal) {
                break;
            }
            if (ss < 2) {
                continue;
            }
            try {
                double[] stds = repErrorStds(
                    similarityFunction,
                    dim,
                    queries,
                    true,
                    fvv,
                    corpusOrdinals,
                    ss,
                    cosine,
                    N_QUERY_CLUSTERS,
                    nDocsPerClusterArray[i],
                    4,
                    1,
                    k
                );
                logNDocsRow[mActual] = Math.log(nDocsPerClusterArray[i]);
                logSampleRow[mActual] = Math.log(sampleSizesArray[i]);
                x[mActual] = logNDocsRow[mActual] - logSampleRow[mActual];
                logCStd[mActual] = Math.log(Math.max(stds[0], 1e-38));
                logQStd[mActual] = Math.log(Math.max(stds[1], 1e-38));
                mActual++;
                if (mActual % 5 == 0) {
                    logger.info("Processed {}/{} samples", mActual, m);
                }
            } catch (IOException e) {
                logger.warn("failed to compute rep error stds for sample size [{}]", ss, e);
            }
        }

        if (mActual < 2) {
            return new RepErrorStdModel(Regression.OLSResult.ZERO, Regression.OLSResult.ZERO);
        }
        double[] xFit = Arrays.copyOf(x, mActual);
        double[] logCFit = Arrays.copyOf(logCStd, mActual);
        double[] logQFit = Arrays.copyOf(logQStd, mActual);
        Regression.OLSResult cparams = Regression.fitOls(xFit, logCFit);
        Regression.OLSResult qparams = Regression.fitOls(xFit, logQFit);

        double[] logNDocsPerCluster = Arrays.copyOf(logNDocsRow, mActual);
        double[] logSampleSizes = Arrays.copyOf(logSampleRow, mActual);

        double scalingSeconds = (System.nanoTime() - scalingStartNanos) / 1_000_000_000.0;
        double r2c = repErrorStdR2(logNDocsPerCluster, logSampleSizes, logCFit, cparams);
        double r2q = repErrorStdR2(logNDocsPerCluster, logSampleSizes, logQFit, qparams);
        logger.info(
            "--------------------------------------\nFit error scaling models in {}s\n"
                + "centroid error {} (L/N)^{} (R² = {})\n"
                + "quantization error ∝ (L/N)^{} (R² = {})",
            String.format(Locale.ROOT, "%.5f", scalingSeconds),
            String.format(Locale.ROOT, "%.4f", Math.exp(cparams.beta0())),
            String.format(Locale.ROOT, "%.4f", cparams.beta1()),
            String.format(Locale.ROOT, "%.4f", r2c),
            String.format(Locale.ROOT, "%.4f", qparams.beta1()),
            String.format(Locale.ROOT, "%.4f", r2q)
        );

        return new RepErrorStdModel(cparams, qparams);
    }

    /** R² for log(error) ~ beta0 + beta1 * (log(L) - log(N)). */
    private static double repErrorStdR2(
        double[] logNDocsPerCluster,
        double[] logSampleSizes,
        double[] logErrorStd,
        Regression.OLSResult res
    ) {
        int n = logErrorStd.length;
        double[] x = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = logNDocsPerCluster[i] - logSampleSizes[i];
        }
        return Regression.rSquared(x, logErrorStd, res);
    }

    /**
     * Estimate the magnitude of representation error using default (full) sweep parameters.
     */
    public static RepErrorStdModel estimateRepErrorStdMagnitudeParameter(
        RepErrorStdModel scalingModel,
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        boolean usePreconditionedQueries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        boolean cosine,
        int k,
        int qbits,
        int dbits
    ) {
        return estimateRepErrorStdMagnitudeParameter(
            scalingModel,
            similarityFunction,
            dim,
            queries,
            usePreconditionedQueries,
            fvv,
            corpusOrdinals,
            cosine,
            k,
            qbits,
            dbits,
            N_DOCS_PER_CLUSTER_MAGNITUDE
        );
    }

    /**
     * Estimate the magnitude of representation error using reduced sweep parameters for faster execution.
     */
    public static RepErrorStdModel estimateRepErrorStdMagnitudeParameterFast(
        RepErrorStdModel scalingModel,
        VectorSimilarityFunction similarityFunction,
        int dim,
        CalibrationQueries queries,
        boolean usePreconditionedQueries,
        FloatVectorValues fvv,
        int[] corpusOrdinals,
        boolean cosine,
        int k,
        int qbits,
        int dbits
    ) {
        return estimateRepErrorStdMagnitudeParameter(
            scalingModel,
            similarityFunction,
            dim,
            queries,
            usePreconditionedQueries,
            fvv,
            corpusOrdinals,
            cosine,
            k,
            qbits,
            dbits,
            N_DOCS_PER_CLUSTER_MAGNITUDE_FAST
        );
    }

    /**
     * Estimate the magnitude of representation error for a specific (qbits, dbits) pair.
     * Sweeps cluster sizes at a fixed sample size and fits a plug-in regression that
     * reuses the slope from the scaling model.
     */
    static RepErrorStdModel estimateRepErrorStdMagnitudeParameter(
        RepErrorStdModel scalingModel,
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
        int[] nDocsPerClusterArray
    ) {
        int m = nDocsPerClusterArray.length;
        int sampleSize = Math.min(SAMPLE_SIZE_MAGNITUDE, corpusOrdinals.length);

        long magnitudeStartNanos = System.nanoTime();

        double[] logNDocs = new double[m];
        double[] logSizes = new double[m];
        double[] logQStd = new double[m];
        int mActual = 0;
        for (int i = 0; i < m; i++) {
            try {
                double[] stds = repErrorStds(
                    similarityFunction,
                    dim,
                    queries,
                    usePreconditionedQueries,
                    fvv,
                    corpusOrdinals,
                    sampleSize,
                    cosine,
                    N_QUERY_CLUSTERS,
                    nDocsPerClusterArray[i],
                    qbits,
                    dbits,
                    k
                );
                logNDocs[mActual] = Math.log(nDocsPerClusterArray[i]);
                logSizes[mActual] = Math.log(sampleSize);
                logQStd[mActual] = Math.log(Math.max(stds[1], 1e-38));
                mActual++;
            } catch (IOException e) {
                logger.warn("failed to compute rep error stds for magnitude iteration [{}]", i, e);
            }
        }

        if (mActual < 2) {
            return scalingModel;
        }
        double[] logNDocsTrim = Arrays.copyOf(logNDocs, mActual);
        double[] logSizesTrim = Arrays.copyOf(logSizes, mActual);
        double[] logQTrim = Arrays.copyOf(logQStd, mActual);
        Regression.OLSResult qparams = fitRepErrorStdPlugin(scalingModel, logNDocsTrim, logSizesTrim, logQTrim);

        double magnitudeSeconds = (System.nanoTime() - magnitudeStartNanos) / 1_000_000_000.0;
        double r2Mag = repErrorStdR2(logNDocsTrim, logSizesTrim, logQTrim, qparams);
        logger.info(
            "--------------------------------------\nFit error magnitude models in {}s\nquantization error {} (L/N)^{} (R² = {})",
            String.format(Locale.ROOT, "%.2f", magnitudeSeconds),
            String.format(Locale.ROOT, "%.4f", Math.exp(qparams.beta0())),
            String.format(Locale.ROOT, "%.4f", qparams.beta1()),
            String.format(Locale.ROOT, "%.4f", r2Mag)
        );

        return new RepErrorStdModel(scalingModel.cparams(), qparams);
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
