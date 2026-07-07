/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.core.WelfordVariance;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansWithOverspill;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.elasticsearch.core.Strings.format;

/**
 * Error model for error in scalar quantization.
 * Estimates the standard deviation of the error in distance/similarity after quantizing
 * queries and documents. Used by calibration to predict recall.
 * Fitting is two-stage: {@link #estimateErrorScalingFit} sweeps sample sizes at fixed
 * 4q/1d encoding to fit the slope of {@code log(error_std)} vs {@code log(L/N)}; then
 * {@link #estimateMagnitudeModel} sweeps at a target (qbits, dbits) pair and fits magnitude
 * while reusing that slope.
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

    /**
     * Quantized representation error standard deviation. Quantizes doc residuals and
     * query residuals using OSQ, estimates dot products, and compares to exact
     * similarities for the top-5k ranked documents per query.
     */
    static QuantizedQueryErrorResult quantizedRepErrorStd(
        CalibrationSource source,
        boolean usePreconditioned,
        int nDocs,
        int[] docAssignments,
        float[][] docCentroids,
        int nQueryClusters,
        int qbits,
        int dbits,
        HierarchicalKMeans<float[]> kmeans,
        float[][] warmStartQueryCentroids,
        QuantizedErrorScratch scratch
    ) throws IOException {
        VectorSimilarityFunction sim = source.similarityFunction();
        int dim = source.dim();
        boolean cosine = source.cosine();

        int nDocClusters = docCentroids.length;
        if (nDocClusters == 0 || nDocs == 0) {
            return new QuantizedQueryErrorResult(1.0, docCentroids.length > 0 ? new float[][] { docCentroids[0].clone() } : new float[0][]);
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
            KMeansWithOverspill<float[]> queryClustering = kmeans.cluster(centroidVectors, targetSize, warmStartQueryCentroids);
            queryCentroids = queryClustering.centroids();
            docCentroidAssignments = queryClustering.assignments();
        }
        int actualQueryClusters = queryCentroids.length;

        double[] centroidDotCentroid = scratch.centroidDotCentroid;
        for (int i = 0; i < nDocClusters; i++) {
            centroidDotCentroid[i] = ESVectorUtil.dotProduct(queryCentroids[docCentroidAssignments[i]], docCentroids[i]);
        }

        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(VectorSimilarityFunction.EUCLIDEAN);
        float[] residualScratch = scratch.residualScratch;
        float[] normScratch = scratch.normScratch;
        int[] quantizeScratch = scratch.quantizeScratch;

        float[] docLower = scratch.docLower;
        float[] docUpper = scratch.docUpper;
        int[] docL1 = scratch.docL1;
        byte[][] docQuantized = scratch.docQuantized;
        double[] corpusDotCentroid = scratch.corpusDotCentroid;
        double[] docDotDoc = scratch.docDotDoc;

        for (int i = 0; i < nDocs; i++) {
            float[] doc = source.vectors().vectorValue(source.corpusOrdinals()[i]);
            if (cosine) {
                doc = CalibrationUtils.copyAndNormalize(doc, normScratch);
            }
            int qc = docCentroidAssignments[docAssignments[i]];
            corpusDotCentroid[i] = ESVectorUtil.dotProduct(queryCentroids[qc], doc);
            var qr = quantizer.scalarQuantize(doc, residualScratch, quantizeScratch, (byte) dbits, docCentroids[docAssignments[i]]);
            ESVectorUtil.packAsBytes(quantizeScratch, docQuantized[i], dim);
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

        float[] queryLower = scratch.queryLower;
        float[] queryUpper = scratch.queryUpper;
        int[] queryL1 = scratch.queryL1;
        byte[][] queryQuantized = scratch.queryQuantized;

        float[] queryScratch = scratch.queryScratch;
        float[] preconditionScratch = scratch.preconditionScratch;

        double[] queryDotCentroid = scratch.queryDotCentroid;
        double[] simOsq = scratch.simOsq;
        int[] order = scratch.order;

        // Group doc indices by their query cluster once (assignments are query-independent), so the
        // packed-byte dot product can be evaluated four docs at a time against the shared quantized
        // query operand for that cluster. bucketedDocs holds doc indices sorted by cluster;
        // bucketStart is the CSR offset array with cluster c occupying [bucketStart[c], bucketStart[c+1]).
        int[] bucketStart = scratch.bucketStart;
        int[] bucketedDocs = scratch.bucketedDocs;
        int[] bucketCursor = scratch.bucketCursor;
        int[] intDots = scratch.intDots;
        float[] bulkDistances = scratch.bulkDistances;
        Arrays.fill(bucketStart, 0, actualQueryClusters + 1, 0);
        for (int i = 0; i < nDocs; i++) {
            bucketStart[docCentroidAssignments[docAssignments[i]] + 1]++;
        }
        for (int c = 0; c < actualQueryClusters; c++) {
            bucketStart[c + 1] += bucketStart[c];
        }
        System.arraycopy(bucketStart, 0, bucketCursor, 0, actualQueryClusters);
        for (int i = 0; i < nDocs; i++) {
            int qc = docCentroidAssignments[docAssignments[i]];
            bucketedDocs[bucketCursor[qc]++] = i;
        }

        for (int queryOrdinal : source.queryOrdinals()) {
            CalibrationUtils.materializeCalibrationQuery(
                source.vectors(),
                queryOrdinal,
                source.baseDim(),
                dim,
                cosine,
                source.neyshabur(),
                source.preconditioner(),
                usePreconditioned,
                queryScratch,
                preconditionScratch
            );
            for (int qc = 0; qc < actualQueryClusters; qc++) {
                var qr = quantizer.scalarQuantize(queryScratch, residualScratch, quantizeScratch, (byte) qbits, queryCentroids[qc]);
                ESVectorUtil.packAsBytes(quantizeScratch, queryQuantized[qc], dim);
                queryLower[qc] = qr.lowerInterval();
                queryUpper[qc] = qr.upperInterval();
                queryL1[qc] = qr.quantizedComponentSum();
            }

            for (int i = 0; i < nDocClusters; i++) {
                queryDotCentroid[i] = ESVectorUtil.dotProduct(queryScratch, docCentroids[i]);
            }

            // Bulk-evaluate the packed-byte doc·query dot products four docs at a time, one query
            // cluster at a time so the shared quantized query operand is reused across the group.
            for (int c = 0; c < actualQueryClusters; c++) {
                byte[] qq = queryQuantized[c];
                int p = bucketStart[c];
                int end = bucketStart[c + 1];
                int bulkLimit = end - 3;
                for (; p < bulkLimit; p += 4) {
                    int d0 = bucketedDocs[p], d1 = bucketedDocs[p + 1], d2 = bucketedDocs[p + 2], d3 = bucketedDocs[p + 3];
                    ESVectorUtil.dotProductBulk(
                        qq,
                        docQuantized[d0],
                        docQuantized[d1],
                        docQuantized[d2],
                        docQuantized[d3],
                        0,
                        bulkDistances
                    );
                    intDots[d0] = Math.round(bulkDistances[0]);
                    intDots[d1] = Math.round(bulkDistances[1]);
                    intDots[d2] = Math.round(bulkDistances[2]);
                    intDots[d3] = Math.round(bulkDistances[3]);
                }
                for (; p < end; p++) {
                    int d = bucketedDocs[p];
                    intDots[d] = Math.round(ESVectorUtil.dotProduct(qq, docQuantized[d]));
                }
            }

            for (int i = 0; i < nDocs; i++) {
                int dc = docAssignments[i];
                int qc = docCentroidAssignments[dc];

                double ad = docLower[i];
                double ld = dScale * (docUpper[i] - docLower[i]);
                double aq = queryLower[qc];
                double lq = qScale * (queryUpper[qc] - queryLower[qc]);

                double dotEst = ad * aq * dim + aq * ld * docL1[i] + ad * lq * queryL1[qc] + ld * lq * intDots[i];

                dotEst += corpusDotCentroid[i] + queryDotCentroid[dc] - centroidDotCentroid[dc];

                if (sim == VectorSimilarityFunction.EUCLIDEAN) {
                    dotEst = 2.0 * dotEst;
                    if (docDotDoc != null) {
                        dotEst -= docDotDoc[i];
                    }
                }
                simOsq[i] = dotEst;
            }

            sortIndicesByKeysDescending(simOsq, order, nDocs);

            int topN = Math.min(5 * source.k(), nDocs);
            for (int i = 0; i < topN; i++) {
                int docIdx = order[i];
                float[] doc = source.vectors().vectorValue(source.corpusOrdinals()[docIdx]);
                if (cosine) {
                    doc = CalibrationUtils.copyAndNormalize(doc, normScratch);
                }
                double exact;
                if (sim == VectorSimilarityFunction.EUCLIDEAN) {
                    assert docDotDoc != null;
                    exact = 2.0 * ESVectorUtil.dotProduct(queryScratch, doc) - docDotDoc[docIdx];
                } else {
                    exact = ESVectorUtil.dotProduct(queryScratch, doc);
                }
                moments.add(exact - simOsq[docIdx]);
            }
        }

        return new QuantizedQueryErrorResult(Math.sqrt(3.0 * moments.sampleVariance()), queryCentroids);
    }

    private record QuantizedQueryErrorResult(double std, float[][] queryCentroids) {}

    /**
     * Clusters the corpus prefix and measures quantized representation error standard deviation.
     */
    static QuantizedErrorComputeResult quantizedRepErrorStdWithCentroids(
        CalibrationSource source,
        boolean usePreconditioned,
        int nDocs,
        int nDocsPerCluster,
        int qbits,
        int dbits,
        HierarchicalKMeans<float[]> kmeans,
        float[][] warmStartDocCentroids,
        float[][] warmStartQueryCentroids,
        QuantizedErrorScratch scratch
    ) throws IOException {
        KMeansFloatVectorValues corpusVectors = KMeansFloatVectorValues.wrap(source.vectors(), source.corpusOrdinals(), nDocs);
        KMeansWithOverspill<float[]> docClusters = kmeans.cluster(corpusVectors, nDocsPerCluster, warmStartDocCentroids);

        float[][] docCentroids = docClusters.centroids();
        int[] flatAssignments = docClusters.assignments();
        if (docCentroids.length == 0) {
            return new QuantizedErrorComputeResult(1.0, docCentroids, warmStartQueryCentroids);
        }

        QuantizedQueryErrorResult queryError = quantizedRepErrorStd(
            source,
            usePreconditioned,
            nDocs,
            flatAssignments,
            docCentroids,
            ErrorModel.N_QUERY_CLUSTERS,
            qbits,
            dbits,
            kmeans,
            warmStartQueryCentroids,
            scratch
        );

        return new QuantizedErrorComputeResult(queryError.std(), docCentroids, queryError.queryCentroids());
    }

    record QuantizedErrorComputeResult(double std, float[][] docCentroids, float[][] queryCentroids) {}

    public static ErrorScalingFit estimateErrorScalingFit(CalibrationSource source, int nDocsPerCluster) {
        return estimateErrorScalingFit(source, nDocsPerCluster, HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, source.dim()));
    }

    private static ErrorScalingFit estimateErrorScalingFit(
        CalibrationSource source,
        int nDocsPerCluster,
        HierarchicalKMeans<float[]> kmeans
    ) {
        logger.debug("Fitting error scaling model");
        long scalingStartNanos = System.nanoTime();

        double logNDocsPerCluster = Math.log(nDocsPerCluster);
        Regression.OLSAccumulator state = new Regression.OLSAccumulator();
        float[][] warmStartDocCentroids = null;
        float[][] warmStartQueryCentroids = null;
        int corpusLength;

        int maxNDocs = Math.min(SAMPLE_SIZES_SCALING[SAMPLE_SIZES_SCALING.length - 1], source.corpusOrdinals().length);
        QuantizedErrorScratch scratch = new QuantizedErrorScratch(
            maxNDocs,
            source.dim(),
            source.cosine(),
            source.similarityFunction() == VectorSimilarityFunction.EUCLIDEAN,
            source.preconditioner() != null
        );
        for (int i = 0; i < SAMPLE_SIZES_SCALING.length; i++) {
            int sampleSize = SAMPLE_SIZES_SCALING[i];
            if (sampleSize > source.corpusOrdinals().length) {
                break;
            }
            corpusLength = sampleSize;
            try {
                QuantizedErrorComputeResult computed = quantizedRepErrorStdWithCentroids(
                    source,
                    true,
                    corpusLength,
                    nDocsPerCluster,
                    SCALING_QBITS,
                    SCALING_DBITS,
                    kmeans,
                    warmStartDocCentroids,
                    warmStartQueryCentroids,
                    scratch
                );
                warmStartDocCentroids = computed.docCentroids();
                warmStartQueryCentroids = computed.queryCentroids();
                double x = logNDocsPerCluster - Math.log(sampleSize);
                double y = Math.log(Math.max(computed.std(), 1e-38));
                state.update(x, y);
                if ((i + 1) % 4 == 0) {
                    logger.debug("Processed {}/{} scaling samples", i + 1, SAMPLE_SIZES_SCALING.length);
                }
            } catch (IOException e) {
                logger.warn("failed to compute quantization error std for sample size [{}]", sampleSize, e);
            }
        }

        Regression.OLSResult params = state.fit();
        if (params == Regression.OLSResult.ZERO) {
            return new ErrorScalingFit(
                new QuantizationErrorStdModel(Regression.OLSResult.ZERO),
                warmStartDocCentroids,
                warmStartQueryCentroids
            );
        }

        double scalingSeconds = (System.nanoTime() - scalingStartNanos) / 1_000_000_000.0;
        logger.debug(
            () -> format(
                "Fit error scaling model in %.5fs\nquantization error ∝ (L/N)^%.4f (R² = %.4f)",
                scalingSeconds,
                params.beta1(),
                state.r2(params)
            )
        );

        return new ErrorScalingFit(new QuantizationErrorStdModel(params), warmStartDocCentroids, warmStartQueryCentroids);
    }

    /**
     * Estimate the magnitude of quantization error for a specific (qbits, dbits) quantization.
     * Sweeps sample sizes at fixed {@code nDocsPerCluster} and fits a plug-in regression
     * that reuses the slope from {@link ErrorScalingFit#scalingModel()}.
     *
     * @param qbits query-side quantization bit width
     * @param dbits document-side quantization bit width
     */
    public static QuantizationErrorStdModel estimateMagnitudeModel(
        ErrorScalingFit scalingFit,
        CalibrationSource source,
        boolean usePreconditionedQueries,
        int qbits,
        int dbits,
        int nDocsPerCluster
    ) {
        return estimateMagnitudeModel(
            scalingFit,
            source,
            usePreconditionedQueries,
            qbits,
            dbits,
            nDocsPerCluster,
            HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, source.dim())
        );
    }

    private static QuantizationErrorStdModel estimateMagnitudeModel(
        ErrorScalingFit scalingFit,
        CalibrationSource source,
        boolean usePreconditionedQueries,
        int qbits,
        int dbits,
        int nDocsPerCluster,
        HierarchicalKMeans<float[]> kmeans
    ) {
        QuantizationErrorStdModel scalingModel = scalingFit.scalingModel();
        long magnitudeStartNanos = System.nanoTime();

        double logNDocsPerCluster = Math.log(nDocsPerCluster);
        Regression.OLSAccumulator state = new Regression.OLSAccumulator();
        float[][] docWarmStart = scalingFit.lastDocCentroids;
        float[][] queryWarmStart = scalingFit.lastQueryCentroids;

        int maxNDocs = Math.min(SAMPLE_SIZES_MAGNITUDE[SAMPLE_SIZES_MAGNITUDE.length - 1], source.corpusOrdinals().length);
        QuantizedErrorScratch scratch = new QuantizedErrorScratch(
            maxNDocs,
            source.dim(),
            source.cosine(),
            source.similarityFunction() == VectorSimilarityFunction.EUCLIDEAN,
            source.preconditioner() != null
        );

        for (int sampleSize : SAMPLE_SIZES_MAGNITUDE) {
            if (sampleSize > source.corpusOrdinals().length) {
                break;
            }
            try {
                QuantizedErrorComputeResult computed = quantizedRepErrorStdWithCentroids(
                    source,
                    usePreconditionedQueries,
                    sampleSize,
                    nDocsPerCluster,
                    qbits,
                    dbits,
                    kmeans,
                    docWarmStart,
                    queryWarmStart,
                    scratch
                );
                docWarmStart = computed.docCentroids();
                queryWarmStart = computed.queryCentroids();
                double x = logNDocsPerCluster - Math.log(sampleSize);
                double y = Math.log(Math.max(computed.std(), 1e-38));
                state.update(x, y);
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
            () -> format(
                "Fit error magnitude model in %.2fs\nquantization error %.4f (L/N)^%.4f (R² = %.4f)",
                magnitudeSeconds,
                Math.exp(params.beta0()),
                params.beta1(),
                state.r2(params)
            )
        );

        return new QuantizationErrorStdModel(params);
    }

    /**
     * Estimates quantization error std for {@code (qbits, dbits)} using synthetic Gaussian
     * residuals derived from the manifold model, requiring no k-means, no NN assignment.
     * <p>
     * The expected cluster radius R is computed analytically as
     * {@code |expectedRankDistance(sim, alpha, invDim, numVectors, nDocsPerCluster/2)|}; then
     * {@code nDocs} independent Gaussian residuals with per-dimension std {@code R/sqrt(dim)}
     * are generated and quantized with OSQ against a zero centroid. Actual corpus vectors are
     * used as queries (materialized via {@link CalibrationUtils#materializeCalibrationQuery})
     * and also quantized against the zero centroid. The slope {@code beta1 = invDim} is taken
     * directly from the manifold model; only the intercept {@code beta0} is fit from the
     * single-point OSQ error measurement.
     * <p>
     * Cost: {@code O(nDocs × dim)} for residual generation + quantization plus the usual
     * {@code O(nQueries × nDocs)} for the error measurement, avoid k-means pass.
     */
    public static QuantizationErrorStdModel estimateMagnitudeFromManifoldResiduals(
        double alpha,
        double invDim,
        CalibrationSource source,
        boolean usePreconditionedQueries,
        int qbits,
        int dbits,
        int nDocsPerCluster,
        int numVectors
    ) throws IOException {
        VectorSimilarityFunction sim = source.similarityFunction();
        int dim = source.dim();
        boolean euclidean = sim == VectorSimilarityFunction.EUCLIDEAN;
        int nSamples = source.corpusOrdinals().length;

        int halfCluster = Math.max(1, nDocsPerCluster / 2);
        double R = Math.abs(ManifoldModel.expectedRankDistance(sim, alpha, invDim, numVectors, halfCluster));
        double perDimStd = R / Math.sqrt(dim);

        int nDocs = Math.min(SAMPLE_SIZES_MAGNITUDE[0], nSamples);
        float[] zeroCentroid = new float[dim];
        float[] residualScratch = new float[dim];
        int[] quantizeScratch = new int[dim];

        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(VectorSimilarityFunction.EUCLIDEAN);
        Random rng = new Random(42L);

        float[][] syntheticDocs = new float[nDocs][];
        float[] docLower = new float[nDocs];
        float[] docUpper = new float[nDocs];
        int[] docL1 = new int[nDocs];
        byte[][] docQuantized = new byte[nDocs][dim];
        double[] docNormSq = euclidean ? new double[nDocs] : null;

        for (int i = 0; i < nDocs; i++) {
            float[] v = new float[dim];
            for (int j = 0; j < dim; j++) {
                v[j] = (float) (rng.nextGaussian() * perDimStd);
            }
            syntheticDocs[i] = v;
            var qr = quantizer.scalarQuantize(v, residualScratch, quantizeScratch, (byte) dbits, zeroCentroid);
            ESVectorUtil.packAsBytes(quantizeScratch, docQuantized[i], dim);
            docLower[i] = qr.lowerInterval();
            docUpper[i] = qr.upperInterval();
            docL1[i] = qr.quantizedComponentSum();
            if (docNormSq != null) {
                docNormSq[i] = ESVectorUtil.dotProduct(v, v);
            }
        }

        WelfordVariance moments = new WelfordVariance();
        double dScale = 1.0 / ((1 << dbits) - 1);
        double qScale = 1.0 / ((1 << qbits) - 1);
        double[] simOsq = new double[nDocs];
        int[] order = new int[nDocs];
        for (int i = 0; i < nDocs; i++) {
            order[i] = i;
        }
        float[] queryScratch = new float[dim];
        float[] preconditionScratch = source.preconditioner() != null ? new float[dim] : null;
        byte[] queryQuantized = new byte[dim];
        float[] bulkDistances = new float[4];

        for (int queryOrdinal : source.queryOrdinals()) {
            CalibrationUtils.materializeCalibrationQuery(
                source.vectors(),
                queryOrdinal,
                source.baseDim(),
                dim,
                source.cosine(),
                source.neyshabur(),
                source.preconditioner(),
                usePreconditionedQueries,
                queryScratch,
                preconditionScratch
            );
            var qqr = quantizer.scalarQuantize(queryScratch, residualScratch, quantizeScratch, (byte) qbits, zeroCentroid);
            ESVectorUtil.packAsBytes(quantizeScratch, queryQuantized, dim);
            double aq = qqr.lowerInterval();
            double lq = qScale * (qqr.upperInterval() - qqr.lowerInterval());
            int queryL1val = qqr.quantizedComponentSum();

            // All docs share the single quantized query here, so score four docs per bulk SIMD call.
            int d = 0;
            int bulkLimit = nDocs - 3;
            for (; d < bulkLimit; d += 4) {
                ESVectorUtil.dotProductBulk(
                    queryQuantized,
                    docQuantized[d],
                    docQuantized[d + 1],
                    docQuantized[d + 2],
                    docQuantized[d + 3],
                    0,
                    bulkDistances
                );
                for (int t = 0; t < 4; t++) {
                    int di = d + t;
                    double ad = docLower[di];
                    double ld = dScale * (docUpper[di] - docLower[di]);
                    double dotEst = ad * aq * dim + aq * ld * docL1[di] + ad * lq * queryL1val + ld * lq * Math.round(bulkDistances[t]);
                    if (euclidean) {
                        dotEst = 2.0 * dotEst;
                        if (docNormSq != null) {
                            dotEst -= docNormSq[di];
                        }
                    }
                    simOsq[di] = dotEst;
                }
            }
            for (; d < nDocs; d++) {
                double ad = docLower[d];
                double ld = dScale * (docUpper[d] - docLower[d]);
                long intDot = (long) ESVectorUtil.dotProduct(docQuantized[d], queryQuantized);
                double dotEst = ad * aq * dim + aq * ld * docL1[d] + ad * lq * queryL1val + ld * lq * intDot;
                if (euclidean) {
                    dotEst = 2.0 * dotEst;
                    if (docNormSq != null) {
                        dotEst -= docNormSq[d];
                    }
                }
                simOsq[d] = dotEst;
            }

            sortIndicesByKeysDescending(simOsq, order, nDocs);
            int topN = Math.min(5 * source.k(), nDocs);
            for (int i = 0; i < topN; i++) {
                int docIdx = order[i];
                float[] doc = syntheticDocs[docIdx];
                double exact;
                if (euclidean) {
                    exact = 2.0 * ESVectorUtil.dotProduct(queryScratch, doc) - (docNormSq != null ? docNormSq[docIdx] : 0.0);
                } else {
                    exact = ESVectorUtil.dotProduct(queryScratch, doc);
                }
                moments.add(exact - simOsq[docIdx]);
            }
        }

        double errorStd = Math.sqrt(3.0 * moments.sampleVariance());
        double beta0 = Math.log(Math.max(errorStd, 1e-38)) - invDim * (Math.log(nDocsPerCluster) - Math.log(numVectors));
        return new QuantizationErrorStdModel(new Regression.OLSResult(beta0, invDim, 0, 0, 0, 0));
    }

    /**
     * Sorts {@code idx[0..len)} into a permutation of {@code 0..len-1} such that
     * {@code keys[idx[i]]} is non-increasing (descending).
     * <p>
     * TODO: callers only consume the top {@code 5 * k} entries, so a bounded partial selection
     * (quickselect / bounded heap) would avoid fully sorting all {@code len} docs per query.
     */
    private static void sortIndicesByKeysDescending(double[] keys, int[] idx, int len) {
        if (len < 2) {
            if (len == 1) {
                idx[0] = 0;
            }
            return;
        }
        for (int i = 0; i < len; i++) {
            idx[i] = i;
        }
        new IntroSorter() {
            double pivot;

            @Override
            protected void swap(int i, int j) {
                int tmp = idx[i];
                idx[i] = idx[j];
                idx[j] = tmp;
            }

            @Override
            protected void setPivot(int i) {
                pivot = keys[idx[i]];
            }

            @Override
            protected int comparePivot(int j) {
                // descending: pivot > keys[idx[j]] means pivot should come first
                return Double.compare(keys[idx[j]], pivot);
            }
        }.sort(0, len);
    }

    /**
     * Reusable scratch buffers for {@link #quantizedRepErrorStd}, sized for the maximum
     * sample size of a sweep. Allocated once per sweep in {@link #estimateErrorScalingFit}
     * and {@link #estimateMagnitudeModel} and threaded through each sample-size iteration to
     * avoid repeated large array allocations.
     * Every buffer is fully overwritten before it is read on each call (per the logical
     * lengths {@code nDocs}, {@code actualQueryClusters}, {@code nDocClusters}), so no
     * clearing is needed between invocations and correctness is independent of call order.
     */
    static final class QuantizedErrorScratch {
        // per-dim
        final float[] residualScratch;
        final float[] queryScratch;
        final int[] quantizeScratch;
        /** null unless cosine normalisation is active */
        final float[] normScratch;
        /** null unless a preconditioner is provided */
        final float[] preconditionScratch;

        // per-doc (sized to maxNDocs)
        final float[] docLower;
        final float[] docUpper;
        final int[] docL1;
        final byte[][] docQuantized;
        final double[] corpusDotCentroid;
        /** null unless the similarity function is EUCLIDEAN */
        final double[] docDotDoc;
        /** per-query-loop: OSQ estimated similarities, indexed [0..nDocs) */
        final double[] simOsq;
        /** per-query-loop: sort permutation over simOsq, indexed [0..nDocs) */
        final int[] order;
        /** per-query-loop: raw packed-byte doc·query dot products, indexed [0..nDocs) */
        final int[] intDots;
        /** doc indices grouped by query cluster, so bulk scoring reuses one query operand */
        final int[] bucketedDocs;
        /** CSR-style start offset of each query cluster's slice in {@link #bucketedDocs} */
        final int[] bucketStart;
        /** fill cursors used while populating {@link #bucketedDocs} */
        final int[] bucketCursor;
        /** 4-lane scratch for {@link ESVectorUtil#dotProductBulk(byte[], byte[], byte[], byte[], byte[], int, float[])} */
        final float[] bulkDistances;

        // per-query-cluster and per-doc-cluster arrays.
        // Although the target is N_QUERY_CLUSTERS query clusters, k-means can return up to
        // nDocClusters centroids (e.g. when targetSize=1). nDocClusters is itself bounded by
        // nDocs <= maxNDocs, so maxNDocs is the safe upper bound for all cluster-count arrays.
        final float[] queryLower;
        final float[] queryUpper;
        final int[] queryL1;
        final byte[][] queryQuantized;
        final double[] centroidDotCentroid;
        /** per-query-loop: query · each doc-centroid, indexed [0..nDocClusters) */
        final double[] queryDotCentroid;

        QuantizedErrorScratch(int maxNDocs, int dim, boolean cosine, boolean euclidean, boolean hasPreconditioner) {
            residualScratch = new float[dim];
            queryScratch = new float[dim];
            quantizeScratch = new int[dim];
            normScratch = cosine ? new float[dim] : null;
            preconditionScratch = hasPreconditioner ? new float[dim] : null;

            docLower = new float[maxNDocs];
            docUpper = new float[maxNDocs];
            docL1 = new int[maxNDocs];
            docQuantized = new byte[maxNDocs][dim];
            corpusDotCentroid = new double[maxNDocs];
            docDotDoc = euclidean ? new double[maxNDocs] : null;
            simOsq = new double[maxNDocs];
            order = new int[maxNDocs];
            intDots = new int[maxNDocs];
            bucketedDocs = new int[maxNDocs];
            bucketStart = new int[maxNDocs + 1];
            bucketCursor = new int[maxNDocs];
            bulkDistances = new float[4];

            queryLower = new float[maxNDocs];
            queryUpper = new float[maxNDocs];
            queryL1 = new int[maxNDocs];
            queryQuantized = new byte[maxNDocs][dim];

            centroidDotCentroid = new double[maxNDocs];
            queryDotCentroid = new double[maxNDocs];
        }
    }
}
