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
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.IntroSorter;
import org.elasticsearch.core.WelfordVariance;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;

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
            var queryClustering = kmeans.cluster(centroidVectors, targetSize, warmStartQueryCentroids);
            queryCentroids = queryClustering.centroids();
            docCentroidAssignments = queryClustering.assignments();
        }
        int actualQueryClusters = queryCentroids.length;

        // Cluster-count scratch: sized to the actual cluster counts (~nDocs / vectorsPerCluster), not maxNDocs.
        double[] centroidDotCentroid = new double[nDocClusters];
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

        float[] queryLower = new float[actualQueryClusters];
        float[] queryUpper = new float[actualQueryClusters];
        int[] queryL1 = new int[actualQueryClusters];
        byte[][] queryQuantized = new byte[actualQueryClusters][dim];

        float[] queryScratch = scratch.queryScratch;
        float[] preconditionScratch = scratch.preconditionScratch;

        double[] queryDotCentroid = new double[nDocClusters];
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

            int topN = Math.min(5 * source.k(), nDocs);
            selectTopNDescending(simOsq, order, nDocs, topN);
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
        var docClusters = kmeans.cluster(corpusVectors, nDocsPerCluster, warmStartDocCentroids);

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

        Regression.OLSResult rawParams = state.fit();
        if (rawParams == Regression.OLSResult.ZERO) {
            return new ErrorScalingFit(
                new QuantizationErrorStdModel(Regression.OLSResult.ZERO),
                warmStartDocCentroids,
                warmStartQueryCentroids
            );
        }

        // Clamp to 0 so the error model degenerates to a constant (no corpus-size extrapolation) rather than
        // producing unbounded estimates.
        // This can happen when the k-means warm-start introduces non-monotone error steps
        // over the narrow x-range of the sweep (~1.3 log-units for typical nDocsPerCluster).
        Regression.OLSResult params;
        if (rawParams.beta1() < 0) {
            logger.debug(
                () -> format(
                    "Error scaling fit produced negative slope [%.4f] (R²=[%.4f]); "
                        + "clamping to 0 (constant error model, no corpus-size extrapolation).",
                    rawParams.beta1(),
                    state.r2(rawParams)
                )
            );
            params = new Regression.OLSResult(rawParams.beta0(), 0.0, 0, 0, 0, rawParams.sigmaSq());
        } else {
            params = rawParams;
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

    /** Corpus sample size for the single-shot real-residual magnitude measurement. */
    static final int REAL_RESIDUAL_SAMPLE = 2048;

    /**
     * Opaque per-sweep state for the real-residual magnitude path: reusable OSQ scratch, a serial k-means
     * instance, and the (encoding-independent) clustering from the first candidate, reused as a warm start
     * for subsequent candidates so k-means is not recomputed from scratch per encoding. Construct once per
     * calibration via {@link #newRealResidualState} and thread through every candidate.
     */
    public static final class RealResidualState {
        private final QuantizedErrorScratch scratch;
        private final HierarchicalKMeans<float[]> kmeans;
        private final int nDocs;
        private QuantizedErrorComputeResult shared;

        private RealResidualState(CalibrationSource source) {
            this.nDocs = Math.min(REAL_RESIDUAL_SAMPLE, source.corpusOrdinals().length);
            this.kmeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, source.dim());
            this.scratch = new QuantizedErrorScratch(
                nDocs,
                source.dim(),
                source.cosine(),
                source.similarityFunction() == VectorSimilarityFunction.EUCLIDEAN,
                source.preconditioner() != null
            );
        }

        public int ndocs() {
            return nDocs;
        }
    }

    /** Creates the shared state for a real-residual magnitude sweep over {@code source}. */
    public static RealResidualState newRealResidualState(CalibrationSource source) {
        return new RealResidualState(source);
    }

    /**
     * Estimates the quantization error-std model for {@code (qbits, dbits)} from <em>real</em> corpus
     * residuals. The clustering warm start is reused across candidates via {@code state} so k-means is
     * not recomputed per encoding.
     * <p>
     * Measures OSQ error once at {@link #REAL_RESIDUAL_SAMPLE} and anchors the intercept at that sample
     * size. The manifold slope {@code invDim} is used as the scaling exponent, so evaluating at the real corpus size {@code N}
     * extrapolates as {@code errorStd = measuredStd × (REAL_RESIDUAL_SAMPLE / N)^invDim}.
     */
    public static QuantizationErrorStdModel estimateMagnitudeFromRealResiduals(
        double invDim,
        CalibrationSource source,
        boolean usePreconditionedQueries,
        int qbits,
        int dbits,
        int nDocsPerCluster,
        RealResidualState state
    ) throws IOException {
        float[][] warmDoc = state.shared == null ? null : state.shared.docCentroids();
        float[][] warmQuery = state.shared == null ? null : state.shared.queryCentroids();
        QuantizedErrorComputeResult r = quantizedRepErrorStdWithCentroids(
            source,
            usePreconditionedQueries,
            state.nDocs,
            nDocsPerCluster,
            qbits,
            dbits,
            state.kmeans,
            warmDoc,
            warmQuery,
            state.scratch
        );
        if (state.shared == null) {
            state.shared = r;
        }
        // 1/d is negative for similarities like cosine, so use -invDim
        double invDimEffective = ManifoldModel.isDotLike(source.similarityFunction()) ? -invDim : invDim;
        // single measurement anchored at state.nDocs (not numVectors),
        // so evaluating at N gives measuredStd × (state.nDocs / N)^invDim
        double beta0 = Math.log(Math.max(r.std(), 1e-38)) - invDimEffective * (Math.log(nDocsPerCluster) - Math.log(state.nDocs));
        return new QuantizationErrorStdModel(new Regression.OLSResult(beta0, invDim, 0, 0, 0, 0));
    }

    /**
     * Fills {@code idx[0..n)} with the indices of the {@code n} largest {@code keys[0..len)}, ordered by
     * descending key.
     * Requires {@code idx.length >= len} and {@code 0 <= n <= len}. Survivors are ordered descending so the caller
     * accumulates error moments largest-first.
     */
    static void selectTopNDescending(double[] keys, int[] idx, int len, int n) {
        for (int i = 0; i < len; i++) {
            idx[i] = i;
        }
        int m = Math.min(n, len);
        if (m <= 0) {
            return;
        }
        if (m < len) {
            // partition idx so idx[0..m) hold the m largest keys (unordered). select(from, to, k) leaves the k
            // elements that sort first in [from, k); under this descending comparator those are the m largest.
            new IntroSelector() {
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
                    // descending: pivot sorts before j when pivot's key is larger
                    return Double.compare(keys[idx[j]], pivot);
                }
            }.select(0, len, m);
        }
        // order the m selected indices descending by key.
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
                // descending: pivot sorts before j when pivot's key is larger
                return Double.compare(keys[idx[j]], pivot);
            }
        }.sort(0, m);
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

        // Cluster-count arrays (queryLower/Upper/L1/queryQuantized, centroidDotCentroid, queryDotCentroid) are
        // indexed by cluster (nDocClusters / actualQueryClusters ~= nDocs / vectorsPerCluster).
        // they are allocated per call in quantizedRepErrorStd at the exact cluster count, which is known once k-means has run.

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
        }
    }
}
