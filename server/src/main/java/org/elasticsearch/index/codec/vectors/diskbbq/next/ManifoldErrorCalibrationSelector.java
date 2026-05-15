/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate.CalibrationQueries;
import org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate.CalibrationUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate.ErrorModel;
import org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate.ExpectedRecall;
import org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate.ManifoldModel;
import org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate.RepErrorStdModel;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Selects a {@link IvfSegmentConfig} on
 * <strong>merge</strong> by reusing persisted segment metadata when possible, otherwise running fast or full
 * calibration on merged vectors. {@link #select} requires a non-null {@link MergeState}; flush-time calibration is not
 * supported here (flush uses codec defaults when auto_calibrate is enabled).
 */
public class ManifoldErrorCalibrationSelector implements AutoCalibrationSelector {

    private static final Logger logger = LogManager.getLogger(ManifoldErrorCalibrationSelector.class);

    static final double DEFAULT_TARGET_RECALL = 0.9;
    static final int DEFAULT_K = 10;
    public static final int MIN_VECTORS_FOR_CALIBRATION = 10_000;

    /**
     * If the merged segment is more than this factor larger than the largest input segment,
     * re-run calibration because the OLS models may not extrapolate well.
     */
    static final double RECALIBRATE_GROWTH_RATIO = 4.0;

    /**
     * Maximum per-dimension squared centroid shift before triggering re-calibration.
     * squareDistance(inputGlobalCentroid, mergedGlobalCentroid) / dim must stay below this.
     */
    static final float RECALIBRATE_DRIFT_THRESHOLD = 0.1f;

    /**
     * Minimum fraction of total docs that must agree on a single encoding to skip re-calibration
     * when input segments disagree.
     */
    static final double ENCODING_AGREEMENT_THRESHOLD = 0.8;

    /**
     * Candidate encodings paired with their (qbits, dbits) for the calibration model. Each entry encodes the ES
     * {@link ESNextDiskBBQVectorsFormat.QuantEncoding} and the query/doc bit widths used during recall estimation.
     */
    private static final CandidateEncoding[] CANDIDATES = {
        new CandidateEncoding(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, 4, 1),
        new CandidateEncoding(ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY, 4, 2),
        new CandidateEncoding(ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC, 4, 4),
        new CandidateEncoding(ESNextDiskBBQVectorsFormat.QuantEncoding.SEVEN_BIT_SYMMETRIC, 7, 7), };

    /**
     * Rerank depth multipliers (numerator/denominator), aligned with auto_osq {@code rerankDepthCandidates}.
     */
    private static final int[][] RERANK_RATIOS = { { 5, 2 }, { 15, 10 }, { 7, 4 }, { 2, 1 } };

    /**
     * Weight of rerank depth in the calibration cost model ({@code dbits + RERANK_COST_WEIGHT * rerankDepth}).
     */
    private static final double RERANK_COST_WEIGHT = 1.5;

    /**
     * Sweeps (encoding, rerank ratio) in ascending estimated cost so the first config meeting target recall is cheap.
     */
    private static final CalibrationSweep[] COST_ORDERED_SWEEPS = buildCostOrderedSweeps();

    private final int vectorsPerCluster;
    private final int blockDimension;
    private final double targetRecall;
    private final int k;

    public ManifoldErrorCalibrationSelector(int vectorsPerCluster) {
        this(vectorsPerCluster, ESNextDiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION);
    }

    public ManifoldErrorCalibrationSelector(int vectorsPerCluster, int blockDimension) {
        this(vectorsPerCluster, blockDimension, DEFAULT_TARGET_RECALL, DEFAULT_K);
    }

    public ManifoldErrorCalibrationSelector(int vectorsPerCluster, int blockDimension, double targetRecall, int k) {
        this.vectorsPerCluster = vectorsPerCluster;
        this.blockDimension = blockDimension;
        this.targetRecall = targetRecall;
        this.k = k;
    }

    /**
     * On merge, attempts to reuse quantization metadata from input segments via {@link #selectFromMergeState},
     * except for <em>bounded</em> (force-merge) merges: those run {@link #runFastCalibration} first so calibration
     * is not skipped after major segment consolidation; if the fast path does not meet {@link #targetRecall},
     * full {@link #calibrate} runs once on the same vectors (fast-then-full fallback) when the fast path does not
     * meet the configured target recall. Bounded merges are detected
     * from the merged segment's Lucene diagnostics key {@code mergeMaxNumSegments} ({@code >= 1}).
     */
    @Override
    public IvfSegmentConfig select(
        FieldInfo fieldInfo,
        FloatVectorValues floatVectorValues,
        float[] globalCentroid,
        MergeState mergeState
    ) {
        Objects.requireNonNull(mergeState, "mergeState");
        int dim = fieldInfo.getVectorDimension();
        VectorSimilarityFunction similarityFunction = fieldInfo.getVectorSimilarityFunction();
        int numVectors = floatVectorValues.size();

        if (numVectors < MIN_VECTORS_FOR_CALIBRATION) {
            return new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, false, DEFAULT_CALIBRATED_OVERSAMPLE);
        }

        MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);
        if (mergeCtx.boundedForceMerge()) {
            logger.info(
                "Merge calibration: bounded force merge (mergeMaxNumSegments=[{}], inputSegments=[{}]), skipping metadata reuse; running fast calibration",
                mergeCtx.mergeMaxNumSegmentsForLog(),
                mergeCtx.inputSegments()
            );
            try {
                // TODO : use fast if met target recall AND with good quantization error OLS fit
                // FastCalibrationOutcome fastOutcome = runFastCalibration(floatVectorValues, dim, similarityFunction, numVectors,
                // mergeCtx);
                // if (fastOutcome.metTargetRecall()) {
                // return fastOutcome.result();
                // }
                return calibrate(floatVectorValues, similarityFunction);
            } catch (IOException e) {
                logger.warn("calibration failed on bounded force merge, falling back to ONE_BIT_4BIT_QUERY", e);
                return new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, false, NO_CALIBRATED_OVERSAMPLE);
            }
        } else {
            IvfSegmentConfig reused = selectFromMergeState(fieldInfo, globalCentroid, mergeState, mergeCtx, floatVectorValues.size());
            if (reused != null) {
                return reused;
            }
            logger.debug("Merge calibration reuse not possible, running fast calibration");
            try {
                return calibrateFast(floatVectorValues, dim, similarityFunction, numVectors, mergeCtx);
            } catch (IOException e) {
                logger.warn("fast calibration failed, falling back to ONE_BIT_4BIT_QUERY", e);
                return new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, false, NO_CALIBRATED_OVERSAMPLE);
            }
        }
    }

    /**
     * Attempts to reuse calibration results from the input segments being merged.
     * Returns a merged {@link IvfSegmentConfig} if the data has not changed significantly,
     * or {@code null} if merge-time fast calibration should be performed.
     * Not used for bounded (force-merge) merges; those use {@link #runFastCalibration} with a full
     * {@link #calibrate} fallback when the fast path does not meet the target recall.
     */
    IvfSegmentConfig selectFromMergeState(
        FieldInfo fieldInfo,
        float[] mergedGlobalCentroid,
        MergeState mergeState,
        MergeCalibrationContext mergeCtx,
        long mergedVectorCount
    ) {
        int dim = fieldInfo.getVectorDimension();
        Map<ESNextDiskBBQVectorsFormat.QuantEncoding, Long> encodingDocCounts = new EnumMap<>(
            ESNextDiskBBQVectorsFormat.QuantEncoding.class
        );
        double oversampleWeightedSum = 0;
        long totalDocs = 0;
        long largestSegmentDocs = 0;
        long preconditionTrueDocs = 0;
        long preconditionFalseDocs = 0;
        int calibratedSegments = 0;

        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
            if (reader instanceof CalibrationAwareReader car) {
                ESNextDiskBBQVectorsFormat.QuantEncoding enc = car.getQuantEncoding(fieldInfo);
                if (enc == null) {
                    continue;
                }
                long docs = mergeState.maxDocs[i];
                calibratedSegments++;
                encodingDocCounts.merge(enc, docs, Long::sum);
                float oversample = car.getOversampleFactor(fieldInfo);
                oversampleWeightedSum += oversample * docs;
                totalDocs += docs;
                largestSegmentDocs = Math.max(largestSegmentDocs, docs);

                if (car.shouldPrecondition(fieldInfo)) {
                    preconditionTrueDocs += docs;
                } else {
                    preconditionFalseDocs += docs;
                }

                if (mergedGlobalCentroid != null) {
                    float[] segmentCentroid = car.getGlobalCentroid(fieldInfo);
                    if (segmentCentroid != null) {
                        float drift = ESVectorUtil.squareDistance(segmentCentroid, mergedGlobalCentroid) / dim;
                        if (drift > RECALIBRATE_DRIFT_THRESHOLD) {
                            logger.info(
                                "Merge calibration: centroid drift [{}] exceeds threshold [{}] for segment [{}], re-calibrating [inputSegments={} mergeKind={} mergeMaxNumSegments={}]",
                                drift,
                                RECALIBRATE_DRIFT_THRESHOLD,
                                i,
                                mergeCtx.inputSegments(),
                                mergeCtx.mergeKind(),
                                mergeCtx.mergeMaxNumSegmentsForLog()
                            );
                            return null;
                        }
                    }
                }
            }
        }

        if (calibratedSegments == 0) {
            return null;
        }

        if (mergedVectorCount > RECALIBRATE_GROWTH_RATIO * largestSegmentDocs) {
            logger.info(
                "Merge calibration: growth ratio [{}] exceeds threshold [{}], re-calibrating [inputSegments={} mergeKind={} mergeMaxNumSegments={}]",
                (double) mergedVectorCount / largestSegmentDocs,
                RECALIBRATE_GROWTH_RATIO,
                mergeCtx.inputSegments(),
                mergeCtx.mergeKind(),
                mergeCtx.mergeMaxNumSegmentsForLog()
            );
            return null;
        }

        if (encodingDocCounts.size() > 1) {
            long maxEncDocs = encodingDocCounts.values().stream().mapToLong(Long::longValue).max().orElse(0);
            if (maxEncDocs < ENCODING_AGREEMENT_THRESHOLD * totalDocs) {
                logger.info(
                    "Merge calibration: encoding disagreement (max encoding covers [{}]% of docs), re-calibrating [inputSegments={} mergeKind={} mergeMaxNumSegments={}]",
                    (100.0 * maxEncDocs / totalDocs),
                    mergeCtx.inputSegments(),
                    mergeCtx.mergeKind(),
                    mergeCtx.mergeMaxNumSegmentsForLog()
                );
                return null;
            }
        }

        ESNextDiskBBQVectorsFormat.QuantEncoding bestEncoding = encodingDocCounts.entrySet()
            .stream()
            .max(Map.Entry.comparingByValue())
            .get()
            .getKey();
        float avgOversample = (float) (oversampleWeightedSum / totalDocs);
        boolean doPreconditionResult = preconditionTrueDocs > preconditionFalseDocs;

        logger.info(
            "Merge calibration: reusing encoding [{}] (oversample={}, precondition={}) from [{}] input segments [inputSegments={} mergeKind={} mergeMaxNumSegments={}]",
            bestEncoding,
            avgOversample,
            doPreconditionResult,
            calibratedSegments,
            mergeCtx.inputSegments(),
            mergeCtx.mergeKind(),
            mergeCtx.mergeMaxNumSegmentsForLog()
        );
        return new IvfSegmentConfig(bestEncoding, doPreconditionResult, avgOversample);
    }

    /**
     * Reuse calibration metadata from input segments when {@link MergeState} is available but merged
     * {@link FloatVectorValues} are not yet materialized (e.g. Lucene merge entry). Uses
     * {@code mergedVectorCount} in place of {@link FloatVectorValues#size()} for growth-ratio checks.
     */
    public IvfSegmentConfig tryMergeMetadataReuse(FieldInfo fieldInfo, MergeState mergeState, long mergedVectorCount) {
        MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);
        return selectFromMergeState(fieldInfo, null, mergeState, mergeCtx, mergedVectorCount);
    }

    /**
     * Runs full calibration on {@code floatVectorValues}. The IVF merge path passes a dense heap view
     * (see {@link org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsWriter}); other callers must not pass
     * Lucene's single-pass merged {@link FloatVectorValues} without first calling
     * {@link CalibrationUtils#toHeapDenseFloatVectorValues}.
     */
    public IvfSegmentConfig calibrate(FloatVectorValues floatVectorValues, VectorSimilarityFunction similarityFunction) throws IOException {

        int numVectors = floatVectorValues.size();
        int dim = floatVectorValues.dimension();

        // sample some docs as queries
        CalibrationUtils.SampledData sampled = CalibrationUtils.sampleData(floatVectorValues);
        int[] queryOrdinals = sampled.queryOrdinals();
        int[] corpusOrdinals = sampled.corpusOrdinals();

        boolean cosine = similarityFunction == VectorSimilarityFunction.COSINE;

        // for dot-product / MIPS we need to add one more dimension with max squared norm (to treat it as it was Euclidean distance)
        boolean neyshabur = CalibrationUtils.needsNeyshaburSrebroLift(similarityFunction);

        int dimWork = dim;
        FloatVectorValues fvvForCalibration = floatVectorValues;
        if (neyshabur) {
            double maxNormSq = CalibrationUtils.maxSquaredNormOverCorpusSample(floatVectorValues, corpusOrdinals, dim);
            fvvForCalibration = new CalibrationUtils.NeyshaburCorpusFloatVectorValues(floatVectorValues, dim, maxNormSq);
            dimWork = dim + 1;
        }

        Preconditioner calibrationPreconditioner = Preconditioner.createPreconditioner(dimWork, blockDimension);
        CalibrationQueries calibrationQueries = new CalibrationQueries(
            floatVectorValues,
            queryOrdinals,
            dim,
            cosine,
            neyshabur,
            calibrationPreconditioner,
            dimWork
        );

        logger.info("Read {} corpus vectors of dimension {}", corpusOrdinals.length, dim);
        logger.info("Sampled {} queries from the corpus", queryOrdinals.length);
        logger.info("Using {} documents per cluster", vectorsPerCluster);
        logger.info("Calibrating quantization parameters");
        logger.info("block dim: {}", blockDimension);

        // manifold model uses original (un-preconditioned) data; after optional Neyshabur lift for dot/MIP.
        double[] manifold = ManifoldModel.estimateManifoldParameters(
            similarityFunction,
            dimWork,
            calibrationQueries,
            fvvForCalibration,
            corpusOrdinals,
            k
        );
        double alpha = manifold[0];
        double invDim = manifold[1];

        // fit the error scaling model on random orthogonal transforms,
        // independent of whether the field enables preconditioning at index time
        FloatVectorValues fvvOrth = preconditionFvv(fvvForCalibration, calibrationPreconditioner);

        RepErrorStdModel errorScalingModel = ErrorModel.estimateRepErrorStdScalingParameter(
            similarityFunction,
            dimWork,
            calibrationQueries,
            fvvOrth,
            corpusOrdinals,
            cosine,
            k
        );

        SweepOutcome outcome = sweepQuantizationCandidates(
            similarityFunction,
            numVectors,
            alpha,
            invDim,
            dimWork,
            calibrationQueries,
            fvvForCalibration,
            fvvOrth,
            corpusOrdinals,
            cosine,
            errorScalingModel,
            false
        );
        if (outcome.metTargetRecall()) {
            logger.info(
                "Selected: encoding [{}] docs per cluster {} preconditioning {} {} query bits {} document bits rerank {} candidates (expected recall {}%)",
                outcome.config().quantEncoding(),
                vectorsPerCluster,
                outcome.config().usePrecondition(),
                outcome.selectedQbits(),
                outcome.selectedDbits(),
                outcome.selectedRerankN(),
                String.format(Locale.ROOT, "%.2f", outcome.selectedExpectedRecall() * 100.0)
            );
            return outcome.config();
        }
        logger.info(
            "Calibration: no encoding met target recall [{}], selecting best [{}] with oversample [{}] precondition [{}] and recall [{}]",
            targetRecall,
            outcome.config().quantEncoding(),
            outcome.config().rescoreOversample(),
            outcome.config().usePrecondition(),
            outcome.bestRecall()
        );
        return outcome.config();
    }

    /**
     * Outcome of {@link #runFastCalibration}: {@code metTargetRecall} is true when an encoding met the
     * target recall before the best-effort path.
     */
    protected record FastCalibrationOutcome(IvfSegmentConfig result, boolean metTargetRecall) {}

    /**
     * Runs calibration with reduced sample sizes, fewer sweep iterations, and fewer
     * manifold model data points for faster execution during merge re-calibration.
     */
    IvfSegmentConfig calibrateFast(
        FloatVectorValues floatVectorValues,
        int dim,
        VectorSimilarityFunction similarityFunction,
        int N,
        MergeCalibrationContext mergeCtx
    ) throws IOException {
        return runFastCalibration(floatVectorValues, dim, similarityFunction, N, mergeCtx).result();
    }

    /**
     * Same work as {@link #calibrateFast} but exposes whether the target recall was reached (for bounded-merge fallback).
     */
    protected FastCalibrationOutcome runFastCalibration(
        FloatVectorValues floatVectorValues,
        int dim,
        VectorSimilarityFunction similarityFunction,
        int N,
        MergeCalibrationContext mergeCtx
    ) throws IOException {
        CalibrationUtils.SampledData sampled = CalibrationUtils.sampleDataFast(floatVectorValues);
        int[] queryOrdinals = sampled.queryOrdinals();
        int[] corpusOrdinals = sampled.corpusOrdinals();

        boolean cosine = similarityFunction == VectorSimilarityFunction.COSINE;
        boolean neyshabur = CalibrationUtils.needsNeyshaburSrebroLift(similarityFunction);

        int dimWork = dim;
        FloatVectorValues fvvForCalibration = floatVectorValues;
        if (neyshabur) {
            double maxNormSq = CalibrationUtils.maxSquaredNormOverCorpusSample(floatVectorValues, corpusOrdinals, dim);
            fvvForCalibration = new CalibrationUtils.NeyshaburCorpusFloatVectorValues(floatVectorValues, dim, maxNormSq);
            dimWork = dim + 1;
        }

        Preconditioner calibrationPreconditioner = Preconditioner.createPreconditioner(dimWork, blockDimension);
        CalibrationQueries calibrationQueries = new CalibrationQueries(
            floatVectorValues,
            queryOrdinals,
            dim,
            cosine,
            neyshabur,
            calibrationPreconditioner,
            dimWork
        );

        logger.info("Read {} corpus vectors of dimension {} (fast calibration sample)", corpusOrdinals.length, dim);
        logger.info("Sampled {} queries from the corpus", queryOrdinals.length);
        logger.info("Using {} documents per cluster", vectorsPerCluster);
        logger.info("Calibrating quantization parameters (fast)");
        logger.info("block dim: {}", blockDimension);

        double[] manifold = ManifoldModel.estimateManifoldParametersFast(
            similarityFunction,
            dimWork,
            calibrationQueries,
            fvvForCalibration,
            corpusOrdinals,
            k
        );
        double alpha = manifold[0];
        double invDim = manifold[1];

        FloatVectorValues fvvOrth = preconditionFvv(fvvForCalibration, calibrationPreconditioner);

        RepErrorStdModel errorScalingModel = ErrorModel.estimateRepErrorStdScalingParameterFast(
            similarityFunction,
            dimWork,
            calibrationQueries,
            fvvOrth,
            corpusOrdinals,
            cosine,
            k
        );

        SweepOutcome outcome = sweepQuantizationCandidates(
            similarityFunction,
            N,
            alpha,
            invDim,
            dimWork,
            calibrationQueries,
            fvvForCalibration,
            fvvOrth,
            corpusOrdinals,
            cosine,
            errorScalingModel,
            true
        );
        if (outcome.metTargetRecall()) {
            if (mergeCtx != null) {
                logger.info(
                    "Fast calibration Selected: encoding [{}] docs per cluster {} preconditioning {} {} query bits {} document bits rerank {} candidates (expected recall {}%) [inputSegments={} mergeKind={} mergeMaxNumSegments={}]",
                    outcome.config().quantEncoding(),
                    vectorsPerCluster,
                    outcome.config().usePrecondition(),
                    outcome.selectedQbits(),
                    outcome.selectedDbits(),
                    outcome.selectedRerankN(),
                    String.format(Locale.ROOT, "%.2f", outcome.selectedExpectedRecall() * 100.0),
                    mergeCtx.inputSegments(),
                    mergeCtx.mergeKind(),
                    mergeCtx.mergeMaxNumSegmentsForLog()
                );
            } else {
                logger.info(
                    "Fast calibration Selected: encoding [{}] docs per cluster {} preconditioning {} {} query bits {} document bits rerank {} candidates (expected recall {}%)",
                    outcome.config().quantEncoding(),
                    vectorsPerCluster,
                    outcome.config().usePrecondition(),
                    outcome.selectedQbits(),
                    outcome.selectedDbits(),
                    outcome.selectedRerankN(),
                    String.format(Locale.ROOT, "%.2f", outcome.selectedExpectedRecall() * 100.0)
                );
            }
            return new FastCalibrationOutcome(outcome.config(), true);
        }
        if (mergeCtx != null) {
            logger.info(
                "Fast calibration: no encoding met target recall [{}], selecting best [{}] oversample [{}] precondition [{}] recall [{}] [inputSegments={} mergeKind={} mergeMaxNumSegments={}]",
                targetRecall,
                outcome.config().quantEncoding(),
                outcome.config().rescoreOversample(),
                outcome.config().usePrecondition(),
                outcome.bestRecall(),
                mergeCtx.inputSegments(),
                mergeCtx.mergeKind(),
                mergeCtx.mergeMaxNumSegmentsForLog()
            );
        } else {
            logger.info(
                "Fast calibration: no encoding met target recall [{}], selecting best [{}] oversample [{}] precondition [{}] recall [{}]",
                targetRecall,
                outcome.config().quantEncoding(),
                outcome.config().rescoreOversample(),
                outcome.config().usePrecondition(),
                outcome.bestRecall()
            );
        }
        return new FastCalibrationOutcome(outcome.config(), false);
    }

    private static CalibrationSweep[] buildCostOrderedSweeps() {
        List<CalibrationSweep> sweeps = new ArrayList<>();
        for (CandidateEncoding candidate : CANDIDATES) {
            for (int[] rerankRatio : RERANK_RATIOS) {
                sweeps.add(
                    new CalibrationSweep(
                        candidate,
                        rerankRatio[0],
                        rerankRatio[1],
                        calibrationCost(candidate.dbits(), rerankRatio[0], rerankRatio[1])
                    )
                );
            }
        }
        sweeps.sort(Comparator.comparingDouble(CalibrationSweep::cost));
        return sweeps.toArray(CalibrationSweep[]::new);
    }

    private static double calibrationCost(int dbits, int rerankNumerator, int rerankDenominator) {
        return dbits + RERANK_COST_WEIGHT * ((double) rerankNumerator / rerankDenominator);
    }

    private static int configurationKey(int qbits, int dbits, boolean precondition) {
        return 16 * qbits + 2 * dbits + (precondition ? 1 : 0);
    }

    private SweepOutcome sweepQuantizationCandidates(
        VectorSimilarityFunction similarityFunction,
        int numVectors,
        double alpha,
        double invDim,
        int dimWork,
        CalibrationQueries calibrationQueries,
        FloatVectorValues fvvForCalibration,
        FloatVectorValues fvvOrth,
        int[] corpusOrdinals,
        boolean cosine,
        RepErrorStdModel errorScalingModel,
        boolean fast
    ) throws IOException {
        double bestRecall = -1;
        ESNextDiskBBQVectorsFormat.QuantEncoding bestEncoding = ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY;
        float bestOversample = AutoCalibrationSelector.NO_CALIBRATED_OVERSAMPLE;
        boolean bestPrecondition = false;

        Map<Integer, RepErrorStdModel> errorModelCache = new HashMap<>();
        boolean[] preconditionValues = new boolean[] { false, true };

        for (boolean precondition : preconditionValues) {
            for (CalibrationSweep sweep : COST_ORDERED_SWEEPS) {
                CandidateEncoding candidate = sweep.candidate();
                int configKey = configurationKey(candidate.qbits(), candidate.dbits(), precondition);
                RepErrorStdModel errorModel = errorModelCache.get(configKey);
                if (errorModel == null) {
                    FloatVectorValues magnitudeFvv = precondition ? fvvOrth : fvvForCalibration;
                    errorModel = fast
                        ? ErrorModel.estimateRepErrorStdMagnitudeParameterFast(
                            errorScalingModel,
                            similarityFunction,
                            dimWork,
                            calibrationQueries,
                            precondition,
                            magnitudeFvv,
                            corpusOrdinals,
                            cosine,
                            k,
                            candidate.qbits(),
                            candidate.dbits()
                        )
                        : ErrorModel.estimateRepErrorStdMagnitudeParameter(
                            errorScalingModel,
                            similarityFunction,
                            dimWork,
                            calibrationQueries,
                            precondition,
                            magnitudeFvv,
                            corpusOrdinals,
                            cosine,
                            k,
                            candidate.qbits(),
                            candidate.dbits()
                        );
                    errorModelCache.put(configKey, errorModel);
                }

                int rerankVal = ExpectedRecall.rerankN(k, sweep.rerankNumerator(), sweep.rerankDenominator());
                float oversample = (float) sweep.rerankNumerator() / sweep.rerankDenominator();
                double errorStd = errorModel.quantizeRepErrorStd(vectorsPerCluster, numVectors);
                double expected = ExpectedRecall.expectedRecallAtK(similarityFunction, numVectors, alpha, invDim, errorStd, k, rerankVal);

                logger.info(
                    "Quantization recall(({}, {}) | {}, {}) = {}%",
                    candidate.qbits(),
                    candidate.dbits(),
                    rerankVal,
                    precondition ? "precondition" : "no precondition",
                    String.format(Locale.ROOT, "%.2f", expected * 100.0)
                );

                if (expected >= targetRecall) {
                    IvfSegmentConfig config = new IvfSegmentConfig(candidate.encoding(), precondition, oversample);
                    return new SweepOutcome(config, expected, true, candidate.qbits(), candidate.dbits(), rerankVal, bestRecall);
                }
                if (expected > bestRecall) {
                    bestRecall = expected;
                    bestEncoding = candidate.encoding();
                    bestOversample = oversample;
                    bestPrecondition = precondition;
                }
            }
        }

        return new SweepOutcome(
            new IvfSegmentConfig(bestEncoding, bestPrecondition, bestOversample),
            bestRecall,
            false,
            -1,
            -1,
            -1,
            bestRecall
        );
    }

    private static FloatVectorValues preconditionFvv(FloatVectorValues fvv, Preconditioner preconditioner) {
        return new FloatVectorValues() {
            final float[] preconditioned = new float[fvv.dimension()];
            int cachedOrd = -1;

            @Override
            public float[] vectorValue(int ord) throws IOException {
                if (ord != cachedOrd) {
                    float[] raw = fvv.vectorValue(ord);
                    preconditioner.applyTransform(raw, preconditioned);
                    cachedOrd = ord;
                }
                return preconditioned;
            }

            @Override
            public FloatVectorValues copy() throws IOException {
                return fvv.copy();
            }

            @Override
            public int dimension() {
                return fvv.dimension();
            }

            @Override
            public int size() {
                return fvv.size();
            }

            @Override
            public DocIndexIterator iterator() {
                return fvv.iterator();
            }
        };
    }

    private record CalibrationSweep(CandidateEncoding candidate, int rerankNumerator, int rerankDenominator, double cost) {}

    private record SweepOutcome(
        IvfSegmentConfig config,
        double selectedExpectedRecall,
        boolean metTargetRecall,
        int selectedQbits,
        int selectedDbits,
        int selectedRerankN,
        double bestRecall
    ) {}

    private record CandidateEncoding(ESNextDiskBBQVectorsFormat.QuantEncoding encoding, int qbits, int dbits) {}
}
