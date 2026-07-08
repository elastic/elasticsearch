/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.calibrate.CalibrationSource;
import org.elasticsearch.index.codec.vectors.diskbbq.calibrate.CalibrationUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.calibrate.ErrorModel;
import org.elasticsearch.index.codec.vectors.diskbbq.calibrate.ErrorScalingFit;
import org.elasticsearch.index.codec.vectors.diskbbq.calibrate.ExpectedRecall;
import org.elasticsearch.index.codec.vectors.diskbbq.calibrate.ManifoldModel;
import org.elasticsearch.index.codec.vectors.diskbbq.calibrate.QuantizationErrorStdModel;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * Resolves a {@link IvfSegmentConfig} on <strong>merge</strong> when {@code auto_calibrate} is enabled: reuses
 * persisted segment metadata when possible, otherwise runs full calibration on merged vectors.
 * {@link #resolve} requires a non-null {@link MergeState}. Segments with fewer than
 * {@link #MIN_VECTORS_FOR_CALIBRATION} merged vectors get {@link #DEFAULT_CALIBRATED_OVERSAMPLE}.
 */
public class IvfAutoCalibration {

    private static final Logger logger = LogManager.getLogger(IvfAutoCalibration.class);

    /**
     * Sentinel for segments with no calibrated oversample persisted.
     */
    public static final float NO_CALIBRATED_OVERSAMPLE = Float.NaN;

    /**
     * Default oversample used when the segment is too small for calibration.
     */
    public static final float DEFAULT_CALIBRATED_OVERSAMPLE = 3f;

    /**
     * Default target recall for calibration sweeps. Calibration selects the cheapest
     * (encoding, rerank-depth) pair whose predicted recall meets or exceeds this value.
     */
    static final double DEFAULT_TARGET_RECALL = 0.9;

    /**
     * Default number of nearest neighbors {@code k} used in recall estimation during calibration.
     */
    static final int DEFAULT_K = 10;

    public static final int MIN_VECTORS_FOR_CALIBRATION = 10_000;

    /**
     * Minimum fraction of total docs that must agree on a single encoding to skip re-calibration
     * when input segments disagree.
     */
    static final double ENCODING_AGREEMENT_THRESHOLD = 0.8;

    /**
     * Lucene diagnostic key written by {@code IndexWriter} to record the force-merge target segment
     * count. Background merges use {@code -1}; force merges set a target ({@code >= 1}).
     * This is an internal Lucene convention, not a stable API.
     */
    private static final String MERGE_MAX_NUM_SEGMENTS_DIAG_KEY = "mergeMaxNumSegments";

    /**
     * Candidate encodings paired with their (qbits, dbits) for the calibration model. Each entry encodes the ES
     * {@link QuantEncoding} and the query/doc bit widths used during recall estimation.
     */
    private static final CandidateEncoding[] CANDIDATES = {
        new CandidateEncoding(QuantEncoding.ONE_BIT_1BIT_QUERY, 1, 1),
        new CandidateEncoding(QuantEncoding.ONE_BIT_4BIT_QUERY, 4, 1),
        new CandidateEncoding(QuantEncoding.TWO_BIT_4BIT_QUERY, 4, 2),
        new CandidateEncoding(QuantEncoding.FOUR_BIT_SYMMETRIC, 4, 4),
        new CandidateEncoding(QuantEncoding.SEVEN_BIT_SYMMETRIC, 7, 7), };

    /**
     * Rerank depth multipliers swept in ascending cost order.
     */
    private static final double[] RERANK_DEPTHS = { 1.25, 1.5, 1.75, 2.0, 2.5, 3.0 };

    /**
     * The distinct candidate quantization encodings evaluated during calibration. Exposed so tests can assert
     * that a calibrated segment picked one of these without duplicating (and drifting from) the list here.
     */
    public static Set<QuantEncoding> candidateEncodings() {
        return Arrays.stream(CANDIDATES).map(CandidateEncoding::encoding).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * The distinct rerank oversample factors evaluated during calibration, mirroring {@link #RERANK_DEPTHS}.
     * Exposed so tests stay in sync with the swept values.
     */
    public static Set<Float> rerankOversamples() {
        return Arrays.stream(RERANK_DEPTHS).mapToObj(d -> (float) d).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Weight of rerank depth in the calibration cost model ({@code dbits + RERANK_COST_WEIGHT * rerankDepth}).
     * A value greater than 1 penalizes rerank depth more than an extra doc bit, reflecting that
     * oversampling raises query-time DRAM pressure across <em>all</em> candidate vectors while an
     * extra doc bit only raises storage cost. The coefficient 1.3 was chosen empirically to prefer
     * low-bit encodings over aggressive reranking when both achieve similar recall.
     */
    private static final double RERANK_COST_WEIGHT = 1.3;

    /**
     * Sweeps (encoding, rerank ratio) in ascending estimated cost so the first config meeting target recall is cheap.
     */
    private static final CalibrationSweep[] COST_ORDERED_SWEEPS = buildCostOrderedSweeps();

    private final int vectorsPerCluster;
    private final int blockDimension;
    private final double targetRecall;
    private final int k;

    public IvfAutoCalibration(int vectorsPerCluster) {
        this(vectorsPerCluster, ES950DiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION);
    }

    public IvfAutoCalibration(int vectorsPerCluster, int blockDimension) {
        this(vectorsPerCluster, blockDimension, DEFAULT_TARGET_RECALL, DEFAULT_K);
    }

    public IvfAutoCalibration(int vectorsPerCluster, int blockDimension, double targetRecall, int k) {
        this.vectorsPerCluster = vectorsPerCluster;
        this.blockDimension = blockDimension;
        this.targetRecall = targetRecall;
        this.k = k;
    }

    /**
     * Returns an {@link IvfMergeConfigResolver} that runs merge-time auto-calibration for the given cluster size.
     */
    public static IvfMergeConfigResolver mergeConfigResolver(int vectorsPerCluster) {
        return (fieldInfo, mergeState, codecDefault) -> new IvfAutoCalibration(vectorsPerCluster).resolve(
            fieldInfo,
            mergeState,
            codecDefault
        );
    }

    /**
     * On merge, attempts to reuse quantization metadata from input segments via {@link #selectFromMergeState}.
     * When reuse is not possible, runs full calibration on merged vectors. Bounded (force-merge) merges
     * skip metadata reuse and always calibrate.
     */
    public IvfSegmentConfig resolve(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig codecDefault) throws IOException {
        Objects.requireNonNull(mergeState, "mergeState");
        Objects.requireNonNull(codecDefault, "codecDefault");
        VectorSimilarityFunction similarityFunction = fieldInfo.getVectorSimilarityFunction();
        // count total vectors cheaply from segment metadata, no need to open a merged view yet.
        int numVectors = CalibrationUtils.countMergedVectors(fieldInfo, mergeState);

        if (numVectors < MIN_VECTORS_FOR_CALIBRATION) {
            logger.debug(
                "Merge calibration: merged segment has [{}] vectors, below threshold [{}], skipping calibration",
                numVectors,
                MIN_VECTORS_FOR_CALIBRATION
            );
            return codecDefault;
        }

        if (isBoundedForceMerge(mergeState) == false) {
            IvfSegmentConfig reused = selectFromMergeState(fieldInfo, mergeState);
            if (reused != null) {
                return reused;
            }
        } else {
            logger.debug(
                "Merge calibration: bounded force merge (inputSegments=[{}]), skipping metadata reuse",
                mergeState.knnVectorsReaders == null ? 0 : mergeState.knnVectorsReaders.length
            );
            try {
                KMeansFloatVectorValues sampledVectors = CalibrationUtils.buildSampled(fieldInfo, mergeState, numVectors);
                return calibrate(sampledVectors, similarityFunction, numVectors, CalibrationMode.FULL);
            } catch (IOException e) {
                logger.warn("calibration failed on bounded force merge, falling back to codec default encoding", e);
                return codecDefault;
            }
        }

        try {
            KMeansFloatVectorValues sampledVectors = CalibrationUtils.buildSampled(fieldInfo, mergeState, numVectors);
            return calibrate(sampledVectors, similarityFunction, numVectors, CalibrationMode.FAST);
        } catch (IOException e) {
            logger.warn("calibration failed on merge, falling back to codec default encoding", e);
            return codecDefault;
        }
    }

    /**
     * Returns {@code true} when {@code mergeState} represents a bounded force merge
     * (i.e. the Lucene {@code IndexWriter} set a target segment count {@code >= 1}).
     * Background merges return {@code false}. Detection relies on the
     * {@value MERGE_MAX_NUM_SEGMENTS_DIAG_KEY} diagnostic key; if absent or unparseable the
     * merge is treated as a background merge.
     */
    static boolean isBoundedForceMerge(MergeState mergeState) {
        if (mergeState.segmentInfo == null) {
            return false;
        }
        Map<String, String> diagnostics = mergeState.segmentInfo.getDiagnostics();
        if (diagnostics == null) {
            return false;
        }
        String raw = diagnostics.get(MERGE_MAX_NUM_SEGMENTS_DIAG_KEY);
        if (raw == null) {
            return false;
        }
        try {
            return Integer.parseInt(raw) >= 1;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Attempts to reuse calibration results from the input segments being merged.
     * Returns a merged {@link IvfSegmentConfig} if the data has not changed significantly,
     * or {@code null} if merge-time calibration should be performed.
     */
    IvfSegmentConfig selectFromMergeState(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        Map<QuantEncoding, EncodingStats> byEncoding = new EnumMap<>(QuantEncoding.class);
        long totalVectors = 0;
        int calibratedSegments = 0;

        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
            if (reader instanceof PerFieldKnnVectorsFormat.FieldsReader perField) {
                reader = perField.getFieldReader(fieldInfo.name);
            }
            if (reader instanceof CalibrationAwareReader car) {
                QuantEncoding enc = car.getQuantEncoding(fieldInfo);
                if (Float.isNaN(car.getOversampleFactor(fieldInfo)) || enc == null) {
                    continue;
                }
                long vectors = liveVectorCount(reader, fieldInfo, mergeState.liveDocs[i]);
                if (vectors == 0) {
                    continue;
                }
                calibratedSegments++;
                EncodingStats stats = byEncoding.computeIfAbsent(enc, e -> new EncodingStats());
                stats.vectors += vectors;
                stats.oversampleWeightedSum += (double) car.getOversampleFactor(fieldInfo) * vectors;
                if (car.shouldPrecondition(fieldInfo)) {
                    stats.preconditionTrueVectors += vectors;
                } else {
                    stats.preconditionFalseVectors += vectors;
                }
                totalVectors += vectors;
            }
        }

        if (calibratedSegments == 0 || totalVectors == 0) {
            return null;
        }

        Map.Entry<QuantEncoding, EncodingStats> best = byEncoding.entrySet()
            .stream()
            .max(Comparator.comparingLong(e -> e.getValue().vectors))
            .orElseThrow();
        long maxEncVectors = best.getValue().vectors;
        if (byEncoding.size() > 1 && maxEncVectors < ENCODING_AGREEMENT_THRESHOLD * totalVectors) {
            logger.debug(
                "Merge calibration: encoding disagreement (max encoding covers [{}]% of vectors), re-calibrating [inputSegments={}]",
                (100.0 * maxEncVectors / totalVectors),
                mergeState.knnVectorsReaders.length
            );
            return null;
        }

        // oversample and precondition are derived from the winning encoding's segments
        QuantEncoding bestEncoding = best.getKey();
        EncodingStats bestStats = best.getValue();
        float avgOversample = (float) (bestStats.oversampleWeightedSum / bestStats.vectors);
        boolean doPreconditionResult = bestStats.preconditionTrueVectors > bestStats.preconditionFalseVectors;

        logger.debug(
            "Merge calibration: reusing encoding [{}] (oversample={}, precondition={}) from [{}] input segments",
            bestEncoding,
            avgOversample,
            doPreconditionResult,
            calibratedSegments
        );
        return new IvfSegmentConfig(CentroidIndexFormat.FLAT, bestEncoding, doPreconditionResult, avgOversample);
    }

    /** Per-encoding accumulator for {@link #selectFromMergeState}: live-vector-weighted oversample and precondition votes. */
    private static final class EncodingStats {
        long vectors;
        double oversampleWeightedSum;
        long preconditionTrueVectors;
        long preconditionFalseVectors;
    }

    /**
     * number of live (non-deleted) vectors for {@code fieldInfo} in a single merge input.
     */
    private static long liveVectorCount(KnnVectorsReader reader, FieldInfo fieldInfo, Bits liveDocs) throws IOException {
        KnnVectorValues values = fieldInfo.getVectorEncoding() == VectorEncoding.BYTE
            ? reader.getByteVectorValues(fieldInfo.name)
            : reader.getFloatVectorValues(fieldInfo.name);
        if (values == null) {
            return 0;
        }
        if (liveDocs == null) {
            return values.size();
        }
        long live = 0;
        KnnVectorValues.DocIndexIterator iterator = values.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            if (liveDocs.get(doc)) {
                live++;
            }
        }
        return live;
    }

    /**
     * Runs full calibration on {@code floatVectorValues}. Uses {@code floatVectorValues.size()} as
     * the corpus size for the recall model.
     */
    public IvfSegmentConfig calibrate(FloatVectorValues floatVectorValues, VectorSimilarityFunction similarityFunction) throws IOException {
        return calibrate(floatVectorValues, similarityFunction, floatVectorValues.size());
    }

    /**
     * Like {@link #calibrate(FloatVectorValues, VectorSimilarityFunction)} but uses {@code realNumVectors}
     * as the corpus size for the recall model rather than {@code floatVectorValues.size()}. Used when
     * {@code floatVectorValues} is a reservoir-sampled subset of a larger merged corpus.
     */
    protected IvfSegmentConfig calibrate(
        FloatVectorValues floatVectorValues,
        VectorSimilarityFunction similarityFunction,
        int realNumVectors
    ) throws IOException {
        return calibrate(floatVectorValues, similarityFunction, realNumVectors, CalibrationMode.FAST);
    }

    /**
     * Like {@link #calibrate(FloatVectorValues, VectorSimilarityFunction, int)} but selects
     * the calibration strategy via {@code mode}:
     * <ul>
     *   <li>{@link CalibrationMode#FAST} — synthetic manifold residuals; zero k-means; fast but less
     *       precise. Used for background merges.</li>
     *   <li>{@link CalibrationMode#FULL} — k-means, per-cluster NN assignment, and OLS regression;
     *       slower but accurate. Used for bounded force merges.</li>
     * </ul>
     */
    protected IvfSegmentConfig calibrate(
        FloatVectorValues floatVectorValues,
        VectorSimilarityFunction similarityFunction,
        int realNumVectors,
        CalibrationMode mode
    ) throws IOException {
        CalibrationContext ctx = prepareCalibrationRun(floatVectorValues, similarityFunction, realNumVectors);
        logger.debug("Calibrating quantization parameters");

        SweepOutcome outcome = runCalibrationPipeline(ctx, similarityFunction, mode);

        switch (outcome) {
            case SweepOutcome.Success s -> logger.debug(
                () -> format(
                    "Selected: encoding [%s] docs per cluster %d preconditioning %s %d query bits %d document bits"
                        + " rerank %d candidates (expected recall %.2f%%)",
                    outcome.config().quantEncoding(),
                    vectorsPerCluster,
                    outcome.config().usePrecondition(),
                    s.qbits(),
                    s.dbits(),
                    s.rerankN(),
                    s.expectedRecall() * 100.0
                )
            );
            case SweepOutcome.BestEffort b -> logger.debug(
                "No encoding met target recall [{}], selecting best [{}] with oversample [{}] precondition [{}] and recall [{}]",
                targetRecall,
                outcome.config().quantEncoding(),
                outcome.config().rescoreOversample(),
                outcome.config().usePrecondition(),
                b.bestRecall()
            );
        }
        return outcome.config();
    }

    private CalibrationContext prepareCalibrationRun(
        FloatVectorValues floatVectorValues,
        VectorSimilarityFunction similarityFunction,
        int numVectors
    ) throws IOException {
        int dim = floatVectorValues.dimension();
        CalibrationUtils.SampledData sampled = CalibrationUtils.sampleData(floatVectorValues);
        int[] queryOrdinals = sampled.queryOrdinals();
        int[] corpusOrdinals = sampled.corpusOrdinals();

        boolean cosine = similarityFunction == VectorSimilarityFunction.COSINE;
        boolean neyshabur = CalibrationUtils.needsNeyshaburSrebroLift(similarityFunction);

        int dimWork = dim;
        FloatVectorValues fvvForCalibration = floatVectorValues;
        if (neyshabur) {
            double maxNormSq = CalibrationUtils.maxSquaredNormOverCorpusSample(floatVectorValues, corpusOrdinals);
            fvvForCalibration = new CalibrationUtils.NeyshaburLiftedSource<>(floatVectorValues, dim, maxNormSq).asFloatVectorValues();
            dimWork = dim + 1;
        }

        Preconditioner calibrationPreconditioner = Preconditioner.createPreconditioner(dimWork, blockDimension);

        return new CalibrationContext(
            dim,
            dimWork,
            numVectors,
            cosine,
            neyshabur,
            queryOrdinals,
            calibrationPreconditioner,
            corpusOrdinals,
            fvvForCalibration
        );
    }

    private SweepOutcome runCalibrationPipeline(CalibrationContext ctx, VectorSimilarityFunction similarityFunction, CalibrationMode mode)
        throws IOException {
        CalibrationSource calibrationSource = new CalibrationSource(
            similarityFunction,
            ctx.dimWork(),
            ctx.fvvForCalibration(),
            ctx.queryOrdinals(),
            ctx.dim(),
            ctx.cosine(),
            ctx.neyshabur(),
            ctx.calibrationPreconditioner(),
            ctx.corpusOrdinals(),
            k
        );
        double[] manifold = ManifoldModel.estimateManifoldParameters(calibrationSource);
        double alpha = manifold[0];
        double invDim = manifold[1];

        if (mode == CalibrationMode.FAST) {
            return sweepQuantizationCandidatesManifoldResiduals(similarityFunction, ctx.numVectors(), alpha, invDim, calibrationSource);
        } else {
            ErrorScalingFit scalingFit = ErrorModel.estimateErrorScalingFit(calibrationSource, vectorsPerCluster);
            return sweepQuantizationCandidates(similarityFunction, ctx.numVectors(), alpha, invDim, scalingFit, calibrationSource);
        }
    }

    /**
     * Sweeps (encoding, rerank) candidates using synthetic Gaussian residuals scaled to the
     * manifold cluster radius: no k-means, no NN assignment. The slope {@code beta1 = invDim}
     * is taken directly from the manifold; only the intercept {@code beta0} is measured via a
     * single OSQ pass on random residuals. This is the cheapest calibration path.
     * <p>
     * TODO: the synthetic Gaussian residuals (and their squared norms) depend only on the manifold
     * cluster radius, which is constant across candidates in a single sweep — only the OSQ
     * quantization varies with (qbits, dbits). Generating them once and reusing across all candidates
     * would avoid regenerating {@code nDocs * dim} Gaussians per candidate. See also the sort in
     * {@code ErrorModel#quantizedRepErrorStd} which fully sorts to take only the top-5k.
     */
    private SweepOutcome sweepQuantizationCandidatesManifoldResiduals(
        VectorSimilarityFunction similarityFunction,
        int numVectors,
        double alpha,
        double invDim,
        CalibrationSource calibrationSource
    ) throws IOException {
        Map<EncKey, QuantizationErrorStdModel> errorModelCache = new HashMap<>();
        return sweepCandidates(similarityFunction, numVectors, alpha, invDim, (candidate, precondition) -> {
            EncKey key = new EncKey(candidate.qbits(), candidate.dbits(), precondition);
            QuantizationErrorStdModel errorModel = errorModelCache.get(key);
            if (errorModel == null) {
                errorModel = ErrorModel.estimateMagnitudeFromManifoldResiduals(
                    alpha,
                    invDim,
                    calibrationSource,
                    precondition,
                    candidate.qbits(),
                    candidate.dbits(),
                    vectorsPerCluster,
                    numVectors
                );
                errorModelCache.put(key, errorModel);
            }
            return errorModel.errorStd(vectorsPerCluster, numVectors);
        });
    }

    private record CalibrationContext(
        int dim,
        int dimWork,
        int numVectors,
        boolean cosine,
        boolean neyshabur,
        int[] queryOrdinals,
        Preconditioner calibrationPreconditioner,
        int[] corpusOrdinals,
        FloatVectorValues fvvForCalibration
    ) {}

    private static CalibrationSweep[] buildCostOrderedSweeps() {
        List<CalibrationSweep> sweeps = new ArrayList<>();
        for (CandidateEncoding candidate : CANDIDATES) {
            for (double rerankDepth : RERANK_DEPTHS) {
                sweeps.add(new CalibrationSweep(candidate, rerankDepth, calibrationCost(candidate.dbits(), rerankDepth)));
            }
        }
        sweeps.sort(Comparator.comparingDouble(CalibrationSweep::cost).thenComparingInt(s -> s.candidate().qbits()));
        return sweeps.toArray(CalibrationSweep[]::new);
    }

    private static double calibrationCost(int dbits, double rerankDepth) {
        return dbits + RERANK_COST_WEIGHT * rerankDepth;
    }

    private SweepOutcome sweepQuantizationCandidates(
        VectorSimilarityFunction similarityFunction,
        int numVectors,
        double alpha,
        double invDim,
        ErrorScalingFit scalingFit,
        CalibrationSource calibrationSource
    ) throws IOException {
        // Error models depend only on (qbits, dbits, precondition), not on rerank depth, so cache across the sweep.
        Map<EncKey, QuantizationErrorStdModel> errorModelCache = new HashMap<>();
        return sweepCandidates(similarityFunction, numVectors, alpha, invDim, (candidate, precondition) -> {
            EncKey key = new EncKey(candidate.qbits(), candidate.dbits(), precondition);
            QuantizationErrorStdModel errorModel = errorModelCache.computeIfAbsent(
                key,
                ignored -> ErrorModel.estimateMagnitudeModel(
                    scalingFit,
                    calibrationSource,
                    precondition,
                    candidate.qbits(),
                    candidate.dbits(),
                    vectorsPerCluster
                )
            );
            return errorModel.errorStd(vectorsPerCluster, numVectors);
        });
    }

    /**
     * Sweeps every {@code (precondition, encoding, rerank-depth)} candidate in cost order and returns the first
     * configuration whose predicted recall meets {@link #targetRecall}, or the best-effort configuration if none
     * does. The two calibration paths differ only in how the quantization error std is obtained, which is supplied
     * by {@code errorStdProvider}.
     */
    private SweepOutcome sweepCandidates(
        VectorSimilarityFunction similarityFunction,
        int numVectors,
        double alpha,
        double invDim,
        ErrorStdProvider errorStdProvider
    ) throws IOException {
        double bestRecall = -1;
        QuantEncoding bestEncoding = QuantEncoding.ONE_BIT_4BIT_QUERY;
        float bestOversample = DEFAULT_CALIBRATED_OVERSAMPLE;
        boolean bestPrecondition = false;

        boolean[] preconditionValues = new boolean[] { false, true };

        for (boolean precondition : preconditionValues) {
            for (CalibrationSweep sweep : COST_ORDERED_SWEEPS) {
                CandidateEncoding candidate = sweep.candidate();
                double errorStd = errorStdProvider.errorStd(candidate, precondition);
                int rerankVal = ExpectedRecall.rerankN(k, sweep.rerankDepth());
                float oversample = (float) sweep.rerankDepth();
                double expected = ExpectedRecall.expectedRecallAtK(similarityFunction, numVectors, alpha, invDim, errorStd, k, rerankVal);

                logger.debug(
                    () -> format(
                        "Quantization recall((%d, %d) | %d, %s) = %.2f%%",
                        candidate.qbits(),
                        candidate.dbits(),
                        rerankVal,
                        precondition ? "precondition" : "no precondition",
                        expected * 100.0
                    )
                );

                if (expected >= targetRecall) {
                    IvfSegmentConfig config = new IvfSegmentConfig(
                        CentroidIndexFormat.FLAT,
                        candidate.encoding(),
                        precondition,
                        oversample
                    );
                    return new SweepOutcome.Success(config, expected, candidate.qbits(), candidate.dbits(), rerankVal);
                }
                if (expected > bestRecall) {
                    bestRecall = expected;
                    bestEncoding = candidate.encoding();
                    bestOversample = oversample;
                    bestPrecondition = precondition;
                }
            }
        }

        return new SweepOutcome.BestEffort(
            new IvfSegmentConfig(CentroidIndexFormat.FLAT, bestEncoding, bestPrecondition, bestOversample),
            bestRecall
        );
    }

    /** Supplies the quantization error std for a given candidate encoding and precondition setting. */
    @FunctionalInterface
    private interface ErrorStdProvider {
        double errorStd(CandidateEncoding candidate, boolean precondition) throws IOException;
    }

    private record CalibrationSweep(CandidateEncoding candidate, double rerankDepth, double cost) {}

    /**
     * Outcome of the quantization sweep. Either a {@link Success} when some (encoding, rerank) combination
     * met the target recall, or a {@link BestEffort} when no combination did.
     */
    private sealed interface SweepOutcome {
        IvfSegmentConfig config();

        /** Target recall was met; records the winning configuration and its estimated recall. */
        record Success(IvfSegmentConfig config, double expectedRecall, int qbits, int dbits, int rerankN) implements SweepOutcome {}

        /** No configuration met target recall; records the best config found and its recall. */
        record BestEffort(IvfSegmentConfig config, double bestRecall) implements SweepOutcome {}
    }

    private record CandidateEncoding(QuantEncoding encoding, int qbits, int dbits) {}

    /** Selects the calibration strategy used by {@link #calibrate(FloatVectorValues, VectorSimilarityFunction, int, CalibrationMode)}. */
    enum CalibrationMode {
        /** Fast path: synthetic manifold residuals — zero k-means, zero NN assignment. */
        FAST,
        /** Full path: k-means, per-cluster NN assignment, and OLS regression. */
        FULL
    }

    private record EncKey(int qbits, int dbits, boolean precondition) {}
}
