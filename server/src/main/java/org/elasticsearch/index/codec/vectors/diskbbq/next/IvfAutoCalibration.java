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
import org.apache.lucene.index.MergeState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

/**
 * Resolves a {@link IvfSegmentConfig} on <strong>merge</strong> when {@code auto_calibrate} is enabled: reuses
 * persisted segment metadata when possible, otherwise returns the codec default. Full statistical calibration
 * is deferred to a follow-up change.
 * {@link #resolve} requires a non-null {@link MergeState}. Segments with fewer than
 * {@link #MIN_VECTORS_FOR_CALIBRATION} merged vectors get {@link #DEFAULT_CALIBRATED_OVERSAMPLE}.
 */
public class IvfAutoCalibration {

    private static final Logger logger = LogManager.getLogger(IvfAutoCalibration.class);

    /**
     * Default oversample used when the segment is too small for calibration.
     */
    public static final float DEFAULT_CALIBRATED_OVERSAMPLE = 3f;

    public static final int MIN_VECTORS_FOR_CALIBRATION = 10_000;

    /**
     * If the merged segment is more than this factor larger than the largest input segment,
     * re-run calibration because prior metadata may not extrapolate well.
     */
    static final double RECALIBRATE_GROWTH_RATIO = 4.0;

    /**
     * Minimum fraction of total docs that must agree on a single encoding to skip re-calibration
     * when input segments disagree.
     */
    static final double ENCODING_AGREEMENT_THRESHOLD = 0.8;

    /**
     * Returns an {@link IvfMergeConfigResolver} that runs merge-time auto-calibration for the given cluster size.
     */
    public static IvfMergeConfigResolver mergeConfigResolver(int vectorsPerCluster) {
        return (fieldInfo, mergeState, codecDefault) -> new IvfAutoCalibration().resolve(fieldInfo, mergeState, codecDefault);
    }

    /**
     * On merge, attempts to reuse quantization metadata from input segments via {@link #selectFromMergeState}.
     * When reuse is not possible, returns {@code codecDefault}. Bounded (force-merge) merges skip metadata reuse.
     */
    public IvfSegmentConfig resolve(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig codecDefault) throws IOException {
        Objects.requireNonNull(mergeState, "mergeState");
        Objects.requireNonNull(codecDefault, "codecDefault");
        int numVectors = MergeCalibrationSampleVectors.countMergedVectors(fieldInfo, mergeState);

        if (numVectors < MIN_VECTORS_FOR_CALIBRATION) {
            return new IvfSegmentConfig(ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY, false, DEFAULT_CALIBRATED_OVERSAMPLE);
        }

        MergeCalibrationContext mergeCtx = MergeCalibrationContext.from(mergeState);
        if (mergeCtx.boundedForceMerge() == false) {
            IvfSegmentConfig reused = selectFromMergeState(fieldInfo, mergeState, mergeCtx, numVectors);
            if (reused != null) {
                return reused;
            }
        } else {
            logger.debug(
                "Merge calibration: bounded force merge (mergeMaxNumSegments=[{}], inputSegments=[{}]), skipping metadata reuse",
                mergeCtx.mergeMaxNumSegmentsForLog(),
                mergeCtx.inputSegments()
            );
        }

        logger.debug("Merge calibration reuse not possible, using codec default");
        return codecDefault;
    }

    /**
     * Attempts to reuse calibration results from the input segments being merged.
     * Returns a merged {@link IvfSegmentConfig} if the data has not changed significantly,
     * or {@code null} if merge-time calibration should be performed.
     */
    IvfSegmentConfig selectFromMergeState(
        FieldInfo fieldInfo,
        MergeState mergeState,
        MergeCalibrationContext mergeCtx,
        long mergedVectorCount
    ) {
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
                long docs = mergeState.liveDocs[i].length();
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
            }
        }

        if (calibratedSegments == 0) {
            return null;
        }

        if (mergedVectorCount > RECALIBRATE_GROWTH_RATIO * largestSegmentDocs) {
            logger.debug(
                "Merge calibration: growth ratio [{}] exceeds threshold [{}], "
                    + "re-calibrating [inputSegments={} mergeKind={} mergeMaxNumSegments={}]",
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
                logger.debug(
                    "Merge calibration: encoding disagreement (max encoding covers [{}]% of docs), "
                        + "re-calibrating [inputSegments={} mergeKind={} mergeMaxNumSegments={}]",
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
            "Merge calibration: reusing encoding [{}] (oversample={}, precondition={}) from [{}] "
                + "input segments [inputSegments={} mergeKind={} mergeMaxNumSegments={}]",
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
}
