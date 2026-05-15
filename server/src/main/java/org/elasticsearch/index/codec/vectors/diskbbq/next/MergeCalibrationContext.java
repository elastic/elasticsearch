/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.index.MergeState;

import java.util.Map;
import java.util.Objects;

/**
 * Summarizes Lucene merge metadata used for diskbbq auto-calibration decisions and logging.
 * <p>
 * Bounded (force-merge) merges are detected via {@code mergeMaxNumSegments} in the merged segment's
 * diagnostics (see Lucene {@code IndexWriter}): background merges use {@code -1} (unbounded); force
 * merges set a target segment count ({@code >= 1}).
 */
public record MergeCalibrationContext(int inputSegments, Integer mergeMaxNumSegments, boolean boundedForceMerge) {

    /**
     * Builds context from a merge in progress. Safe when {@link MergeState#segmentInfo} or diagnostics are absent.
     */
    public static MergeCalibrationContext from(MergeState mergeState) {
        Objects.requireNonNull(mergeState);
        int inputSegments = mergeState.knnVectorsReaders == null ? 0 : mergeState.knnVectorsReaders.length;
        Integer mergeMaxNumSegments = null;
        if (mergeState.segmentInfo != null) {
            Map<String, String> diagnostics = mergeState.segmentInfo.getDiagnostics();
            if (diagnostics != null) {
                String raw = diagnostics.get("mergeMaxNumSegments");
                if (raw != null) {
                    try {
                        mergeMaxNumSegments = Integer.parseInt(raw);
                    } catch (NumberFormatException e) {
                        // leave null; treat as unknown background merge for branching
                    }
                }
            }
        }
        boolean bounded = mergeMaxNumSegments != null && mergeMaxNumSegments >= 1;
        return new MergeCalibrationContext(inputSegments, mergeMaxNumSegments, bounded);
    }

    /** {@code force} when this is a bounded (force-merge) merge; {@code background} otherwise. */
    public String mergeKind() {
        return boundedForceMerge ? "force" : "background";
    }

    /** For logs when the diagnostic is absent or unparsed. */
    public String mergeMaxNumSegmentsForLog() {
        return mergeMaxNumSegments == null ? "n/a" : mergeMaxNumSegments.toString();
    }
}
