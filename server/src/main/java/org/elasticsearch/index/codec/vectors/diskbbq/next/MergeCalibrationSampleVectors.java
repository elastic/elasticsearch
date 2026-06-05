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
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Builds a bounded, random-access calibration sample from merge input segments before the merged
 * vector stream is materialized.
 */
public final class MergeCalibrationSampleVectors {

    /** Align with future {@code CalibrationUtils.MAX_QUERY_SAMPLE}. */
    public static final int MAX_QUERY_SAMPLE = 512;

    /** Align with future {@code CalibrationUtils.MAX_CORPUS_SAMPLE}. */
    public static final int MAX_CORPUS_SAMPLE = 16_384;

    public static final int DEFAULT_GLOBAL_CAP = MAX_QUERY_SAMPLE + MAX_CORPUS_SAMPLE;

    private MergeCalibrationSampleVectors() {}

    /**
     * Total live vectors for {@code fieldInfo} across merge inputs.
     */
    public static int countMergedVectors(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        Objects.requireNonNull(fieldInfo, "fieldInfo");
        Objects.requireNonNull(mergeState, "mergeState");
        if (mergeState.knnVectorsReaders == null) {
            return 0;
        }
        int total = 0;
        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            FloatVectorValues segmentVectors = segmentFloatVectorValues(fieldInfo, mergeState, i);
            if (segmentVectors != null) {
                total += segmentVectors.size();
            }
        }
        return total;
    }

    /**
     * Builds a heap-backed {@link FloatVectorValues} sample using {@link #DEFAULT_GLOBAL_CAP}.
     */
    public static FloatVectorValues build(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        return build(fieldInfo, mergeState, DEFAULT_GLOBAL_CAP);
    }

    /**
     * Builds a heap-backed {@link FloatVectorValues} sample capped at {@code globalCap} vectors.
     * Takes the first {@code perSegmentTarget} ordinals from each input segment in order, then
     * backfills from segments with remaining vectors until the target is reached.
     */
    public static FloatVectorValues build(FieldInfo fieldInfo, MergeState mergeState, int globalCap) throws IOException {
        Objects.requireNonNull(fieldInfo, "fieldInfo");
        Objects.requireNonNull(mergeState, "mergeState");
        if (globalCap <= 0) {
            return KMeansFloatVectorValues.build(List.of(), null, fieldInfo.getVectorDimension());
        }

        List<SegmentVectors> segments = loadSegments(fieldInfo, mergeState);
        if (segments.isEmpty()) {
            return KMeansFloatVectorValues.build(List.of(), null, fieldInfo.getVectorDimension());
        }

        int perSegmentTarget = Math.max(1, globalCap / segments.size());
        int targetTotal = Math.min(globalCap, totalAvailable(segments));
        int dim = fieldInfo.getVectorDimension();
        List<float[]> samples = new ArrayList<>(Math.min(targetTotal, globalCap));

        for (SegmentVectors segment : segments) {
            int take = Math.min(perSegmentTarget, segment.fvv.size());
            appendOrdinals(segment, 0, take, dim, samples);
            segment.nextOrd = take;
        }

        int remaining = targetTotal - samples.size();
        while (remaining > 0) {
            boolean progress = false;
            for (SegmentVectors segment : segments) {
                if (segment.nextOrd >= segment.fvv.size()) {
                    continue;
                }
                samples.add(Arrays.copyOf(segment.fvv.vectorValue(segment.nextOrd), dim));
                segment.nextOrd++;
                remaining--;
                progress = true;
                if (remaining == 0) {
                    break;
                }
            }
            if (progress == false) {
                break;
            }
        }

        return KMeansFloatVectorValues.build(samples, null, dim);
    }

    private static int totalAvailable(List<SegmentVectors> segments) {
        int total = 0;
        for (SegmentVectors segment : segments) {
            total += segment.fvv.size();
        }
        return total;
    }

    private static void appendOrdinals(SegmentVectors segment, int startOrd, int endOrd, int dim, List<float[]> samples)
        throws IOException {
        for (int ord = startOrd; ord < endOrd; ord++) {
            samples.add(Arrays.copyOf(segment.fvv.vectorValue(ord), dim));
        }
    }

    private static List<SegmentVectors> loadSegments(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        List<SegmentVectors> segments = new ArrayList<>();
        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            FloatVectorValues segmentVectors = segmentFloatVectorValues(fieldInfo, mergeState, i);
            if (segmentVectors != null && segmentVectors.size() > 0) {
                segments.add(new SegmentVectors(segmentVectors));
            }
        }
        return segments;
    }

    private static FloatVectorValues segmentFloatVectorValues(FieldInfo fieldInfo, MergeState mergeState, int segmentIndex)
        throws IOException {
        if (mergeState.fieldInfos != null) {
            if (mergeState.fieldInfos[segmentIndex].fieldInfo(fieldInfo.name) == null) {
                return null;
            }
        }
        KnnVectorsReader reader = mergeState.knnVectorsReaders[segmentIndex];
        if (reader == null) {
            return null;
        }
        return reader.getFloatVectorValues(fieldInfo.name);
    }

    private static final class SegmentVectors {
        private final FloatVectorValues fvv;
        private int nextOrd;

        private SegmentVectors(FloatVectorValues fvv) {
            this.fvv = fvv;
        }
    }
}
