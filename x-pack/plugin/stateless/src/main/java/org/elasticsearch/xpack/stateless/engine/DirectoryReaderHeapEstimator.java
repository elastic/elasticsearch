/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.codec.TrackingPostingsInMemoryBytesCodec;
import org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsService;

import java.util.HashMap;
import java.util.Map;

/**
 * Estimates the heap footprint of a {@link SegmentInfos} from commit metadata without opening a reader. Per
 * segment, sums a fixed baseline ({@link StatelessMemoryMetricsService#ADAPTIVE_SEGMENT_MEMORY_OVERHEAD}),
 * the in-memory postings bytes recorded by {@link TrackingPostingsInMemoryBytesCodec}, and the soft-delete
 * {@code FixedBitSet} when the segment has soft deletes.
 */
public final class DirectoryReaderHeapEstimator {

    public static final long PER_SEGMENT_BASELINE_BYTES = StatelessMemoryMetricsService.ADAPTIVE_SEGMENT_MEMORY_OVERHEAD.getBytes();

    private DirectoryReaderHeapEstimator() {}

    public static long estimate(SegmentInfos infos) {
        long total = 0L;
        for (SegmentCommitInfo sci : infos) {
            total += segmentBytes(sci);
        }
        return total;
    }

    /**
     * Incremental heap bytes for upgrading a reader on {@code current} to one on {@code next}. Shared segments
     * with the same doc-values generation contribute nothing; new segments contribute their full
     * {@link #segmentBytes}; segments with a bumped doc-values generation contribute only the bitset.
     */
    public static long delta(SegmentInfos current, SegmentInfos next) {
        Map<String, SegmentCommitInfo> currentByName = new HashMap<>();
        for (SegmentCommitInfo sci : current) {
            currentByName.put(sci.info.name, sci);
        }
        long delta = 0L;
        for (SegmentCommitInfo sci : next) {
            SegmentCommitInfo currentSci = currentByName.get(sci.info.name);
            if (currentSci == null) {
                delta += segmentBytes(sci);
            } else if (sci.getDocValuesGen() != currentSci.getDocValuesGen()) {
                // Same segment files but a new doc-values generation produces a fresh SegmentReader with a
                // fresh FixedBitSet; baseline and postings are shared with the previous reader.
                delta += softDeleteBitsetBytes(sci);
            }
        }
        return delta;
    }

    public static long segmentBytes(SegmentCommitInfo sci) {
        long bytes = PER_SEGMENT_BASELINE_BYTES;
        bytes += postingsBytes(sci);
        if (sci.getSoftDelCount() > 0) {
            bytes += softDeleteBitsetBytes(sci);
        }
        return bytes;
    }

    public static long postingsBytes(SegmentCommitInfo sci) {
        String attr = sci.info.getAttribute(TrackingPostingsInMemoryBytesCodec.IN_MEMORY_POSTINGS_BYTES_KEY);
        if (attr == null) {
            return 0L;
        }
        try {
            return Long.parseLong(attr);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    public static long softDeleteBitsetBytes(SegmentCommitInfo sci) {
        // FixedBitSet uses ceil(maxDoc / 64) longs; ignore the small fixed object header for this estimate.
        return ((((long) sci.info.maxDoc()) + 63) >>> 6) << 3;
    }

    public static ByteSizeValue estimateAsByteSize(SegmentInfos infos) {
        return ByteSizeValue.ofBytes(estimate(infos));
    }
}
