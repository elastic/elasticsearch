/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * Compact storage of per-file byte ranges using CSR (Compressed Sparse Row) indexing
 * and dictionary encoding for length templates and split statistics.
 * <p>
 * Per-range offsets are stored explicitly (ranges need not be contiguous within a file).
 * Per-range lengths are stored via a length template dictionary: files with identical
 * range-length sequences share the same {@code long[]} instance, reducing memory when
 * many files have the same row-group geometry (common for Parquet files written by the
 * same job).
 * <p>
 * Per-range {@link SplitStats} are deduplicated via a dictionary so that ranges with
 * identical statistics share a single object reference.
 * <p>
 * Returns {@code null} from {@link #build} when ranges contain invalid data (negative
 * offsets, non-positive lengths) — callers fall back to the slow path.
 */
final class CompactRangeStore {

    private static final Logger LOGGER = LogManager.getLogger(CompactRangeStore.class);

    private final int[] fileRangeStarts;         // CSR, length = fileCount + 1
    private final long[][] lengthTemplates;      // unique length sequences
    private final short[] fileLengthTemplateId;  // per-file -> template index
    private final long[] rangeOffsets;            // per-range offset, length = totalRanges
    private final SplitStats[] statsDictionary;  // unique SplitStats objects
    private final short[] rangeStatsId;          // per-range -> stats dict index, length = totalRanges

    private CompactRangeStore(
        int[] fileRangeStarts,
        long[][] lengthTemplates,
        short[] fileLengthTemplateId,
        long[] rangeOffsets,
        SplitStats[] statsDictionary,
        short[] rangeStatsId
    ) {
        this.fileRangeStarts = fileRangeStarts;
        this.lengthTemplates = lengthTemplates;
        this.fileLengthTemplateId = fileLengthTemplateId;
        this.rangeOffsets = rangeOffsets;
        this.statsDictionary = statsDictionary;
        this.rangeStatsId = rangeStatsId;
    }

    int rangeCount(int fileIndex) {
        return fileRangeStarts[fileIndex + 1] - fileRangeStarts[fileIndex];
    }

    long rangeOffset(int fileIndex, int rangeIndex) {
        return rangeOffsets[fileRangeStarts[fileIndex] + rangeIndex];
    }

    long rangeLength(int fileIndex, int rangeIndex) {
        return lengthTemplates[Short.toUnsignedInt(fileLengthTemplateId[fileIndex])][rangeIndex];
    }

    @Nullable
    SplitStats rangeStats(int fileIndex, int rangeIndex) {
        int globalIndex = fileRangeStarts[fileIndex] + rangeIndex;
        short id = rangeStatsId[globalIndex];
        return id < 0 ? null : statsDictionary[id];
    }

    long estimatedBytes() {
        // object header
        long bytes = 64;
        // fileRangeStarts
        bytes += (long) fileRangeStarts.length * Integer.BYTES;
        // lengthTemplates: array of arrays
        bytes += (long) lengthTemplates.length * 16; // array references + headers
        for (long[] template : lengthTemplates) {
            bytes += (long) template.length * Long.BYTES;
        }
        // fileLengthTemplateId
        bytes += (long) fileLengthTemplateId.length * Short.BYTES;
        // rangeOffsets
        bytes += (long) rangeOffsets.length * Long.BYTES;
        // statsDictionary: array of references
        bytes += (long) statsDictionary.length * 8;
        // rangeStatsId
        bytes += (long) rangeStatsId.length * Short.BYTES;
        return bytes;
    }

    /**
     * Builds a compact range store from the given file-to-range mapping.
     * Returns {@code null} if the map is null, empty, or no file has any ranges.
     *
     * @param fileCount    the number of files
     * @param pathAt       function returning the {@link StoragePath} for a file index
     * @param rangesByPath mapping from storage path to split ranges
     * @return the compact store, or {@code null} if there are no ranges
     */
    @Nullable
    static CompactRangeStore build(
        int fileCount,
        IntFunction<StoragePath> pathAt,
        Map<StoragePath, List<RangeAwareFormatReader.SplitRange>> rangesByPath
    ) {
        if (rangesByPath == null || rangesByPath.isEmpty()) {
            return null;
        }

        // Materialize paths once — pathAt.apply() may reconstruct paths from dictionaries
        StoragePath[] paths = new StoragePath[fileCount];
        for (int f = 0; f < fileCount; f++) {
            paths[f] = pathAt.apply(f);
        }

        int[] fileRangeStarts = new int[fileCount + 1];
        short[] fileLengthTemplateId = new short[fileCount];

        Map<LongArrayKey, Short> templateDict = new HashMap<>();
        int templateCount = 0;
        long[][] tempTemplates = new long[16][];

        Map<SplitStats, Short> statsDict = new HashMap<>();
        int statsCount = 0;
        SplitStats[] tempStats = new SplitStats[16];

        int totalRanges = 0;
        boolean hasAnyRange = false;

        for (int f = 0; f < fileCount; f++) {
            List<RangeAwareFormatReader.SplitRange> ranges = rangesByPath.get(paths[f]);
            int count = (ranges != null) ? ranges.size() : 0;
            fileRangeStarts[f] = totalRanges;
            totalRanges += count;
            if (count > 0) {
                hasAnyRange = true;
            }
        }
        fileRangeStarts[fileCount] = totalRanges;

        if (hasAnyRange == false) {
            return null;
        }

        long[] rangeOffsets = new long[totalRanges];
        short[] rangeStatsId = new short[totalRanges];
        Arrays.fill(rangeStatsId, (short) -1);

        for (int f = 0; f < fileCount; f++) {
            StoragePath path = paths[f];
            List<RangeAwareFormatReader.SplitRange> ranges = rangesByPath.get(path);
            int count = (ranges != null) ? ranges.size() : 0;

            if (count == 0) {
                long[] emptyLengths = new long[0];
                LongArrayKey key = new LongArrayKey(emptyLengths);
                Short existingEmpty = templateDict.get(key);
                if (existingEmpty != null) {
                    fileLengthTemplateId[f] = existingEmpty;
                } else {
                    if (templateCount >= 65535) {
                        LOGGER.debug("Template dictionary overflow ({} templates); falling back to slow path", templateCount);
                        return null;
                    }
                    short id = (short) templateCount;
                    if (templateCount >= tempTemplates.length) {
                        tempTemplates = Arrays.copyOf(tempTemplates, tempTemplates.length * 2);
                    }
                    tempTemplates[templateCount++] = emptyLengths;
                    templateDict.put(key, id);
                    fileLengthTemplateId[f] = id;
                }
                continue;
            }

            int globalStart = fileRangeStarts[f];
            long[] lengths = new long[count];
            for (int r = 0; r < count; r++) {
                RangeAwareFormatReader.SplitRange range = ranges.get(r);
                if (range.offset() < 0 || range.length() <= 0) {
                    LOGGER.warn(
                        "Aborting compact range store build: invalid range for [{}] (offset={}, length={}); falling back to slow path",
                        path,
                        range.offset(),
                        range.length()
                    );
                    return null;
                }
                rangeOffsets[globalStart + r] = range.offset();
                lengths[r] = range.length();
            }

            LongArrayKey key = new LongArrayKey(lengths);
            Short existingId = templateDict.get(key);
            if (existingId != null) {
                fileLengthTemplateId[f] = existingId;
            } else {
                if (templateCount > Short.MAX_VALUE) {
                    return null;
                }
                short id = (short) templateCount;
                if (templateCount >= tempTemplates.length) {
                    tempTemplates = Arrays.copyOf(tempTemplates, tempTemplates.length * 2);
                }
                tempTemplates[templateCount++] = lengths;
                templateDict.put(key, id);
                fileLengthTemplateId[f] = id;
            }

            for (int r = 0; r < count; r++) {
                Map<String, Object> statsMap = ranges.get(r).statistics();
                SplitStats stats = SplitStats.of(statsMap);
                if (stats != null) {
                    Short sid = statsDict.get(stats);
                    if (sid != null) {
                        rangeStatsId[globalStart + r] = sid;
                    } else {
                        if (statsCount > Short.MAX_VALUE) {
                            LOGGER.debug("Stats dictionary overflow ({} entries); falling back to slow path", statsCount);
                            return null;
                        }
                        if (statsCount >= tempStats.length) {
                            tempStats = Arrays.copyOf(tempStats, tempStats.length * 2);
                        }
                        tempStats[statsCount] = stats;
                        statsDict.put(stats, (short) statsCount);
                        rangeStatsId[globalStart + r] = (short) statsCount;
                        statsCount++;
                    }
                }
            }
        }

        long[][] lengthTemplates = Arrays.copyOf(tempTemplates, templateCount);
        SplitStats[] statsDictionary = Arrays.copyOf(tempStats, statsCount);

        return new CompactRangeStore(fileRangeStarts, lengthTemplates, fileLengthTemplateId, rangeOffsets, statsDictionary, rangeStatsId);
    }

    /**
     * Wrapper for {@code long[]} that provides value-based {@code equals} and {@code hashCode}
     * using {@link Arrays#equals(long[], long[])} and {@link Arrays#hashCode(long[])}.
     */
    private record LongArrayKey(long[] values) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof LongArrayKey that) {
                return Arrays.equals(values, that.values);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }
}
