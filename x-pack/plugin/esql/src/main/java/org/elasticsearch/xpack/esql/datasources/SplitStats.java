/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Compact representation of per-split (row-group/stripe) statistics with path-segment
 * dictionary compression. Replaces raw {@code Map<String, Object>} for wire efficiency:
 * column names are stored as ordinal references into a shared segment dictionary, and
 * per-column stats use parallel arrays instead of string-keyed map entries.
 * <p>
 * Use {@link Builder} to construct instances from format reader metadata, and
 * {@link #toMap()} for backward-compatible access by consumers that expect the flat
 * {@code _stats.*} key convention from {@link SourceStatisticsSerializer}.
 * <p>
 * Instances are effectively immutable after construction and safe for use within a single
 * request processing thread.
 */
public final class SplitStats implements Writeable {

    /** Empty stats with zero rows and no columns. */
    public static final SplitStats EMPTY = new SplitStats(
        0,
        -1,
        new String[0],
        new int[0][],
        new long[0],
        new Object[0],
        new Object[0],
        new long[0]
    );

    private final long rowCount;
    private final long sizeInBytes;
    private final String[] segments;
    private final int[][] columnSegmentOrdinals;
    private final long[] nullCounts;
    private final Object[] mins;
    private final Object[] maxs;
    private final long[] sizesBytes;

    SplitStats(
        long rowCount,
        long sizeInBytes,
        String[] segments,
        int[][] columnSegmentOrdinals,
        long[] nullCounts,
        Object[] mins,
        Object[] maxs,
        long[] sizesBytes
    ) {
        this.rowCount = rowCount;
        this.sizeInBytes = sizeInBytes;
        this.segments = segments;
        this.columnSegmentOrdinals = columnSegmentOrdinals;
        this.nullCounts = nullCounts;
        this.mins = mins;
        this.maxs = maxs;
        this.sizesBytes = sizesBytes;
    }

    public SplitStats(StreamInput in) throws IOException {
        this.rowCount = in.readVLong();
        this.sizeInBytes = in.readZLong();
        int segCount = in.readVInt();
        this.segments = new String[segCount];
        for (int i = 0; i < segCount; i++) {
            segments[i] = in.readString();
        }
        int colCount = in.readVInt();
        this.columnSegmentOrdinals = new int[colCount][];
        this.nullCounts = new long[colCount];
        this.mins = new Object[colCount];
        this.maxs = new Object[colCount];
        this.sizesBytes = new long[colCount];
        for (int i = 0; i < colCount; i++) {
            int segLen = in.readVInt();
            columnSegmentOrdinals[i] = new int[segLen];
            for (int j = 0; j < segLen; j++) {
                columnSegmentOrdinals[i][j] = in.readVInt();
            }
            nullCounts[i] = in.readZLong();
            if (in.readBoolean()) {
                mins[i] = in.readGenericValue();
            }
            if (in.readBoolean()) {
                maxs[i] = in.readGenericValue();
            }
            sizesBytes[i] = in.readZLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(rowCount);
        out.writeZLong(sizeInBytes);
        out.writeVInt(segments.length);
        for (String seg : segments) {
            out.writeString(seg);
        }
        out.writeVInt(columnCount());
        for (int i = 0; i < columnCount(); i++) {
            int[] segs = columnSegmentOrdinals[i];
            out.writeVInt(segs.length);
            for (int ord : segs) {
                out.writeVInt(ord);
            }
            out.writeZLong(nullCounts[i]);
            if (mins[i] != null) {
                out.writeBoolean(true);
                out.writeGenericValue(mins[i]);
            } else {
                out.writeBoolean(false);
            }
            if (maxs[i] != null) {
                out.writeBoolean(true);
                out.writeGenericValue(maxs[i]);
            } else {
                out.writeBoolean(false);
            }
            out.writeZLong(sizesBytes[i]);
        }
    }

    public int columnCount() {
        return columnSegmentOrdinals.length;
    }

    public long rowCount() {
        return rowCount;
    }

    /** Returns size in bytes, or {@code -1} if unknown. */
    public long sizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Resolves the full dotted column name for the given ordinal by joining
     * the path segments from the dictionary. Each call allocates a new String;
     * callers that iterate over columns should prefer {@link #resolveColumnName(int, StringBuilder)}
     * to reuse a buffer.
     */
    public String columnName(int ordinal) {
        Objects.checkIndex(ordinal, columnCount());
        int[] segs = columnSegmentOrdinals[ordinal];
        if (segs.length == 1) {
            return segments[segs[0]];
        }
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < segs.length; j++) {
            if (j > 0) sb.append('.');
            sb.append(segments[segs[j]]);
        }
        return sb.toString();
    }

    /**
     * Resolves the full dotted column name into the given {@link StringBuilder}, which is
     * cleared before use. Avoids per-call String allocation when iterating over columns.
     */
    String resolveColumnName(int ordinal, StringBuilder sb) {
        Objects.checkIndex(ordinal, columnCount());
        sb.setLength(0);
        int[] segs = columnSegmentOrdinals[ordinal];
        for (int j = 0; j < segs.length; j++) {
            if (j > 0) sb.append('.');
            sb.append(segments[segs[j]]);
        }
        return sb.toString();
    }

    /** Returns null count for the column, or {@code -1} if unknown. */
    public long nullCount(int col) {
        return nullCounts[col];
    }

    /** Returns the min value for the column, or {@code null} if unknown. */
    @Nullable
    public Object min(int col) {
        return mins[col];
    }

    /** Returns the max value for the column, or {@code null} if unknown. */
    @Nullable
    public Object max(int col) {
        return maxs[col];
    }

    /** Returns size in bytes for the column, or {@code -1} if unknown. */
    public long sizeBytes(int col) {
        return sizesBytes[col];
    }

    /**
     * Finds a column ordinal by its full dotted name. Returns {@code -1} if not found.
     * Matches directly against the segment dictionary without materializing column name
     * strings, keeping the compact representation intact.
     */
    public int findColumn(String name) {
        int nameLen = name.length();
        for (int i = 0; i < columnCount(); i++) {
            int[] segs = columnSegmentOrdinals[i];
            int pos = 0;
            boolean match = true;
            for (int j = 0; j < segs.length; j++) {
                if (j > 0) {
                    if (pos >= nameLen || name.charAt(pos) != '.') {
                        match = false;
                        break;
                    }
                    pos++;
                }
                String seg = segments[segs[j]];
                int segLen = seg.length();
                if (pos + segLen > nameLen || name.regionMatches(pos, seg, 0, segLen) == false) {
                    match = false;
                    break;
                }
                pos += segLen;
            }
            if (match && pos == nameLen) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the null count for the column with the given name, or {@code -1} if the column
     * is not found or its null count is unknown.
     */
    public long columnNullCount(String name) {
        int col = findColumn(name);
        return col >= 0 ? nullCounts[col] : -1;
    }

    /** Returns the min value for the column with the given name, or {@code null} if not found. */
    @Nullable
    public Object columnMin(String name) {
        int col = findColumn(name);
        return col >= 0 ? mins[col] : null;
    }

    /** Returns the max value for the column with the given name, or {@code null} if not found. */
    @Nullable
    public Object columnMax(String name) {
        int col = findColumn(name);
        return col >= 0 ? maxs[col] : null;
    }

    /** Returns the size in bytes for the column with the given name, or {@code -1} if not found. */
    public long columnSizeBytes(String name) {
        int col = findColumn(name);
        return col >= 0 ? sizesBytes[col] : -1;
    }

    /**
     * Merges multiple {@link SplitStats} instances into a single one. Row counts and column
     * sizes are summed, null counts are summed, mins take the minimum, maxes take the maximum.
     * Columns present in some splits but not others are included in the result with partial
     * data from the splits where they appear.
     *
     * @return the merged stats, or {@code null} if the list is null or empty
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Nullable
    public static SplitStats merge(List<SplitStats> statsList) {
        if (statsList == null || statsList.isEmpty()) {
            return null;
        }
        if (statsList.size() == 1) {
            return statsList.getFirst();
        }
        Builder builder = new Builder();
        long totalRows = 0;
        long totalSize = 0;
        boolean hasSize = false;
        Map<String, Integer> columnOrdinals = new LinkedHashMap<>();
        StringBuilder nameBuf = new StringBuilder();

        for (SplitStats stats : statsList) {
            totalRows += stats.rowCount;
            if (stats.sizeInBytes >= 0) {
                totalSize += stats.sizeInBytes;
                hasSize = true;
            }
            for (int i = 0; i < stats.columnCount(); i++) {
                String colName = stats.resolveColumnName(i, nameBuf);
                Integer existingOrd = columnOrdinals.get(colName);
                if (existingOrd == null) {
                    int ord = builder.addColumn(colName);
                    columnOrdinals.put(colName, ord);
                    builder.nullCount(ord, stats.nullCounts[i]);
                    builder.min(ord, stats.mins[i]);
                    builder.max(ord, stats.maxs[i]);
                    builder.sizeBytes(ord, stats.sizesBytes[i]);
                } else {
                    int ord = existingOrd;
                    // Sum null counts
                    long existingNc = builder.nullCountsList.get(ord);
                    long newNc = stats.nullCounts[i];
                    if (existingNc >= 0 && newNc >= 0) {
                        builder.nullCount(ord, existingNc + newNc);
                    }
                    // Min of mins (only compare values of the same type)
                    Object existingMin = builder.minsList.get(ord);
                    Object newMin = stats.mins[i];
                    if (existingMin != null && newMin != null) {
                        if (existingMin instanceof Comparable eMin
                            && newMin instanceof Comparable nMin
                            && existingMin.getClass() == newMin.getClass()) {
                            builder.min(ord, eMin.compareTo(nMin) <= 0 ? existingMin : newMin);
                        }
                    } else if (newMin != null) {
                        builder.min(ord, newMin);
                    }
                    // Max of maxes (only compare values of the same type)
                    Object existingMax = builder.maxsList.get(ord);
                    Object newMax = stats.maxs[i];
                    if (existingMax != null && newMax != null) {
                        if (existingMax instanceof Comparable eMax
                            && newMax instanceof Comparable nMax
                            && existingMax.getClass() == newMax.getClass()) {
                            builder.max(ord, eMax.compareTo(nMax) >= 0 ? existingMax : newMax);
                        }
                    } else if (newMax != null) {
                        builder.max(ord, newMax);
                    }
                    // Sum sizes
                    long existingSb = builder.sizesBytesList.get(ord);
                    long newSb = stats.sizesBytes[i];
                    if (existingSb >= 0 && newSb >= 0) {
                        builder.sizeBytes(ord, existingSb + newSb);
                    }
                }
            }
        }

        builder.rowCount(totalRows);
        if (hasSize) {
            builder.sizeInBytes(totalSize);
        }
        return builder.build();
    }

    /**
     * Produces a flat {@code Map<String, Object>} using the same {@code _stats.*} key
     * conventions as {@link SourceStatisticsSerializer}, for backward compatibility with
     * all existing consumers.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
        if (sizeInBytes >= 0) {
            map.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, sizeInBytes);
        }
        StringBuilder nameBuf = new StringBuilder();
        for (int i = 0; i < columnCount(); i++) {
            String name = resolveColumnName(i, nameBuf);
            if (nullCounts[i] >= 0) {
                map.put(SourceStatisticsSerializer.columnNullCountKey(name), nullCounts[i]);
            }
            if (mins[i] != null) {
                map.put(SourceStatisticsSerializer.columnMinKey(name), mins[i]);
            }
            if (maxs[i] != null) {
                map.put(SourceStatisticsSerializer.columnMaxKey(name), maxs[i]);
            }
            if (sizesBytes[i] >= 0) {
                map.put(SourceStatisticsSerializer.columnSizeBytesKey(name), sizesBytes[i]);
            }
        }
        return Map.copyOf(map);
    }

    /**
     * Parses a flat {@code _stats.*} map (old format) into a {@code SplitStats} instance.
     * Returns {@code null} if the map is null, empty, or missing the required row count key.
     * <p>
     * Column ordering in the result depends on the input map's iteration order: stable
     * (e.g. {@link LinkedHashMap}) inputs produce stable ordinals. The {@link #toMap()}
     * output is always key-equivalent regardless of column ordering.
     */
    @Nullable
    public static SplitStats of(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        Object rc = map.get(SourceStatisticsSerializer.STATS_ROW_COUNT);
        if (rc instanceof Number == false) {
            return null;
        }
        Builder builder = new Builder();
        builder.rowCount(((Number) rc).longValue());
        Object sb = map.get(SourceStatisticsSerializer.STATS_SIZE_BYTES);
        if (sb instanceof Number) {
            builder.sizeInBytes(((Number) sb).longValue());
        }

        // Group column keys by column name
        Map<String, Map<String, Object>> columnKeys = new LinkedHashMap<>();
        String prefix = "_stats.columns.";
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix) == false) {
                continue;
            }
            String rest = key.substring(prefix.length());
            int dotIdx = rest.lastIndexOf('.');
            if (dotIdx <= 0) {
                continue;
            }
            String colName = rest.substring(0, dotIdx);
            String suffix = rest.substring(dotIdx);
            columnKeys.computeIfAbsent(colName, k -> new HashMap<>()).put(suffix, entry.getValue());
        }

        for (Map.Entry<String, Map<String, Object>> colEntry : columnKeys.entrySet()) {
            String colName = colEntry.getKey();
            Map<String, Object> stats = colEntry.getValue();
            int ordinal = builder.addColumn(colName);

            Object nc = stats.get(".null_count");
            if (nc instanceof Number) {
                builder.nullCount(ordinal, ((Number) nc).longValue());
            }
            Object min = stats.get(".min");
            if (min != null) {
                builder.min(ordinal, min);
            }
            Object max = stats.get(".max");
            if (max != null) {
                builder.max(ordinal, max);
            }
            Object csb = stats.get(".size_bytes");
            if (csb instanceof Number) {
                builder.sizeBytes(ordinal, ((Number) csb).longValue());
            }
        }

        return builder.build();
    }

    /**
     * Resolves the effective {@link SplitStats} for a set of splits. For single-split queries
     * the per-split stats (if available) or the sourceMetadata map is used; for multi-split
     * queries the per-split stats are merged. Returns {@code null} if any split lacks stats.
     */
    @Nullable
    public static SplitStats resolveEffectiveStats(List<? extends ExternalSplit> splits, Map<String, Object> sourceMetadata) {
        if (splits.size() <= 1) {
            if (splits.size() == 1 && splits.getFirst() instanceof FileSplit fs && fs.splitStats() != null) {
                return fs.splitStats();
            }
            return of(sourceMetadata);
        }
        List<SplitStats> perSplit = new ArrayList<>(splits.size());
        for (ExternalSplit split : splits) {
            if (split instanceof FileSplit fs && fs.splitStats() != null) {
                perSplit.add(fs.splitStats());
            } else {
                return null;
            }
        }
        return merge(perSplit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SplitStats that = (SplitStats) o;
        return rowCount == that.rowCount
            && sizeInBytes == that.sizeInBytes
            && Arrays.equals(segments, that.segments)
            && Arrays.deepEquals(columnSegmentOrdinals, that.columnSegmentOrdinals)
            && Arrays.equals(nullCounts, that.nullCounts)
            && Arrays.deepEquals(mins, that.mins)
            && Arrays.deepEquals(maxs, that.maxs)
            && Arrays.equals(sizesBytes, that.sizesBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(rowCount, sizeInBytes);
        result = 31 * result + Arrays.hashCode(segments);
        result = 31 * result + Arrays.deepHashCode(columnSegmentOrdinals);
        result = 31 * result + Arrays.hashCode(nullCounts);
        result = 31 * result + Arrays.deepHashCode(mins);
        result = 31 * result + Arrays.deepHashCode(maxs);
        result = 31 * result + Arrays.hashCode(sizesBytes);
        return result;
    }

    @Override
    public String toString() {
        return "SplitStats[rowCount=" + rowCount + ", columns=" + columnCount() + "]";
    }

    /**
     * Builder for constructing {@link SplitStats} instances. Columns are added via
     * {@link #addColumn(String)} which returns an ordinal for subsequent stat calls.
     * Path segments are deduplicated automatically.
     */
    public static final class Builder {
        private long rowCount = -1;
        private long sizeInBytes = -1;
        private final List<int[]> columnOrdinals = new ArrayList<>();
        private final List<Long> nullCountsList = new ArrayList<>();
        private final List<Object> minsList = new ArrayList<>();
        private final List<Object> maxsList = new ArrayList<>();
        private final List<Long> sizesBytesList = new ArrayList<>();
        private final Map<String, Integer> segmentIndex = new LinkedHashMap<>();
        private final Map<String, Integer> columnNameToOrdinal = new LinkedHashMap<>();

        public Builder rowCount(long rowCount) {
            this.rowCount = rowCount;
            return this;
        }

        public Builder sizeInBytes(long sizeInBytes) {
            this.sizeInBytes = sizeInBytes;
            return this;
        }

        /**
         * Adds a column and returns its ordinal. The column name is split on '.'
         * and each segment is deduplicated in the path-segment dictionary.
         *
         * @throws IllegalArgumentException if a column with the same name was already added
         */
        public int addColumn(String name) {
            if (columnNameToOrdinal.containsKey(name)) {
                throw new IllegalArgumentException("duplicate column name: " + name);
            }
            int ordinal = columnOrdinals.size();
            columnNameToOrdinal.put(name, ordinal);
            String[] parts = name.split("\\.", -1);
            int[] ords = new int[parts.length];
            for (int i = 0; i < parts.length; i++) {
                ords[i] = segmentIndex.computeIfAbsent(parts[i], k -> segmentIndex.size());
            }
            columnOrdinals.add(ords);
            nullCountsList.add(-1L);
            minsList.add(null);
            maxsList.add(null);
            sizesBytesList.add(-1L);
            return ordinal;
        }

        /**
         * Adds a column with all stats in a single call and returns its ordinal.
         * Use {@code -1} for unknown longs and {@code null} for unknown objects.
         *
         * @throws IllegalArgumentException if a column with the same name was already added
         */
        public int addColumn(String name, long nullCount, @Nullable Object min, @Nullable Object max, long sizeBytes) {
            int ordinal = addColumn(name);
            nullCountsList.set(ordinal, nullCount);
            minsList.set(ordinal, min);
            maxsList.set(ordinal, max);
            sizesBytesList.set(ordinal, sizeBytes);
            return ordinal;
        }

        public Builder nullCount(int columnOrdinal, long nullCount) {
            nullCountsList.set(columnOrdinal, nullCount);
            return this;
        }

        public Builder min(int columnOrdinal, Object min) {
            minsList.set(columnOrdinal, min);
            return this;
        }

        public Builder max(int columnOrdinal, Object max) {
            maxsList.set(columnOrdinal, max);
            return this;
        }

        public Builder sizeBytes(int columnOrdinal, long sizeBytes) {
            sizesBytesList.set(columnOrdinal, sizeBytes);
            return this;
        }

        public SplitStats build() {
            if (rowCount < 0) {
                throw new IllegalArgumentException("rowCount must be set (>= 0) before building");
            }
            String[] segs = segmentIndex.keySet().toArray(String[]::new);
            int colCount = columnOrdinals.size();
            int[][] colSegOrdinals = columnOrdinals.toArray(int[][]::new);
            long[] nc = new long[colCount];
            Object[] mn = new Object[colCount];
            Object[] mx = new Object[colCount];
            long[] sb = new long[colCount];
            for (int i = 0; i < colCount; i++) {
                nc[i] = nullCountsList.get(i);
                mn[i] = minsList.get(i);
                mx[i] = maxsList.get(i);
                sb[i] = sizesBytesList.get(i);
            }
            return new SplitStats(rowCount, sizeInBytes, segs, colSegOrdinals, nc, mn, mx, sb);
        }
    }
}
