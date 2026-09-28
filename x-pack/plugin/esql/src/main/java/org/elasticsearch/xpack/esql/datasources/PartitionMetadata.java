/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Holds partition information detected from file paths.
 * <p>
 * Column names and types live in {@link #partitionColumns()} (insertion-ordered). Per-file values are
 * stored <em>column-wise</em>: one {@code Object[]} per column, indexed by a compact row id. By default
 * each file is its own row ({@code row == fileIndex}). After directory-grouped compaction, many files
 * can share one row via {@link #shareByGroups(short[], int)} so identical Hive tuples are not duplicated.
 * <p>
 * Detectors emit ordinal-aligned metadata (no path index). The legacy path-keyed map constructor keeps a
 * side {@link StoragePath} index so callers that build metadata independently of listing order still
 * resolve the correct values; that path is for tests / compat only and is skipped by group sharing.
 */
public final class PartitionMetadata {

    public static final PartitionMetadata EMPTY = new PartitionMetadata(Map.of(), new Object[0][], 0, 0, null, null);

    private final Map<String, DataType> partitionColumns;
    private final String[] columnNames;
    private final Object[][] valuesByColumn;
    private final int rowCount;
    private final int fileCount;
    /**
     * {@code null} means identity: file {@code i} reads row {@code i}. Otherwise file {@code i} reads
     * row {@code fileToRow[i]} (used when values are shared across directory groups).
     */
    @Nullable
    private final int[] fileToRow;
    /**
     * Present only for the legacy path-keyed constructor. When set, {@link #resolveFileIndex(int, StoragePath)}
     * looks up by path so file ordinals need not match listing order.
     */
    @Nullable
    private final StoragePath[] pathsByFile;

    /**
     * Columnar constructor. {@code valuesByColumn[c].length} must equal {@code rowCount}. When
     * {@code fileToRow} is {@code null}, {@code fileCount} must equal {@code rowCount}.
     */
    public PartitionMetadata(
        Map<String, DataType> partitionColumns,
        Object[][] valuesByColumn,
        int rowCount,
        int fileCount,
        @Nullable int[] fileToRow
    ) {
        this(partitionColumns, valuesByColumn, rowCount, fileCount, fileToRow, null);
    }

    private PartitionMetadata(
        Map<String, DataType> partitionColumns,
        Object[][] valuesByColumn,
        int rowCount,
        int fileCount,
        @Nullable int[] fileToRow,
        @Nullable StoragePath[] pathsByFile
    ) {
        if (partitionColumns == null) {
            throw new IllegalArgumentException("partitionColumns cannot be null");
        }
        if (valuesByColumn == null) {
            throw new IllegalArgumentException("valuesByColumn cannot be null");
        }
        if (rowCount < 0 || fileCount < 0) {
            throw new IllegalArgumentException("rowCount and fileCount must be non-negative");
        }
        this.partitionColumns = unmodifiableOrderedCopy(partitionColumns);
        this.columnNames = this.partitionColumns.keySet().toArray(String[]::new);
        if (valuesByColumn.length != this.columnNames.length) {
            throw new IllegalArgumentException(
                "valuesByColumn length [" + valuesByColumn.length + "] != column count [" + this.columnNames.length + "]"
            );
        }
        for (int c = 0; c < valuesByColumn.length; c++) {
            if (valuesByColumn[c] == null || valuesByColumn[c].length != rowCount) {
                throw new IllegalArgumentException("valuesByColumn[" + c + "] must be non-null and length rowCount [" + rowCount + "]");
            }
        }
        if (fileToRow == null) {
            if (fileCount != rowCount) {
                throw new IllegalArgumentException("fileCount must equal rowCount when fileToRow is null");
            }
            this.fileToRow = null;
        } else {
            if (fileToRow.length != fileCount) {
                throw new IllegalArgumentException("fileToRow length must equal fileCount");
            }
            for (int f = 0; f < fileCount; f++) {
                int row = fileToRow[f];
                if (row < 0 || row >= rowCount) {
                    throw new IllegalArgumentException("fileToRow[" + f + "]=" + row + " out of range for rowCount=" + rowCount);
                }
            }
            this.fileToRow = fileToRow.clone();
        }
        if (pathsByFile != null && pathsByFile.length != fileCount) {
            throw new IllegalArgumentException("pathsByFile length must equal fileCount");
        }
        this.valuesByColumn = copyColumns(valuesByColumn, rowCount);
        this.rowCount = rowCount;
        this.fileCount = fileCount;
        this.pathsByFile = pathsByFile == null ? null : pathsByFile.clone();
    }

    private static Object[][] copyColumns(Object[][] valuesByColumn, int rowCount) {
        Object[][] copy = new Object[valuesByColumn.length][];
        for (int c = 0; c < valuesByColumn.length; c++) {
            copy[c] = valuesByColumn[c].clone();
            assert copy[c].length == rowCount;
        }
        return copy;
    }

    /**
     * Builds columnar metadata from the legacy path-keyed map. Keeps a path index so values resolve by
     * {@link StoragePath} regardless of map iteration order. Missing column entries become {@code null}.
     */
    public PartitionMetadata(Map<String, DataType> partitionColumns, Map<StoragePath, Map<String, Object>> filePartitionValues) {
        this(fromPathKeyedMaps(partitionColumns, filePartitionValues));
    }

    private PartitionMetadata(PartitionMetadata other) {
        this.partitionColumns = other.partitionColumns;
        this.columnNames = other.columnNames;
        this.valuesByColumn = other.valuesByColumn;
        this.rowCount = other.rowCount;
        this.fileCount = other.fileCount;
        this.fileToRow = other.fileToRow;
        this.pathsByFile = other.pathsByFile;
    }

    private static PartitionMetadata fromPathKeyedMaps(
        Map<String, DataType> partitionColumns,
        Map<StoragePath, Map<String, Object>> filePartitionValues
    ) {
        if (partitionColumns == null) {
            throw new IllegalArgumentException("partitionColumns cannot be null");
        }
        if (filePartitionValues == null) {
            throw new IllegalArgumentException("filePartitionValues cannot be null");
        }
        Map<String, DataType> cols = unmodifiableOrderedCopy(partitionColumns);
        String[] names = cols.keySet().toArray(String[]::new);
        int nCols = names.length;
        int nFiles = filePartitionValues.size();
        if (nCols == 0) {
            return EMPTY;
        }
        if (nFiles == 0) {
            return new PartitionMetadata(cols, new Object[nCols][0], 0, 0, null, null);
        }
        Object[][] byCol = new Object[nCols][nFiles];
        StoragePath[] paths = new StoragePath[nFiles];
        int fileIndex = 0;
        for (Map.Entry<StoragePath, Map<String, Object>> entry : filePartitionValues.entrySet()) {
            paths[fileIndex] = entry.getKey();
            Map<String, Object> row = entry.getValue() == null ? Map.of() : entry.getValue();
            for (int c = 0; c < nCols; c++) {
                byCol[c][fileIndex] = row.get(names[c]);
            }
            fileIndex++;
        }
        return new PartitionMetadata(cols, byCol, nFiles, nFiles, null, paths);
    }

    /**
     * Factory for detectors: one row per file, identity mapping, no path index.
     */
    public static PartitionMetadata columnar(Map<String, DataType> partitionColumns, Object[][] valuesByColumn, int fileCount) {
        return new PartitionMetadata(partitionColumns, valuesByColumn, fileCount, fileCount, null, null);
    }

    public Map<String, DataType> partitionColumns() {
        return partitionColumns;
    }

    public int fileCount() {
        return fileCount;
    }

    public int rowCount() {
        return rowCount;
    }

    public boolean isEmpty() {
        return partitionColumns.isEmpty();
    }

    /**
     * Resolves the metadata file index for a listing ordinal and path. Detector-built metadata uses the
     * ordinal; path-keyed compat metadata looks up {@code path}. Returns {@code -1} when there is no row.
     */
    public int resolveFileIndex(int fileIndex, @Nullable StoragePath path) {
        if (pathsByFile != null) {
            if (path == null) {
                return -1;
            }
            for (int i = 0; i < pathsByFile.length; i++) {
                if (path.equals(pathsByFile[i])) {
                    return i;
                }
            }
            return -1;
        }
        if (fileIndex < 0 || fileIndex >= fileCount) {
            return -1;
        }
        return fileIndex;
    }

    /**
     * Value of {@code column} for listing file {@code fileIndex}, or {@code null} when the column is
     * absent / Hive-default. Throws if {@code fileIndex} is out of range or {@code column} is unknown.
     */
    public Object getValue(int fileIndex, String column) {
        Objects.checkIndex(fileIndex, fileCount);
        int col = columnIndex(column);
        if (col < 0) {
            throw new IllegalArgumentException("unknown partition column [" + column + "]");
        }
        return valuesByColumn[col][rowIndex(fileIndex)];
    }

    /**
     * Like {@link #getValue(int, String)} after {@link #resolveFileIndex(int, StoragePath)}.
     */
    @Nullable
    public Object getValue(int fileIndex, @Nullable StoragePath path, String column) {
        int resolved = resolveFileIndex(fileIndex, path);
        if (resolved < 0) {
            return null;
        }
        int col = columnIndex(column);
        if (col < 0) {
            return null;
        }
        return valuesByColumn[col][rowIndex(resolved)];
    }

    /**
     * Copies every partition column value for {@code fileIndex} into {@code target} (including explicit
     * {@code null}s). Does not clear {@code target}.
     */
    public void putValues(int fileIndex, Map<String, Object> target) {
        Objects.checkIndex(fileIndex, fileCount);
        int row = rowIndex(fileIndex);
        for (int c = 0; c < columnNames.length; c++) {
            target.put(columnNames[c], valuesByColumn[c][row]);
        }
    }

    /**
     * {@link #putValues(int, Map)} after {@link #resolveFileIndex(int, StoragePath)}. No-op when unresolved.
     */
    public void putValues(int fileIndex, @Nullable StoragePath path, Map<String, Object> target) {
        int resolved = resolveFileIndex(fileIndex, path);
        if (resolved >= 0) {
            putValues(resolved, target);
        }
    }

    /**
     * Small per-file map for filter / test use. Prefer {@link #putValues(int, Map)} on hot paths that
     * already own a mutable map.
     */
    public Map<String, Object> valuesAsMap(int fileIndex) {
        Objects.checkIndex(fileIndex, fileCount);
        if (columnNames.length == 0) {
            return Map.of();
        }
        LinkedHashMap<String, Object> map = Maps.newLinkedHashMapWithExpectedSize(columnNames.length);
        putValues(fileIndex, map);
        return Collections.unmodifiableMap(map);
    }

    /**
     * Rewrites this metadata so each distinct directory group owns one value row and files point at their
     * group via {@code fileToRow}. No-op when {@code groupCount >= fileCount} (no memory win), when
     * metadata is empty, or when this instance still carries a path index (ordinal remap would be unsafe).
     * {@code fileGroups.length} must equal {@link #fileCount()}.
     */
    public PartitionMetadata shareByGroups(short[] fileGroups, int groupCount) {
        if (isEmpty() || fileCount == 0 || groupCount >= fileCount || pathsByFile != null) {
            return this;
        }
        if (fileGroups == null || fileGroups.length != fileCount) {
            throw new IllegalArgumentException("fileGroups length must equal fileCount [" + fileCount + "]");
        }
        if (groupCount <= 0) {
            throw new IllegalArgumentException("groupCount must be positive");
        }
        Object[][] shared = new Object[columnNames.length][groupCount];
        boolean[] filled = new boolean[groupCount];
        int[] mapping = new int[fileCount];
        for (int f = 0; f < fileCount; f++) {
            int g = Short.toUnsignedInt(fileGroups[f]);
            if (g >= groupCount) {
                throw new IllegalArgumentException("fileGroups[" + f + "]=" + g + " out of range for groupCount=" + groupCount);
            }
            mapping[f] = g;
            if (filled[g] == false) {
                int oldRow = rowIndex(f);
                for (int c = 0; c < columnNames.length; c++) {
                    shared[c][g] = valuesByColumn[c][oldRow];
                }
                filled[g] = true;
            } else {
                // Hive / template tuples are directory-bound; siblings in a group must agree.
                int oldRow = rowIndex(f);
                for (int c = 0; c < columnNames.length; c++) {
                    if (Objects.equals(shared[c][g], valuesByColumn[c][oldRow]) == false) {
                        throw new IllegalStateException(
                            "partition values disagree within directory group ["
                                + g
                                + "] for column ["
                                + columnNames[c]
                                + "]"
                        );
                    }
                }
            }
        }
        for (int g = 0; g < groupCount; g++) {
            if (filled[g] == false) {
                throw new IllegalArgumentException("group [" + g + "] has no files");
            }
        }
        return new PartitionMetadata(partitionColumns, shared, groupCount, fileCount, mapping, null);
    }

    /**
     * Structure-bytes estimate for planning / breaker charges. Counts columnar ref arrays and an optional
     * file→row index; does not deep-size interned value objects.
     * <p>
     * Replaces the former flat {@code 560 × files} allowance. The new figure tracks real structure size
     * ({@code ≈ 64 + 24×cols + 8×cols×rows}), so deep layouts no longer under-charge relative to map-entry
     * cost, and shallow layouts reserve less than the old padded constant. Seam-1 tests charge
     * {@link org.elasticsearch.xpack.esql.datasources.spi.FileList#planningBytes()} dynamically; phase-2
     * survivor maps remain a separate fixed per-file constant.
     */
    public long planningBytes() {
        if (isEmpty()) {
            return 0L;
        }
        long bytes = 64L;
        int cols = columnNames.length;
        bytes += (long) cols * 24L;
        bytes += (long) cols * rowCount * 8L;
        if (fileToRow != null) {
            bytes += (long) fileToRow.length * Integer.BYTES;
        }
        if (pathsByFile != null) {
            bytes += (long) pathsByFile.length * 8L;
        }
        return bytes;
    }

    /**
     * Returns the names of partition columns that cannot be proven non-null across the matched
     * fileset. A column is in this set when at least one value row has {@code null} for it — typically
     * because the file lives under a {@code __HIVE_DEFAULT_PARTITION__} directory decoded by
     * {@link HivePartitionDetector}. When per-file values are absent altogether (e.g. metadata
     * constructed without scanning files), every declared column is returned: no evidence means no
     * non-null guarantee.
     * <p>
     * This is a per-query property: different globs over the same dataset can match
     * different subsets of files and therefore yield different null-bearing column sets.
     * Callers use it to decide per-column {@code Nullability.TRUE}/{@code FALSE} when
     * building attributes; columns absent from this set are provably non-null in the
     * matched fileset.
     */
    public Set<String> nullablePartitionColumns() {
        if (partitionColumns.isEmpty()) {
            return Set.of();
        }
        if (rowCount == 0) {
            // No per-file evidence — stay conservative: every column may be null.
            return Set.copyOf(partitionColumns.keySet());
        }
        Set<String> nullable = new LinkedHashSet<>();
        for (int c = 0; c < columnNames.length; c++) {
            Object[] col = valuesByColumn[c];
            for (int r = 0; r < rowCount; r++) {
                if (col[r] == null) {
                    nullable.add(columnNames[c]);
                    break;
                }
            }
        }
        return Collections.unmodifiableSet(nullable);
    }

    private int rowIndex(int fileIndex) {
        return fileToRow == null ? fileIndex : fileToRow[fileIndex];
    }

    private int columnIndex(String column) {
        for (int c = 0; c < columnNames.length; c++) {
            if (columnNames[c].equals(column)) {
                return c;
            }
        }
        return -1;
    }

    private static <K, V> Map<K, V> unmodifiableOrderedCopy(Map<K, V> source) {
        if (source.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof PartitionMetadata == false) {
            return false;
        }
        PartitionMetadata that = (PartitionMetadata) o;
        if (fileCount != that.fileCount || partitionColumns.equals(that.partitionColumns) == false) {
            return false;
        }
        // Compare by resolved per-file values so sharing / path-index representation does not affect equality.
        if (pathsByFile != null || that.pathsByFile != null) {
            if (pathsByFile == null || that.pathsByFile == null) {
                return false;
            }
            for (StoragePath path : pathsByFile) {
                int self = resolveFileIndex(-1, path);
                int other = that.resolveFileIndex(-1, path);
                if (self < 0 || other < 0) {
                    return false;
                }
                for (int c = 0; c < columnNames.length; c++) {
                    if (Objects.equals(valuesByColumn[c][rowIndex(self)], that.valuesByColumn[c][that.rowIndex(other)]) == false) {
                        return false;
                    }
                }
            }
            // Same path set (order-independent).
            if (pathsByFile.length != that.pathsByFile.length) {
                return false;
            }
            for (StoragePath path : that.pathsByFile) {
                if (resolveFileIndex(-1, path) < 0) {
                    return false;
                }
            }
            return true;
        }
        for (int f = 0; f < fileCount; f++) {
            for (int c = 0; c < columnNames.length; c++) {
                if (Objects.equals(valuesByColumn[c][rowIndex(f)], that.valuesByColumn[c][that.rowIndex(f)]) == false) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = partitionColumns.hashCode();
        h = 31 * h + fileCount;
        if (pathsByFile != null) {
            // Order-independent: xor path→values hashes so insertion order cannot break the equals contract.
            int pathHash = 0;
            for (int f = 0; f < fileCount; f++) {
                int vh = Objects.hashCode(pathsByFile[f]);
                for (int c = 0; c < columnNames.length; c++) {
                    vh = 31 * vh + Objects.hashCode(valuesByColumn[c][rowIndex(f)]);
                }
                pathHash ^= vh;
            }
            return 31 * h + pathHash;
        }
        for (int f = 0; f < fileCount; f++) {
            for (int c = 0; c < columnNames.length; c++) {
                h = 31 * h + Objects.hashCode(valuesByColumn[c][rowIndex(f)]);
            }
        }
        return h;
    }

    @Override
    public String toString() {
        return "PartitionMetadata{columns="
            + partitionColumns.keySet()
            + ", fileCount="
            + fileCount
            + ", rowCount="
            + rowCount
            + ", shared="
            + (fileToRow != null)
            + ", pathIndexed="
            + (pathsByFile != null)
            + '}';
    }
}
