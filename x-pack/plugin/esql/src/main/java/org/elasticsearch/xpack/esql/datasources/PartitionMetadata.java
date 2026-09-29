/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
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

    private static final Logger logger = LogManager.getLogger(PartitionMetadata.class);

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
     * Takes ownership of {@code valuesByColumn}, {@code fileToRow} and {@code pathsByFile}: every caller builds
     * them fresh, and copying here would double peak memory for large listings. Callers must not mutate them
     * afterwards. {@code valuesByColumn[c].length} must equal {@code rowCount}; when {@code fileToRow} is
     * {@code null}, {@code fileCount} must equal {@code rowCount}.
     */
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
            this.fileToRow = fileToRow;
        }
        if (pathsByFile != null && pathsByFile.length != fileCount) {
            throw new IllegalArgumentException("pathsByFile length must equal fileCount");
        }
        this.valuesByColumn = valuesByColumn;
        this.rowCount = rowCount;
        this.fileCount = fileCount;
        this.pathsByFile = pathsByFile;
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
            return new PartitionMetadata(cols, new Object[nCols][0], 0, 0, null, new StoragePath[0]);
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
     * Factory for detectors: one row per file, identity mapping, no path index. Takes ownership of
     * {@code valuesByColumn}; the caller must not mutate it afterwards.
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
     * Whether this metadata can be attached to a listing of {@code listingFileCount} files. Ordinal-aligned
     * metadata must cover exactly that many files, since values are looked up by listing position.
     * Path-keyed metadata resolves by {@link StoragePath} and is exempt.
     */
    public boolean coversFileCount(int listingFileCount) {
        return isEmpty() || pathsByFile != null || fileCount == listingFileCount;
    }

    /**
     * Resolves the metadata file index for a listing ordinal and path. Detector-built metadata uses the
     * ordinal, which must be in range: an out-of-range ordinal means the metadata and the listing are
     * misaligned. Path-keyed compat metadata looks up {@code path} and returns {@code -1} when it has no row.
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
            assert false : "file index [" + fileIndex + "] out of range for partition metadata covering [" + fileCount + "] files";
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
     * Value of the column at position {@code column} in {@link #partitionColumns()} iteration order, for
     * callers that already walk the columns and would otherwise pay a name lookup per column.
     */
    public Object getValueAt(int fileIndex, int column) {
        Objects.checkIndex(fileIndex, fileCount);
        return valuesByColumn[column][rowIndex(fileIndex)];
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
     * <p>
     * Sharing is only a memory optimisation, so it never fails the query: if the grouping does not fit this
     * metadata (wrong length, out-of-range group, empty group) or siblings in one group carry different
     * values, it trips an assertion and returns {@code this} unshared. Hive and template values are
     * directory-bound, so disagreement means a detector or grouping bug.
     */
    public PartitionMetadata shareByGroups(short[] fileGroups, int groupCount) {
        if (isEmpty() || fileCount == 0 || groupCount >= fileCount || pathsByFile != null) {
            return this;
        }
        if (fileGroups == null || fileGroups.length != fileCount || groupCount <= 0) {
            assert false : "directory grouping does not fit partition metadata covering [" + fileCount + "] files";
            return this;
        }
        Object[][] shared = new Object[columnNames.length][groupCount];
        boolean[] filled = new boolean[groupCount];
        int[] mapping = new int[fileCount];
        for (int f = 0; f < fileCount; f++) {
            int g = Short.toUnsignedInt(fileGroups[f]);
            if (g >= groupCount) {
                assert false : "fileGroups[" + f + "]=" + g + " out of range for groupCount=" + groupCount;
                return this;
            }
            mapping[f] = g;
            int oldRow = rowIndex(f);
            if (filled[g] == false) {
                for (int c = 0; c < columnNames.length; c++) {
                    shared[c][g] = valuesByColumn[c][oldRow];
                }
                filled[g] = true;
            } else {
                for (int c = 0; c < columnNames.length; c++) {
                    if (Objects.equals(shared[c][g], valuesByColumn[c][oldRow]) == false) {
                        logger.debug(
                            "partition values disagree within directory group [{}] for column [{}], keeping one row per file",
                            g,
                            columnNames[c]
                        );
                        assert false : "partition values disagree within directory group [" + g + "] for column [" + columnNames[c] + "]";
                        return this;
                    }
                }
            }
        }
        for (int g = 0; g < groupCount; g++) {
            if (filled[g] == false) {
                assert false : "directory group [" + g + "] has no files";
                return this;
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

    /**
     * These partition columns, valued over the files a query actually reads.
     * <p>
     * The columns are the dataset's schema: which they are, and what type each holds, is decided once at
     * resolution, and the plan's output attributes already carry that answer. The values are per file, so they
     * belong to whichever listing named the files being read - and when resolution answered the schema from a
     * prefix of the dataset, that is not the listing resolution held.
     * <p>
     * Values are read from the file set's own paths rather than from anything the scan's listing parsed. That is
     * what lets a <em>bounded</em> scan listing work at all: a truncated list carries no per-file evidence to copy,
     * but it still names its files, and a path is the lossless record of what its folders say.
     * <p>
     * One row per file, in listing order, which is the layout {@link #columnar} documents for a detector.
     */
    public PartitionMetadata valuedOver(@Nullable FileList files, @Nullable PartitionConfig partitionConfig) {
        if (partitionColumns.isEmpty() || files == null || files.isResolved() == false || files.fileCount() == 0) {
            return this;
        }
        int fileCount = files.fileCount();
        String[] names = partitionColumns.keySet().toArray(String[]::new);
        Object[][] byColumn = new Object[names.length][fileCount];
        for (int file = 0; file < fileCount; file++) {
            StoragePath path = files.path(file);
            for (int column = 0; column < names.length; column++) {
                byColumn[column][file] = under(tokenFor(path, names[column], partitionConfig), partitionColumns.get(names[column]));
            }
        }
        return columnar(partitionColumns, byColumn, fileCount);
    }

    /** Whether every row is already empty, so there is nothing left to strip and stripping is a no-op. */
    private boolean carriesNoEvidence() {
        for (Object[] column : valuesByColumn) {
            for (Object value : column) {
                if (value != null) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * These partition columns with no per-file values.
     * <p>
     * A truncated listing saw only part of what its pattern matches, so its per-file values are evidence about a
     * prefix rather than about the dataset. The columns still stand; the values do not travel.
     */
    public PartitionMetadata withoutPerFileEvidence() {
        if (partitionColumns.isEmpty() || fileCount == 0 || carriesNoEvidence()) {
            return this;
        }
        // The rows stay and their values go. Dropping the rows instead would leave metadata that no longer covers
        // the listing it hangs off (coversFileCount), and would turn a harmless absent value into an index out of
        // bounds for anything that asks a stripped listing what a file's partition value is.
        return columnar(partitionColumns, new Object[partitionColumns.size()][fileCount], fileCount);
    }

    /**
     * The raw token {@code path} carries for {@code column}, under the dataset's detection strategy, or
     * {@code null} when the path binds no value for it.
     */
    @Nullable
    static String tokenFor(StoragePath path, String column, @Nullable PartitionConfig partitionConfig) {
        PartitionConfig.Strategy strategy = partitionConfig == null ? PartitionConfig.Strategy.HIVE : partitionConfig.strategy();
        String template = partitionConfig == null ? null : partitionConfig.pathTemplate();
        if (strategy == PartitionConfig.Strategy.TEMPLATE) {
            return template == null ? null : TemplatePartitionDetector.columnValue(path.path(), column, template);
        }
        String hive = HivePartitionDetector.extractPartitions(path).get(column);
        if (hive != null || strategy != PartitionConfig.Strategy.AUTO || template == null) {
            return hive;
        }
        // AUTO tries hive first and falls back to the template, the order AutoPartitionDetector detects in.
        return TemplatePartitionDetector.columnValue(path.path(), column, template);
    }

    /**
     * One value under the type the dataset's schema declares for its column, read from the path itself.
     * <p>
     * The path is the only lossless record of a partition value. Everything else is a parse of it under whichever
     * type <em>that</em> listing inferred, and a parse is not reversible: {@code unsigned_long} is held
     * sign-flip-encoded so its {@code long} is not the number, a double has already rounded, and {@code 0} cannot
     * say whether the folder read {@code 0} or {@code 00}. Re-parsing the token under the declared type asks the
     * same question the detector asked and gets the same answer, for every type.
     * <p>
     * A token the declared type cannot hold has no value under it, which is confined to the values that genuinely
     * do not fit rather than falling on every file the schema's listing did not reach.
     */
    @Nullable
    private static Object under(@Nullable String token, DataType declared) {
        if (token == null) {
            return null;
        }
        try {
            return HivePartitionDetector.castValue(token, declared);
        } catch (RuntimeException e) {
            return null;
        }
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
            // Paths are unique map keys and both sides cover fileCount files, so containment in one direction
            // proves the path sets are equal.
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
