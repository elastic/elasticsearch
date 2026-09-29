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
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Holds partition information detected from file paths.
 * Maps partition column names to their inferred types, and each file path
 * to its extracted partition key-value pairs.
 * Both maps preserve insertion order so that partition columns appear
 * in the same order they are declared in the path.
 */
public record PartitionMetadata(Map<String, DataType> partitionColumns, Map<StoragePath, Map<String, Object>> filePartitionValues) {

    public static final PartitionMetadata EMPTY = new PartitionMetadata(Map.of(), Map.of());

    public PartitionMetadata {
        if (partitionColumns == null) {
            throw new IllegalArgumentException("partitionColumns cannot be null");
        }
        if (filePartitionValues == null) {
            throw new IllegalArgumentException("filePartitionValues cannot be null");
        }
        partitionColumns = unmodifiableOrderedCopy(partitionColumns);
        filePartitionValues = unmodifiableOrderedFileValues(filePartitionValues);
    }

    private static <K, V> Map<K, V> unmodifiableOrderedCopy(Map<K, V> source) {
        if (source.isEmpty()) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }

    private static Map<StoragePath, Map<String, Object>> unmodifiableOrderedFileValues(Map<StoragePath, Map<String, Object>> source) {
        if (source.isEmpty()) {
            return Map.of();
        }
        LinkedHashMap<StoragePath, Map<String, Object>> copy = Maps.newLinkedHashMapWithExpectedSize(source.size());
        for (Map.Entry<StoragePath, Map<String, Object>> entry : source.entrySet()) {
            copy.put(entry.getKey(), unmodifiableOrderedCopy(entry.getValue()));
        }
        return Collections.unmodifiableMap(copy);
    }

    public boolean isEmpty() {
        return partitionColumns.isEmpty();
    }

    /**
     * These partition columns, valued over a different set of files.
     * <p>
     * The columns are the dataset's schema: which they are, and what type each holds, is decided once at
     * resolution, and the plan's output attributes already carry that answer. The values are per file, so they
     * belong to whichever listing named the files being read — and when resolution answered the schema from a
     * prefix of the dataset, that is not the listing resolution held. A file the schema's listing never saw has
     * no entry in it, and a missing entry reads as an absent partition value: the column comes back null for
     * every row of that file, silently.
     * <p>
     * Each value is conformed to the column's declared type. The two listings can type a column differently —
     * a wider set of paths can carry a value the prefix's type cannot hold, a narrower one can look more
     * specific than the prefix did — and the declared type is the one the plan is built on, so it wins. A value
     * the declared type cannot hold has none under it, which is confined to the values that genuinely do not fit
     * rather than falling on every file the schema's listing did not reach.
     */
    /**
     * These partition columns, valued over the files a query actually reads.
     * <p>
     * The columns are the dataset's schema: which they are, and what type each holds, is decided once at
     * resolution, and the plan's output attributes already carry that answer. The values are per file, so they
     * belong to whichever listing named the files being read - and when resolution answered the schema from a
     * prefix of the dataset, that is not the listing resolution held.
     * <p>
     * Values are read from the file set's own paths rather than from anything the scan's listing parsed. That is
     * what lets a <em>bounded</em> scan listing work at all: {@code GenericFileList} strips per-file partition
     * evidence from a truncated list, so a listing that stopped early carries no values to copy - but it still
     * names its files, and a path is the lossless record of what its folders say.
     */
    public PartitionMetadata valuedOver(@Nullable FileList files, @Nullable PartitionConfig partitionConfig) {
        if (partitionColumns.isEmpty() || files == null || files.isResolved() == false || files.fileCount() == 0) {
            return this;
        }
        LinkedHashMap<StoragePath, Map<String, Object>> valued = Maps.newLinkedHashMapWithExpectedSize(files.fileCount());
        for (int i = 0; i < files.fileCount(); i++) {
            StoragePath path = files.path(i);
            LinkedHashMap<String, Object> conformed = Maps.newLinkedHashMapWithExpectedSize(partitionColumns.size());
            for (Map.Entry<String, DataType> column : partitionColumns.entrySet()) {
                conformed.put(column.getKey(), under(tokenFor(path, column.getKey(), partitionConfig), column.getValue()));
            }
            valued.put(path, conformed);
        }
        return new PartitionMetadata(partitionColumns, valued);
    }

    /**
     * The raw token {@code path} carries for {@code column}, under the dataset's detection strategy, or
     * {@code null} when the path binds no value for it.
     */
    @Nullable
    private static String tokenFor(StoragePath path, String column, @Nullable PartitionConfig partitionConfig) {
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

    /**
     * These partition columns with no per-file values.
     * <p>
     * The values are evidence, and {@link #nullablePartitionColumns} reads them as evidence about the whole
     * matched fileset: a column present and non-null in every one of them is reported non-null, and the
     * attribute built from it promises the optimizer that no row has a null there. Over a listing that covers
     * part of a dataset that promise is not the listing's to make. Dropping the values makes it unprovable
     * instead of wrong — every column comes back nullable, which is the conservative answer that method already
     * gives when it has no per-file evidence at all.
     */
    public PartitionMetadata withoutPerFileEvidence() {
        return filePartitionValues.isEmpty() ? this : new PartitionMetadata(partitionColumns, Map.of());
    }

    /**
     * Returns the names of partition columns that cannot be proven non-null across the matched
     * fileset. A column is in this set when at least one file in {@link #filePartitionValues}
     * has {@code null} (or no entry) for it — typically because the file lives under a
     * {@code __HIVE_DEFAULT_PARTITION__} directory decoded by {@link HivePartitionDetector}.
     * When per-file values are absent altogether (e.g. metadata constructed without scanning
     * files), every declared column is returned: no evidence means no non-null guarantee.
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
        if (filePartitionValues.isEmpty()) {
            // No per-file evidence — stay conservative: every column may be null.
            return Set.copyOf(partitionColumns.keySet());
        }
        Set<String> nullable = new LinkedHashSet<>();
        for (Map<String, Object> values : filePartitionValues.values()) {
            for (String column : partitionColumns.keySet()) {
                if (nullable.contains(column)) {
                    continue;
                }
                // Map.get returns null for both explicit nulls and absent keys; either case means
                // we have no non-null guarantee for this column.
                if (values.get(column) == null) {
                    nullable.add(column);
                }
            }
            if (nullable.size() == partitionColumns.size()) {
                break;
            }
        }
        return Collections.unmodifiableSet(nullable);
    }
}
