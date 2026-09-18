/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.core.type.DataType;
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
