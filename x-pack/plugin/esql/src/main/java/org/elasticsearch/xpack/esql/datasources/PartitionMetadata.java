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
import java.util.Map;

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
}
