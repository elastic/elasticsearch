/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.Map;

/**
 * Holds Hive-style partition information detected from file paths.
 * Maps partition column names to their inferred types, and each file path
 * to its extracted partition key-value pairs.
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
        partitionColumns = Map.copyOf(partitionColumns);
        filePartitionValues = Map.copyOf(filePartitionValues);
    }

    public boolean isEmpty() {
        return partitionColumns.isEmpty();
    }
}
