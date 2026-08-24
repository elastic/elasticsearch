/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups rows into Hive-style partitions.
 * <p>
 * This is the part of hive fixture generation that has nothing to do with the output format:
 * which rows land in which bucket, and what the bucket's directory is called. Only writing the
 * bytes is format-specific.
 * <p>
 * It lived inside {@code ParquetFixtureGenerator}, which is why hive fixtures existed for
 * Parquet alone -- the two pieces the generators already shared, {@link CsvFixtureParser} and
 * {@link SplitPartitioner}, had spread to every format, and this one had not.
 */
public final class HivePartitioner {

    /** How Hive encodes a null partition value on disk. */
    public static final String NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    private HivePartitioner() {}

    /**
     * Rows grouped by the value of {@code sourceColumn}, in first-seen order so the on-disk
     * layout is deterministic. Rows whose source cell is null are grouped under
     * {@link #NULL_PARTITION}.
     *
     * @throws IllegalArgumentException if the source column is not in the schema
     */
    public static Map<String, List<Object[]>> bucketRows(CsvFixtureParser.CsvFixtureResult result, String sourceColumn) {
        int sourceColIdx = -1;
        List<CsvFixtureParser.ColumnSpec> schema = result.schema();
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).name().equals(sourceColumn)) {
                sourceColIdx = i;
                break;
            }
        }
        if (sourceColIdx < 0) {
            throw new IllegalArgumentException("Source column not found in CSV: " + sourceColumn);
        }

        Map<String, List<Object[]>> buckets = new LinkedHashMap<>();
        for (Object[] row : result.rows()) {
            Object cell = sourceColIdx < row.length ? row[sourceColIdx] : null;
            String bucket = cell == null ? NULL_PARTITION : cell.toString();
            buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(row);
        }
        return buckets;
    }

    /**
     * The partition directory name for a bucket, {@code <partitionColumn>=<value>}.
     * <p>
     * The partition column is deliberately named differently from the source column it was
     * bucketed on: the source column stays in the file payload, and the partition column is
     * path-derived and injected by the reader, so sharing a name would collide.
     */
    public static String partitionDirName(String partitionColumn, String bucket) {
        return partitionColumn + "=" + bucket;
    }
}
