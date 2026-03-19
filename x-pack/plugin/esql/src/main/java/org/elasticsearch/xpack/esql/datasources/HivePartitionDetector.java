/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Detects Hive-style partition columns from file paths (e.g., {@code /year=2024/month=06/file.parquet}).
 * Parses key=value segments, validates consistency across all files, and infers types
 * using Spark-style rules: try Integer, Long, Double, Boolean, fallback to keyword.
 */
final class HivePartitionDetector implements PartitionDetector {

    static final HivePartitionDetector INSTANCE = new HivePartitionDetector();

    HivePartitionDetector() {}

    @Override
    public String name() {
        return "hive";
    }

    @Override
    public PartitionMetadata detect(List<StorageEntry> files, Map<String, Object> config) {
        return detect(files);
    }

    static PartitionMetadata detect(List<StorageEntry> files) {
        if (files == null || files.isEmpty()) {
            return PartitionMetadata.EMPTY;
        }

        List<Map<String, String>> allRawPartitions = new ArrayList<>();
        Set<String> referenceKeys = null;

        for (StorageEntry entry : files) {
            Map<String, String> partitions = extractPartitions(entry.path());
            if (partitions.isEmpty()) {
                return PartitionMetadata.EMPTY;
            }

            Set<String> keys = partitions.keySet();
            if (referenceKeys == null) {
                referenceKeys = new LinkedHashSet<>(keys);
            } else if (referenceKeys.equals(keys) == false) {
                return PartitionMetadata.EMPTY;
            }

            allRawPartitions.add(partitions);
        }

        if (referenceKeys == null || referenceKeys.isEmpty()) {
            return PartitionMetadata.EMPTY;
        }

        LinkedHashMap<String, List<String>> columnValues = Maps.newLinkedHashMapWithExpectedSize(referenceKeys.size());
        for (String key : referenceKeys) {
            columnValues.put(key, new ArrayList<>());
        }
        for (Map<String, String> raw : allRawPartitions) {
            for (Map.Entry<String, String> e : raw.entrySet()) {
                columnValues.get(e.getKey()).add(e.getValue());
            }
        }

        LinkedHashMap<String, DataType> partitionColumns = Maps.newLinkedHashMapWithExpectedSize(referenceKeys.size());
        for (Map.Entry<String, List<String>> e : columnValues.entrySet()) {
            partitionColumns.put(e.getKey(), inferType(e.getValue()));
        }

        LinkedHashMap<StoragePath, Map<String, Object>> filePartitionValues = Maps.newLinkedHashMapWithExpectedSize(files.size());
        for (int i = 0; i < files.size(); i++) {
            Map<String, String> raw = allRawPartitions.get(i);
            LinkedHashMap<String, Object> typed = Maps.newLinkedHashMapWithExpectedSize(referenceKeys.size());
            for (Map.Entry<String, String> e : raw.entrySet()) {
                typed.put(e.getKey(), castValue(e.getValue(), partitionColumns.get(e.getKey())));
            }
            filePartitionValues.put(files.get(i).path(), typed);
        }

        return new PartitionMetadata(partitionColumns, filePartitionValues);
    }

    private static Map<String, String> extractPartitions(StoragePath storagePath) {
        String path = storagePath.path();
        if (path == null || path.isEmpty()) {
            return Map.of();
        }

        String[] segments = path.split("/");
        Map<String, String> partitions = new LinkedHashMap<>();

        for (String segment : segments) {
            if (segment.isEmpty()) {
                continue;
            }
            int eqIdx = segment.indexOf('=');
            if (eqIdx <= 0 || eqIdx == segment.length() - 1) {
                continue;
            }
            String afterEq = segment.substring(eqIdx + 1);
            if (afterEq.indexOf('=') >= 0) {
                continue;
            }
            if (segment.indexOf('.') >= 0) {
                continue;
            }
            String key = segment.substring(0, eqIdx);
            String value = urlDecode(afterEq);
            if (partitions.containsKey(key)) {
                continue;
            }
            partitions.put(key, value);
        }

        return partitions;
    }

    private static String urlDecode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return value;
        }
    }

    static DataType inferType(List<String> values) {
        DataType integralType = tryAllIntegral(values);
        if (integralType != null) {
            return integralType;
        }
        if (tryAllDouble(values)) {
            return DataType.DOUBLE;
        }
        if (tryAllBoolean(values)) {
            return DataType.BOOLEAN;
        }
        return DataType.KEYWORD;
    }

    private static DataType tryAllIntegral(List<String> values) {
        boolean needsLong = false;
        for (String v : values) {
            try {
                Number n = StringUtils.parseIntegral(v);
                if (n instanceof Long) {
                    needsLong = true;
                }
            } catch (Exception e) {
                return null;
            }
        }
        return needsLong ? DataType.LONG : DataType.INTEGER;
    }

    private static boolean tryAllDouble(List<String> values) {
        for (String v : values) {
            try {
                StringUtils.parseDouble(v);
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    private static boolean tryAllBoolean(List<String> values) {
        for (String v : values) {
            if ("true".equalsIgnoreCase(v) == false && "false".equalsIgnoreCase(v) == false) {
                return false;
            }
        }
        return true;
    }

    static Object castValue(String value, DataType type) {
        if (type == DataType.INTEGER) {
            return Integer.parseInt(value);
        }
        if (type == DataType.LONG) {
            return Long.parseLong(value);
        }
        if (type == DataType.DOUBLE) {
            return Double.parseDouble(value);
        }
        if (type == DataType.BOOLEAN) {
            return Booleans.parseBoolean(value);
        }
        return value;
    }
}
