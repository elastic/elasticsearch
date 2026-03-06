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

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects partition columns from bare directory segments using a path template.
 * Template syntax uses {@code {name}} placeholders (e.g., {@code {year}/{month}/{day}}).
 * Values are extracted positionally from the last N directory segments above the filename.
 */
final class TemplatePartitionDetector implements PartitionDetector {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{(\\w+)}");

    private final String template;
    private final List<String> columnNames;

    TemplatePartitionDetector(String template) {
        if (template == null || template.isEmpty()) {
            throw new IllegalArgumentException("template cannot be null or empty");
        }
        this.template = template;
        this.columnNames = parseTemplateColumns(template);
        if (this.columnNames.isEmpty()) {
            throw new IllegalArgumentException("template must contain at least one {name} placeholder: " + template);
        }
    }

    @Override
    public String name() {
        return "template";
    }

    @Override
    public PartitionMetadata detect(List<StorageEntry> files, Map<String, Object> config) {
        if (files == null || files.isEmpty()) {
            return PartitionMetadata.EMPTY;
        }

        int segmentCount = columnNames.size();
        List<Map<String, String>> allRawPartitions = new ArrayList<>();

        for (StorageEntry entry : files) {
            Map<String, String> partitions = extractByTemplate(entry.path(), segmentCount);
            if (partitions == null) {
                return PartitionMetadata.EMPTY;
            }
            allRawPartitions.add(partitions);
        }

        LinkedHashMap<String, List<String>> columnValues = Maps.newLinkedHashMapWithExpectedSize(segmentCount);
        for (String col : columnNames) {
            columnValues.put(col, new ArrayList<>());
        }
        for (Map<String, String> raw : allRawPartitions) {
            for (Map.Entry<String, String> e : raw.entrySet()) {
                columnValues.get(e.getKey()).add(e.getValue());
            }
        }

        LinkedHashMap<String, DataType> partitionColumns = Maps.newLinkedHashMapWithExpectedSize(segmentCount);
        for (Map.Entry<String, List<String>> e : columnValues.entrySet()) {
            partitionColumns.put(e.getKey(), HivePartitionDetector.inferType(e.getValue()));
        }

        LinkedHashMap<StoragePath, Map<String, Object>> filePartitionValues = Maps.newLinkedHashMapWithExpectedSize(files.size());
        for (int i = 0; i < files.size(); i++) {
            Map<String, String> raw = allRawPartitions.get(i);
            LinkedHashMap<String, Object> typed = Maps.newLinkedHashMapWithExpectedSize(segmentCount);
            for (Map.Entry<String, String> e : raw.entrySet()) {
                typed.put(e.getKey(), HivePartitionDetector.castValue(e.getValue(), partitionColumns.get(e.getKey())));
            }
            filePartitionValues.put(files.get(i).path(), typed);
        }

        return new PartitionMetadata(partitionColumns, filePartitionValues);
    }

    private Map<String, String> extractByTemplate(StoragePath storagePath, int expectedSegments) {
        String path = storagePath.path();
        if (path == null || path.isEmpty()) {
            return null;
        }

        String[] segments = path.split("/");
        List<String> nonEmpty = new ArrayList<>();
        for (String s : segments) {
            if (s.isEmpty() == false) {
                nonEmpty.add(s);
            }
        }

        // Need at least expectedSegments + 1 (for the filename)
        if (nonEmpty.size() < expectedSegments + 1) {
            return null;
        }

        // Take the last N segments before the filename
        int filenameIdx = nonEmpty.size() - 1;
        int startIdx = filenameIdx - expectedSegments;

        LinkedHashMap<String, String> result = Maps.newLinkedHashMapWithExpectedSize(expectedSegments);
        for (int i = 0; i < expectedSegments; i++) {
            String segment = nonEmpty.get(startIdx + i);
            String decoded = urlDecode(segment);
            result.put(columnNames.get(i), decoded);
        }
        return result;
    }

    private static String urlDecode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return value;
        }
    }

    static List<String> parseTemplateColumns(String template) {
        List<String> columns = new ArrayList<>();
        String[] segments = template.split("/");
        for (String segment : segments) {
            if (segment.isEmpty()) {
                continue;
            }
            Matcher m = PLACEHOLDER.matcher(segment);
            if (m.matches()) {
                columns.add(m.group(1));
            }
        }
        return columns;
    }

    List<String> columnNames() {
        return columnNames;
    }

    String template() {
        return template;
    }
}
