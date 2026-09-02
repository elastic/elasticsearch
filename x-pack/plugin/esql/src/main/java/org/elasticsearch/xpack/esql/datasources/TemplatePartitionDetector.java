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
public final class TemplatePartitionDetector implements PartitionDetector {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{(\\w+)}");

    private final String template;
    private final List<String> columnNames;
    /** Original placeholder names that {@link ReservedPartitionNames#surface} renamed, for the per-detect warning. */
    private final List<String> renamedColumns;

    public TemplatePartitionDetector(String template) {
        if (template == null || template.isEmpty()) {
            throw new IllegalArgumentException("template cannot be null or empty");
        }
        this.template = template;
        // Reserved metadata names are dedicated; a template placeholder like {_index} cannot claim
        // one (the placeholder grammar accepts any \w+ name, including underscore-led standard
        // metadata names). Surface those columns under the _partition.* prefix — same contract as
        // the Hive detector. Rename targets contain a dot, which the placeholder grammar cannot
        // produce, so a rename can never collide with another template column.
        List<String> parsed = parseTemplateColumns(template);
        List<String> surfaced = new ArrayList<>(parsed.size());
        List<String> renamed = new ArrayList<>(0);
        for (String name : parsed) {
            String surface = ReservedPartitionNames.surface(name);
            if (surface.equals(name) == false) {
                renamed.add(name);
            }
            surfaced.add(surface);
        }
        this.columnNames = surfaced;
        this.renamedColumns = renamed;
        if (this.columnNames.isEmpty()) {
            throw new IllegalArgumentException("template must contain at least one {name} placeholder: " + template);
        }
    }

    @Override
    public String name() {
        return "template";
    }

    @Override
    public PartitionMetadata detect(List<StorageEntry> files) {
        if (files == null || files.isEmpty()) {
            return PartitionMetadata.EMPTY;
        }
        // Warn at detection time (not construction) so the header lands on the resolving request's
        // thread context, mirroring the Hive detector.
        ReservedPartitionNames.warnRenamed(renamedColumns);

        int segmentCount = columnNames.size();

        // Every file must sit at the same directory depth. The template binds the LAST N segments before the
        // filename, so files at different depths bind different physical levels to the same column: over
        // data/2024/f1, data/2024/01/f2 and data/2024/01/15/f3 with template {year}, the three files would report
        // year=2024, year=01 and year=15, and a STATS BY year would bucket a day value as a year. Bailing to EMPTY
        // is the same all-or-nothing stance HivePartitionDetector takes when its key sets disagree across files.
        // The cost is that a comma-separated list mixing prefixes of different depths loses template detection even
        // where the templated tail lines up; no partition columns is safe, a misbound one is not.
        if (hasMixedDepth(files)) {
            return PartitionMetadata.EMPTY;
        }

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

    /** Whether the files sit at differing directory depths, which makes last-N template binding inconsistent. */
    private static boolean hasMixedDepth(List<StorageEntry> files) {
        int depth = -1;
        for (StorageEntry entry : files) {
            int d = pathDepth(entry.path());
            if (depth == -1) {
                depth = d;
            } else if (d != depth) {
                return true;
            }
        }
        return false;
    }

    /**
     * Non-empty path segments excluding the file name itself. Reads {@code path()} rather than {@code objectName()},
     * which is only the last segment — the same full-path walk {@link #extractByTemplate} does.
     */
    private static int pathDepth(StoragePath storagePath) {
        String path = storagePath.path();
        if (path == null || path.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (String segment : path.split("/")) {
            if (segment.isEmpty() == false) {
                count++;
            }
        }
        // the last segment is the file name
        return Math.max(count - 1, 0);
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
            String decoded = HivePartitionDetector.decodePartitionValue(segment);
            result.put(columnNames.get(i), decoded);
        }
        return result;
    }

    public static List<String> parseTemplateColumns(String template) {
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
