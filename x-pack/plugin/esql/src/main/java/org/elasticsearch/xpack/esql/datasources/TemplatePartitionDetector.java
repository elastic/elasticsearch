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
 * A non-placeholder segment is a required directory name. The template (placeholders and
 * literals) is right-aligned to the last N directories above the filename.
 */
public final class TemplatePartitionDetector implements PartitionDetector {

    /**
     * One {@code /}-separated piece of a {@code partition_path} template. A segment is a
     * {@link Placeholder} only when it is exactly {@code {name}}; everything else is a {@link Literal}.
     */
    public sealed interface TemplateSegment {
        record Placeholder(String name) implements TemplateSegment {}

        record Literal(String value) implements TemplateSegment {}
    }

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{(\\w+)}");

    private final String template;
    private final List<TemplateSegment> segments;
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
        List<TemplateSegment> parsed = parseTemplate(template);
        List<TemplateSegment> surfacedSegments = new ArrayList<>(parsed.size());
        List<String> surfaced = new ArrayList<>();
        List<String> renamed = new ArrayList<>(0);
        for (TemplateSegment segment : parsed) {
            switch (segment) {
                case TemplateSegment.Placeholder(String name) -> {
                    String surface = ReservedPartitionNames.surface(name);
                    if (surface.equals(name) == false) {
                        renamed.add(name);
                    }
                    surfaced.add(surface);
                    surfacedSegments.add(new TemplateSegment.Placeholder(surface));
                }
                case TemplateSegment.Literal literal -> surfacedSegments.add(literal);
            }
        }
        this.segments = surfacedSegments;
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

        int columnCount = columnNames.size();

        // Every file must sit at the same directory depth. The template binds the last N directories before the
        // filename (placeholders and literals), so files at different depths bind different physical levels to the
        // same column: over data/2024/f1, data/2024/01/f2 and data/2024/01/15/f3 with template {year}, the three
        // files would report year=2024, year=01 and year=15, and a STATS BY year would bucket a day value as a year.
        // Bailing to EMPTY is the same all-or-nothing stance HivePartitionDetector takes when its key sets disagree
        // across files. The cost is that a comma-separated list mixing prefixes of different depths loses template
        // detection even where the templated tail lines up; no partition columns is safe, a misbound one is not.
        if (hasMixedDepth(files)) {
            return PartitionMetadata.EMPTY;
        }

        List<Map<String, String>> allRawPartitions = new ArrayList<>();

        for (StorageEntry entry : files) {
            Map<String, String> partitions = extractByTemplate(entry.path());
            if (partitions == null) {
                return PartitionMetadata.EMPTY;
            }
            allRawPartitions.add(partitions);
        }

        LinkedHashMap<String, List<String>> columnValues = Maps.newLinkedHashMapWithExpectedSize(columnCount);
        for (String col : columnNames) {
            columnValues.put(col, new ArrayList<>());
        }
        for (Map<String, String> raw : allRawPartitions) {
            for (Map.Entry<String, String> e : raw.entrySet()) {
                columnValues.get(e.getKey()).add(e.getValue());
            }
        }

        LinkedHashMap<String, DataType> partitionColumns = Maps.newLinkedHashMapWithExpectedSize(columnCount);
        for (Map.Entry<String, List<String>> e : columnValues.entrySet()) {
            partitionColumns.put(e.getKey(), HivePartitionDetector.inferType(e.getValue()));
        }

        LinkedHashMap<StoragePath, Map<String, Object>> filePartitionValues = Maps.newLinkedHashMapWithExpectedSize(files.size());
        for (int i = 0; i < files.size(); i++) {
            Map<String, String> raw = allRawPartitions.get(i);
            LinkedHashMap<String, Object> typed = Maps.newLinkedHashMapWithExpectedSize(columnCount);
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
     * Non-empty directory segments of a {@link StoragePath#path()} string, filename dropped.
     * Extract and depth use this so the skip-empty / drop-filename cut cannot drift. Rewrite
     * repeats the same cut on a {@code path.split("/")} array so it can edit slots in place.
     */
    public static List<String> directorySegments(String path) {
        if (path == null || path.isEmpty()) {
            return List.of();
        }
        List<String> nonEmpty = new ArrayList<>();
        for (String segment : path.split("/")) {
            if (segment.isEmpty() == false) {
                nonEmpty.add(segment);
            }
        }
        if (nonEmpty.isEmpty()) {
            return List.of();
        }
        nonEmpty.remove(nonEmpty.size() - 1);
        return nonEmpty;
    }

    private static int pathDepth(StoragePath storagePath) {
        return directorySegments(storagePath.path()).size();
    }

    private Map<String, String> extractByTemplate(StoragePath storagePath) {
        List<String> dirs = directorySegments(storagePath.path());
        if (dirs.size() < segments.size()) {
            return null;
        }
        int startIdx = dirs.size() - segments.size();
        LinkedHashMap<String, String> result = Maps.newLinkedHashMapWithExpectedSize(columnNames.size());
        for (int i = 0; i < segments.size(); i++) {
            String dir = dirs.get(startIdx + i);
            switch (segments.get(i)) {
                case TemplateSegment.Literal(String value) -> {
                    if (value.equals(dir) == false) {
                        return null;
                    }
                }
                case TemplateSegment.Placeholder(String name) -> result.put(name, HivePartitionDetector.decodePartitionValue(dir));
            }
        }
        return result;
    }

    /**
     * Splits {@code partition_path} into placeholders and literals. A segment is a placeholder
     * only when it is exactly {@code {name}}; everything else, including {@code year={year}}, is
     * a literal directory name. Registration and rewrite both need that distinction; the column
     * list is {@link #parseTemplateColumns}.
     */
    public static List<TemplateSegment> parseTemplate(String template) {
        if (template == null || template.isEmpty()) {
            return List.of();
        }
        List<TemplateSegment> parsed = new ArrayList<>();
        for (String segment : template.split("/")) {
            if (segment.isEmpty()) {
                continue;
            }
            Matcher m = PLACEHOLDER.matcher(segment);
            if (m.matches()) {
                parsed.add(new TemplateSegment.Placeholder(m.group(1)));
            } else {
                parsed.add(new TemplateSegment.Literal(segment));
            }
        }
        return parsed;
    }

    public static List<String> parseTemplateColumns(String template) {
        List<String> columns = new ArrayList<>();
        for (TemplateSegment segment : parseTemplate(template)) {
            if (segment instanceof TemplateSegment.Placeholder(String name)) {
                columns.add(name);
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
