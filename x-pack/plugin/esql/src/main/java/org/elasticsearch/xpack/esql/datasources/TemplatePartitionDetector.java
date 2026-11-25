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
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
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
        Set<String> seenSurfaces = new HashSet<>();
        Set<String> seenRenamed = new HashSet<>();
        for (TemplateSegment segment : parsed) {
            switch (segment) {
                case TemplateSegment.Placeholder(String name) -> {
                    String surface = ReservedPartitionNames.surface(name);
                    if (surface.equals(name) == false && seenRenamed.add(name)) {
                        renamed.add(name);
                    }
                    if (seenSurfaces.add(surface)) {
                        surfaced.add(surface);
                    }
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
    public PartitionMetadata detect(List<StorageEntry> files, Consumer<String> warningSink) {
        Objects.requireNonNull(warningSink, "warningSink: a null sink would fall back to HeaderWarning off the request thread");
        if (files == null || files.isEmpty()) {
            return PartitionMetadata.EMPTY;
        }
        // Columns are known from the template, so one pass fills a String[] per column. No per-file map.
        // Every file must sit at the same directory depth. The template binds the last N directories before the
        // filename (placeholders and literals), so files at different depths bind different physical levels to the
        // same column: over data/2024/f1, data/2024/01/f2 and data/2024/01/15/f3 with template {year}, the three
        // files would report year=2024, year=01 and year=15, and a STATS BY year would bucket a day value as a year.
        // Bailing to EMPTY is the same all-or-nothing stance HivePartitionDetector takes when its key sets disagree
        // across files. The cost is that a comma-separated list mixing prefixes of different depths loses template
        // detection even where the templated tail lines up; no partition columns is safe, a misbound one is not.
        int columnCount = columnNames.size();
        int fileCount = files.size();
        String[][] rawValues = new String[columnCount][fileCount];
        boolean[] written = new boolean[columnCount];
        int depth = -1;
        for (int i = 0; i < fileCount; i++) {
            StoragePath path = files.get(i).path();
            int fileDepth = pathDepth(path);
            if (depth == -1) {
                depth = fileDepth;
            } else if (fileDepth != depth) {
                return PartitionMetadata.EMPTY;
            }
            if (fillColumns(path, rawValues, i, written) == false) {
                return PartitionMetadata.EMPTY;
            }
        }

        LinkedHashMap<String, DataType> partitionColumns = Maps.newLinkedHashMapWithExpectedSize(columnCount);
        Object[][] valuesByColumn = new Object[columnCount][];
        HivePartitionDetector.CastInterner interner = new HivePartitionDetector.CastInterner();
        for (int c = 0; c < columnCount; c++) {
            DataType type = HivePartitionDetector.inferType(Arrays.asList(rawValues[c]));
            partitionColumns.put(columnNames.get(c), type);
            Object[] column = new Object[fileCount];
            String[] rawColumn = rawValues[c];
            for (int i = 0; i < fileCount; i++) {
                column[i] = HivePartitionDetector.castValue(rawColumn[i], type, interner);
            }
            valuesByColumn[c] = column;
        }

        // Only now is the rename true: the bail-outs above surface no partition column at all, and a notice raised
        // before them would ride the cached listing into every later run.
        ReservedPartitionNames.warnRenamed(renamedColumns, warningSink);
        return PartitionMetadata.columnar(partitionColumns, valuesByColumn, fileCount);
    }

    /**
     * Non-empty directory segments of a {@link StoragePath#path()} string, filename dropped.
     * Delegates to {@link HivePartitionDetector#directorySegments} so the skip-empty / drop-filename
     * cut cannot drift. Rewrite repeats the same cut on a {@code path.split("/")} array so it can
     * edit slots in place.
     */
    public static List<String> directorySegments(String path) {
        return HivePartitionDetector.directorySegments(path);
    }

    /**
     * The directory value {@code template} binds to {@code column} on a {@link StoragePath#path()}, or {@code null}
     * when the template does not bind that column here. Same alignment, literal check, and percent-decode as
     * {@link #detect}, so a range filter sees the string detection will type — including a default-partition sentinel,
     * which is text and must widen the column rather than be dropped as a number.
     */
    @Nullable
    public static String columnValue(String path, String column, String template) {
        List<TemplateSegment> segments = parseTemplate(template);
        List<String> dirs = directorySegments(path);
        if (dirs.size() < segments.size()) {
            return null;
        }
        int start = dirs.size() - segments.size();
        String bound = null;
        for (int i = 0; i < segments.size(); i++) {
            String dir = dirs.get(start + i);
            switch (segments.get(i)) {
                case TemplateSegment.Literal(String value) -> {
                    if (value.equals(dir) == false) {
                        return null;
                    }
                }
                case TemplateSegment.Placeholder(String name) -> {
                    if (column.equals(ReservedPartitionNames.surface(name)) == false) {
                        continue;
                    }
                    String decoded = HivePartitionDetector.decodePartitionValue(dir);
                    if (bound != null && bound.equals(decoded) == false) {
                        return null;
                    }
                    bound = decoded;
                }
            }
        }
        return bound;
    }

    private static int pathDepth(StoragePath storagePath) {
        return directorySegments(storagePath.path()).size();
    }

    /**
     * Writes this file's template column tokens into {@code rawValues}. A repeated placeholder must bind the
     * same decoded value. Returns {@code false} when the path is shorter than the template, a literal does not
     * match, or a repeated placeholder disagrees with itself.
     */
    private boolean fillColumns(StoragePath storagePath, String[][] rawValues, int fileIndex, boolean[] written) {
        Arrays.fill(written, false);
        List<String> dirs = directorySegments(storagePath.path());
        if (dirs.size() < segments.size()) {
            return false;
        }
        int startIdx = dirs.size() - segments.size();
        for (int i = 0; i < segments.size(); i++) {
            String dir = dirs.get(startIdx + i);
            switch (segments.get(i)) {
                case TemplateSegment.Literal(String value) -> {
                    if (value.equals(dir) == false) {
                        return false;
                    }
                }
                case TemplateSegment.Placeholder(String name) -> {
                    int col = columnIndex(name);
                    String decoded = HivePartitionDetector.decodePartitionValue(dir);
                    // A null token does not count as a previous binding: the map put in the old detector
                    // overwrote it and only rejected a non-null value that disagreed.
                    if (written[col]) {
                        String previous = rawValues[col][fileIndex];
                        if (previous != null && previous.equals(decoded) == false) {
                            return false;
                        }
                    }
                    rawValues[col][fileIndex] = decoded;
                    written[col] = true;
                }
            }
        }
        return true;
    }

    private int columnIndex(String name) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equals(name)) {
                return i;
            }
        }
        throw new IllegalStateException("template column [" + name + "] missing from column list");
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

    /** Whether {@code segment} embeds a {@code {name}} placeholder without being exactly one. */
    static boolean containsEmbeddedPlaceholder(String segment) {
        return PLACEHOLDER.matcher(segment).find();
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

    /**
     * Whether {@code globPart} is a fixed-depth TEMPLATE walk shape: no {@code **}, every directory
     * slot is either {@code *} or a glob-free literal, and at least one directory {@code *} remains.
     * Alignment against the prefix is {@link #walkLayout}; this is only the glob-side gate.
     */
    public static boolean walkableGlob(String glob) {
        if (glob == null || glob.isEmpty() || glob.contains("**")) {
            return false;
        }
        String[] parts = glob.split("/");
        if (parts.length < 2) {
            return false;
        }
        boolean starDir = false;
        for (int i = 0; i < parts.length - 1; i++) {
            if (isSimpleDirSegment(parts[i]) == false) {
                return false;
            }
            if ("*".equals(parts[i])) {
                starDir = true;
            }
        }
        return starDir;
    }

    /**
     * Maps remaining glob-directory levels onto {@code template} by the same right-alignment rewrite
     * and {@link #columnValue} use. {@code null} when the glob is not a fixed-depth walk, the prefix
     * plus glob cannot cover the template, or a glob literal disagrees with a template literal.
     */
    @Nullable
    public static WalkLayout walkLayout(StoragePath prefix, String glob, String template) {
        if (prefix == null || walkableGlob(glob) == false || template == null || template.isEmpty()) {
            return null;
        }
        List<TemplateSegment> segments = parseTemplate(template);
        if (hasPlaceholder(segments) == false) {
            return null;
        }
        String[] globParts = glob.split("/");
        int globDirCount = globParts.length - 1;
        List<String> prefixDirs = nonEmptySegments(prefix.path());
        int fullDirCount = prefixDirs.size() + globDirCount;
        if (fullDirCount < segments.size()) {
            return null;
        }
        int offset = fullDirCount - segments.size();
        List<WalkLayout.Level> levels = new ArrayList<>(globDirCount);
        Set<String> keys = new HashSet<>();
        boolean placeholderLevel = false;
        for (int i = 0; i < globDirCount; i++) {
            int templateIdx = prefixDirs.size() + i - offset;
            if (templateIdx < 0) {
                levels.add(WalkLayout.Level.FREE);
                continue;
            }
            switch (segments.get(templateIdx)) {
                case TemplateSegment.Placeholder(String name) -> {
                    if ("*".equals(globParts[i]) == false) {
                        return null;
                    }
                    String surface = ReservedPartitionNames.surface(name);
                    levels.add(WalkLayout.Level.placeholder(surface));
                    keys.add(surface);
                    placeholderLevel = true;
                }
                case TemplateSegment.Literal(String value) -> {
                    if ("*".equals(globParts[i]) == false && value.equals(globParts[i]) == false) {
                        return null;
                    }
                    if ("*".equals(globParts[i])) {
                        levels.add(WalkLayout.Level.literal(value));
                    } else {
                        levels.add(WalkLayout.Level.globSpelled(globParts[i]));
                    }
                }
            }
        }
        if (placeholderLevel == false) {
            return null;
        }
        LinkedHashMap<String, String> prefixBindings = new LinkedHashMap<>();
        for (int t = 0; t < segments.size(); t++) {
            int fullIdx = offset + t;
            if (fullIdx >= prefixDirs.size()) {
                break;
            }
            String dir = prefixDirs.get(fullIdx);
            switch (segments.get(t)) {
                case TemplateSegment.Placeholder(String name) -> prefixBindings.put(
                    ReservedPartitionNames.surface(name),
                    HivePartitionDetector.decodePartitionValue(dir)
                );
                case TemplateSegment.Literal(String value) -> {
                    if (value.equals(dir) == false) {
                        return null;
                    }
                }
            }
        }
        return new WalkLayout(List.copyOf(levels), Set.copyOf(keys), Map.copyOf(prefixBindings), offset, List.copyOf(segments));
    }

    private static boolean hasPlaceholder(List<TemplateSegment> segments) {
        for (TemplateSegment segment : segments) {
            if (segment instanceof TemplateSegment.Placeholder) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSimpleDirSegment(String segment) {
        if ("*".equals(segment)) {
            return true;
        }
        for (char c : StoragePath.GLOB_METACHARACTERS) {
            if (segment.indexOf(c) >= 0) {
                return false;
            }
        }
        return true;
    }

    /** Non-empty path segments, filename not dropped — a directory path keeps its last folder. */
    static List<String> nonEmptySegments(String path) {
        if (path == null || path.isEmpty()) {
            return List.of();
        }
        List<String> nonEmpty = new ArrayList<>();
        for (String segment : path.split("/")) {
            if (segment.isEmpty() == false) {
                nonEmpty.add(segment);
            }
        }
        return nonEmpty;
    }

    /**
     * Remaining glob-directory roles for a TEMPLATE walk. {@link #prefixBindings()} are placeholders
     * already spelled in the listing prefix (exact-string splice); {@link #bindings(String)} adds
     * folders listed so far, including the current directory.
     */
    public record WalkLayout(
        List<Level> levels,
        Set<String> placeholderKeys,
        Map<String, String> prefixBindings,
        int dirOffset,
        List<TemplateSegment> segments
    ) {
        public Level level(int walkLevel) {
            return levels.get(walkLevel);
        }

        public int depth() {
            return levels.size();
        }

        /**
         * Template placeholder bindings of a directory path, using this layout's right-align offset
         * so a partial path does not bind the wrong slots.
         */
        public Map<String, String> bindings(String directoryPath) {
            List<String> dirs = nonEmptySegments(directoryPath);
            LinkedHashMap<String, String> into = new LinkedHashMap<>(prefixBindings);
            for (int t = 0; t < segments.size(); t++) {
                int fullIdx = dirOffset + t;
                if (fullIdx >= dirs.size()) {
                    break;
                }
                if (segments.get(t) instanceof TemplateSegment.Placeholder(String name)) {
                    into.put(ReservedPartitionNames.surface(name), HivePartitionDetector.decodePartitionValue(dirs.get(fullIdx)));
                }
            }
            return into;
        }

        /** One remaining glob-directory slot. */
        public record Level(@Nullable String name, @Nullable String literal, boolean globSpelled) {
            static final Level FREE = new Level(null, null, false);

            static Level placeholder(String name) {
                return new Level(name, null, false);
            }

            /** Template literal filled from a {@code *} glob slot: a sibling folder withdraws. */
            static Level literal(String value) {
                return new Level(null, value, false);
            }

            /** Glob already spelled this name: a sibling cannot match and is skipped. */
            static Level globSpelled(String value) {
                return new Level(null, value, true);
            }

            public boolean isPlaceholder() {
                return name != null;
            }

            public boolean isLiteral() {
                return literal != null;
            }

            public String decodedFolder(String folderName) {
                return HivePartitionDetector.decodePartitionValue(folderName);
            }
        }
    }
}
