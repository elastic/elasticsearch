/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.AutoPartitionDetector;
import org.elasticsearch.xpack.esql.datasources.HivePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.PartitionConfig;
import org.elasticsearch.xpack.esql.datasources.PartitionDetector;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.TemplatePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Expands glob patterns and comma-separated path lists into resolved {@link FileList} instances.
 * Delegates to {@link StorageProvider#listObjects} for directory listing and uses {@link GlobMatcher}
 * for filtering results against the glob pattern.
 * Supports partition-aware glob rewriting when filter hints are provided.
 */
public final class GlobExpander {

    private GlobExpander() {}

    /** Creates a file list from raw entries. Primarily for tests. */
    public static FileList fileListOf(List<StorageEntry> entries, String pattern) {
        return new GenericFileList(entries, pattern);
    }

    /** Creates a file list from raw entries with partition metadata. Primarily for tests. */
    public static FileList fileListOf(List<StorageEntry> entries, String pattern, @Nullable PartitionMetadata partitionMetadata) {
        return new GenericFileList(entries, pattern, partitionMetadata);
    }

    /** Compresses a raw file list into a compact representation (dictionary or Hive-partitioned). */
    public static FileList compact(FileList raw, String basePath) {
        if (raw instanceof GenericFileList generic) {
            return FileListCompactor.compact(basePath, generic);
        }
        return raw;
    }

    /** Returns a copy of the file list with per-file schema reconciliation info attached. */
    public static FileList withSchemaInfo(FileList fileList, Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo) {
        if (fileList instanceof GenericFileList generic) {
            return generic.withSchemaInfo(schemaInfo);
        }
        return fileList;
    }

    /**
     * Expands a glob/comma pattern and compresses the result into a compact representation
     * (DictionaryFileList or HiveFileList). This is the primary entry point for the resolver.
     */
    public static FileList expandAndCompact(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning,
        StoragePath storagePath
    ) throws IOException {
        return expandAndCompact(path, provider, hints, hivePartitioning, storagePath, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Expands a glob/comma pattern and compresses the result, with safety caps on discovery.
     */
    public static FileList expandAndCompact(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning,
        StoragePath storagePath,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        FileList expanded = path.indexOf(',') >= 0
            ? doExpandCommaSeparated(path, provider, hints, hivePartitioning, null, null, maxDiscoveredFiles, maxGlobExpansion)
            : doExpandGlob(path, provider, hints, hivePartitioning, null, null, maxDiscoveredFiles, maxGlobExpansion);
        if (expanded.isResolved() == false || expanded.fileCount() == 0) {
            return expanded;
        }
        if (expanded instanceof GenericFileList raw) {
            String basePath = storagePath.patternPrefix().toString();
            return FileListCompactor.compact(basePath, raw);
        }
        return expanded;
    }

    public static PartitionDetector resolveDetector(PartitionConfig config) {
        if (config == null) {
            return HivePartitionDetector.INSTANCE;
        }
        return switch (config.strategy()) {
            case NONE -> null;
            case HIVE -> HivePartitionDetector.INSTANCE;
            case TEMPLATE -> {
                String template = config.pathTemplate();
                if (template == null || template.isEmpty()) {
                    yield null;
                }
                yield new TemplatePartitionDetector(template);
            }
            case AUTO -> AutoPartitionDetector.fromConfig(config);
        };
    }

    /**
     * Returns true if the given path string represents multiple files — either because it contains
     * glob metacharacters in the path component, or because it is a comma-separated list.
     *
     * IPv6 host literals in URL authorities (e.g. {@code http://[::1]/data/*.parquet}) use bracket
     * notation per RFC 3986 §3.2.2. Those brackets are parsed as part of the authority, not the
     * path, so they are not treated as glob character-class syntax.
     */
    public static boolean isMultiFile(String path) {
        if (path == null) {
            return false;
        }
        if (path.indexOf(',') >= 0) {
            return true;
        }
        // Only scan the path component for glob metacharacters, not the full URL string.
        // This prevents IPv6 bracket notation in the authority from being mistaken for
        // a glob character class.
        try {
            return StoragePath.of(path).isPattern();
        } catch (IllegalArgumentException e) {
            // Not a parseable URL; fall back to scanning the whole string
            for (char c : StoragePath.GLOB_METACHARACTERS) {
                if (path.indexOf(c) >= 0) {
                    return true;
                }
            }
            return false;
        }
    }

    public static FileList expandGlob(String pattern, StorageProvider provider) throws IOException {
        return expandGlob(pattern, provider, null, true);
    }

    public static FileList expandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning,
        @Nullable PartitionConfig partitionConfig,
        @Nullable Map<String, Object> config
    ) throws IOException {
        return doExpandGlob(pattern, provider, hints, hivePartitioning, partitionConfig, config, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public static FileList expandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning
    ) throws IOException {
        return doExpandGlob(pattern, provider, hints, hivePartitioning, null, null, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public static FileList expandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        return doExpandGlob(pattern, provider, hints, hivePartitioning, null, null, maxDiscoveredFiles, maxGlobExpansion);
    }

    static FileList doExpandGlob(
        String pattern,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning,
        @Nullable PartitionConfig partitionConfig,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        Check.notNull(pattern, "pattern cannot be null");
        Check.notNull(provider, "provider cannot be null");

        String effectivePattern = pattern;
        if (hints != null && hints.isEmpty() == false && hivePartitioning) {
            effectivePattern = rewriteGlobWithHints(pattern, hints, partitionConfig);
        }

        StoragePath storagePath = StoragePath.of(effectivePattern);

        if (storagePath.isPattern() == false) {
            if (effectivePattern.equals(pattern)) {
                return FileList.UNRESOLVED;
            }
            // Hints resolved all wildcards to a concrete path — resolve via exists()
            var obj = provider.newObject(storagePath);
            if (obj.exists()) {
                StorageEntry entry = new StorageEntry(storagePath, obj.length(), obj.lastModified());
                PartitionMetadata partitionMetadata = detectPartitions(List.of(entry), hivePartitioning, partitionConfig, config);
                return new GenericFileList(List.of(entry), pattern, partitionMetadata);
            }
            return FileList.EMPTY;
        }

        StoragePath prefix = storagePath.patternPrefix();
        String glob = storagePath.globPart();

        // Brace-only fast path: use exists()+newObject() instead of listing
        if (BraceExpander.isBraceOnly(glob)) {
            List<String> candidates = BraceExpander.expand(glob, maxGlobExpansion);
            if (candidates != null) {
                List<StorageEntry> matched = new ArrayList<>();
                String prefixStr = prefix.toString();
                for (String candidate : candidates) {
                    StoragePath fullPath = StoragePath.of(prefixStr + candidate);
                    var obj = provider.newObject(fullPath);
                    if (obj.exists()) {
                        matched.add(new StorageEntry(fullPath, obj.length(), obj.lastModified()));
                    }
                    checkDiscoveredFilesLimit(matched.size(), maxDiscoveredFiles);
                }
                if (matched.isEmpty()) {
                    return FileList.EMPTY;
                }
                matched.sort(Comparator.comparing(e -> e.path().toString()));
                PartitionMetadata partitionMetadata = detectPartitions(matched, hivePartitioning, partitionConfig, config);
                return new GenericFileList(matched, pattern, partitionMetadata);
            }
            // candidates == null means expansion exceeded cap; fall through to listing
        }

        GlobMatcher matcher = new GlobMatcher(glob);
        boolean recursive = matcher.needsRecursion();

        List<StorageEntry> matched = new ArrayList<>();
        String prefixStr = prefix.toString();

        try (StorageIterator iterator = provider.listObjects(prefix, recursive)) {
            while (iterator.hasNext()) {
                StorageEntry entry = iterator.next();
                String entryPath = entry.path().toString();
                String relativePath;
                if (entryPath.startsWith(prefixStr)) {
                    relativePath = entryPath.substring(prefixStr.length());
                } else {
                    relativePath = entry.path().objectName();
                }
                if (matcher.matches(relativePath)) {
                    matched.add(entry);
                    checkDiscoveredFilesLimit(matched.size(), maxDiscoveredFiles);
                }
            }
        }

        if (matched.isEmpty()) {
            return FileList.EMPTY;
        }

        matched.sort(Comparator.comparing(e -> e.path().toString()));

        PartitionMetadata partitionMetadata = detectPartitions(matched, hivePartitioning, partitionConfig, config);

        return new GenericFileList(matched, pattern, partitionMetadata);
    }

    static PartitionMetadata detectPartitions(
        List<StorageEntry> files,
        boolean hivePartitioning,
        @Nullable PartitionConfig partitionConfig,
        @Nullable Map<String, Object> config
    ) {
        if (hivePartitioning == false && partitionConfig == null) {
            return null;
        }
        if (partitionConfig != null && PartitionConfig.Strategy.NONE == partitionConfig.strategy()) {
            return null;
        }

        PartitionDetector detector = resolveDetector(partitionConfig);
        if (detector == null) {
            if (hivePartitioning) {
                detector = HivePartitionDetector.INSTANCE;
            } else {
                return null;
            }
        }

        PartitionMetadata result = detector.detect(files, config);
        if (result == null || result.isEmpty()) {
            return null;
        }
        return result;
    }

    private static void checkDiscoveredFilesLimit(int discoveredCount, int maxDiscoveredFiles) {
        if (discoveredCount > maxDiscoveredFiles) {
            throw new QlIllegalArgumentException(
                "Glob pattern discovered too many files ({}, limit {}). "
                    + "Narrow your glob pattern, add partition filters, "
                    + "or increase the [esql.external.max_discovered_files] cluster setting.",
                discoveredCount,
                maxDiscoveredFiles
            );
        }
    }

    public static FileList expandCommaSeparated(String pathList, StorageProvider provider) throws IOException {
        return expandCommaSeparated(pathList, provider, null, true);
    }

    public static FileList expandCommaSeparated(
        String pathList,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning
    ) throws IOException {
        return doExpandCommaSeparated(pathList, provider, hints, hivePartitioning, null, null, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public static FileList expandCommaSeparated(
        String pathList,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        return doExpandCommaSeparated(pathList, provider, hints, hivePartitioning, null, null, maxDiscoveredFiles, maxGlobExpansion);
    }

    private static FileList doExpandCommaSeparated(
        String pathList,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning,
        @Nullable PartitionConfig partitionConfig,
        @Nullable Map<String, Object> config,
        int maxDiscoveredFiles,
        int maxGlobExpansion
    ) throws IOException {
        Check.notNull(pathList, "pathList cannot be null");
        Check.notNull(provider, "provider cannot be null");

        String[] segments = pathList.split(",");
        List<StorageEntry> allEntries = new ArrayList<>();

        for (String segment : segments) {
            String trimmed = segment.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            StoragePath segmentPath = StoragePath.of(trimmed);
            if (segmentPath.isPattern()) {
                int remainingBudget = maxDiscoveredFiles - allEntries.size();
                FileList expanded = doExpandGlob(
                    trimmed,
                    provider,
                    hints,
                    hivePartitioning,
                    partitionConfig,
                    config,
                    remainingBudget,
                    maxGlobExpansion
                );
                if (expanded instanceof GenericFileList g && expanded.fileCount() > 0) {
                    allEntries.addAll(g.files());
                }
            } else {
                var obj = provider.newObject(segmentPath);
                if (obj.exists()) {
                    allEntries.add(new StorageEntry(segmentPath, obj.length(), obj.lastModified()));
                    checkDiscoveredFilesLimit(allEntries.size(), maxDiscoveredFiles);
                }
            }
        }

        if (allEntries.isEmpty()) {
            return FileList.EMPTY;
        }

        allEntries.sort(Comparator.comparing(e -> e.path().toString()));

        PartitionMetadata partitionMetadata = detectPartitions(allEntries, hivePartitioning, partitionConfig, config);

        return new GenericFileList(allEntries, pathList, partitionMetadata);
    }

    public static String rewriteGlobWithHints(String pattern, List<PartitionFilterHint> hints) {
        return rewriteGlobWithHints(pattern, hints, null);
    }

    public static String rewriteGlobWithHints(String pattern, List<PartitionFilterHint> hints, @Nullable PartitionConfig partitionConfig) {
        Map<String, PartitionFilterHint> rewritableHints = indexRewritableHints(hints);
        if (rewritableHints.isEmpty()) {
            return pattern;
        }

        if (partitionConfig != null && partitionConfig.pathTemplate() != null) {
            String templateRewritten = rewriteGlobWithTemplate(pattern, rewritableHints, partitionConfig.pathTemplate());
            if (templateRewritten != null) {
                return templateRewritten;
            }
        }

        String[] segments = pattern.split("/");
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                result.append('/');
            }
            result.append(rewriteSegment(segments[i], rewritableHints));
        }
        return result.toString();
    }

    public static String rewriteGlobWithTemplate(String pattern, Map<String, PartitionFilterHint> rewritableHints, String template) {
        List<String> templateColumns = TemplatePartitionDetector.parseTemplateColumns(template);
        if (templateColumns.isEmpty()) {
            return null;
        }

        String[] segments = pattern.split("/");
        List<Integer> wildcardPositions = new ArrayList<>();
        for (int i = 0; i < segments.length; i++) {
            if ("*".equals(segments[i])) {
                wildcardPositions.add(i);
            }
        }

        if (wildcardPositions.size() < templateColumns.size()) {
            return null;
        }

        // Map template columns to wildcard positions (positional mapping)
        int offset = wildcardPositions.size() - templateColumns.size();
        boolean changed = false;
        for (int t = 0; t < templateColumns.size(); t++) {
            String colName = templateColumns.get(t);
            PartitionFilterHint hint = rewritableHints.get(colName);
            if (hint == null) {
                continue;
            }
            int segIdx = wildcardPositions.get(offset + t);
            List<Object> values = hint.values();
            if (hint.isSingleValue()) {
                segments[segIdx] = escapeGlobMeta(String.valueOf(values.get(0)));
            } else {
                StringBuilder sb = new StringBuilder("{");
                for (int v = 0; v < values.size(); v++) {
                    if (v > 0) {
                        sb.append(',');
                    }
                    sb.append(escapeGlobMeta(String.valueOf(values.get(v))));
                }
                segments[segIdx] = sb.append('}').toString();
            }
            changed = true;
        }

        if (changed == false) {
            return null;
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                result.append('/');
            }
            result.append(segments[i]);
        }
        return result.toString();
    }

    private static Map<String, PartitionFilterHint> indexRewritableHints(List<PartitionFilterHint> hints) {
        Map<String, PartitionFilterHint> byColumn = Maps.newHashMapWithExpectedSize(hints.size());
        for (PartitionFilterHint hint : hints) {
            if (hint.operator().canRewriteGlob()) {
                byColumn.putIfAbsent(hint.columnName(), hint);
            }
        }
        return byColumn;
    }

    private static String rewriteSegment(String segment, Map<String, PartitionFilterHint> rewritableHints) {
        int eqIdx = segment.indexOf('=');
        if (eqIdx <= 0 || eqIdx >= segment.length() - 1) {
            return segment;
        }

        String key = segment.substring(0, eqIdx);
        String valuePart = segment.substring(eqIdx + 1);
        if ("*".equals(valuePart) == false) {
            return segment;
        }

        PartitionFilterHint hint = rewritableHints.get(key);
        if (hint == null) {
            return segment;
        }

        List<Object> values = hint.values();
        if (hint.isSingleValue()) {
            return key + "=" + escapeGlobMeta(String.valueOf(values.get(0)));
        }

        // Multiple values: use glob brace syntax key={v1,v2,...}
        StringBuilder sb = new StringBuilder(key).append("={");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(escapeGlobMeta(String.valueOf(values.get(i))));
        }
        return sb.append('}').toString();
    }

    private static String escapeGlobMeta(String value) {
        if (value.indexOf('*') < 0 && value.indexOf('?') < 0 && value.indexOf('[') < 0 && value.indexOf('{') < 0) {
            return value;
        }
        StringBuilder sb = new StringBuilder(value.length() + 4);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '*' || c == '?' || c == '[' || c == '{') {
                sb.append('[').append(c).append(']');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
