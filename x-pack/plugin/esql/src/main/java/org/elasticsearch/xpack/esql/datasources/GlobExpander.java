/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Expands glob patterns and comma-separated path lists into resolved {@link FileSet} instances.
 * Delegates to {@link StorageProvider#listObjects} for directory listing and uses {@link GlobMatcher}
 * for filtering results against the glob pattern.
 * Supports partition-aware glob rewriting when filter hints are provided.
 */
final class GlobExpander {

    private GlobExpander() {}

    static boolean isMultiFile(String path) {
        if (path == null) {
            return false;
        }
        for (char c : StoragePath.GLOB_METACHARACTERS) {
            if (path.indexOf(c) >= 0) {
                return true;
            }
        }
        return path.indexOf(',') >= 0;
    }

    static FileSet expandGlob(String pattern, StorageProvider provider) throws IOException {
        return expandGlob(pattern, provider, null, true);
    }

    static FileSet expandGlob(String pattern, StorageProvider provider, @Nullable List<PartitionFilterHint> hints, boolean hivePartitioning)
        throws IOException {
        if (pattern == null) {
            throw new IllegalArgumentException("pattern cannot be null");
        }
        if (provider == null) {
            throw new IllegalArgumentException("provider cannot be null");
        }

        String effectivePattern = pattern;
        if (hints != null && hints.isEmpty() == false && hivePartitioning) {
            effectivePattern = rewriteGlobWithHints(pattern, hints);
        }

        StoragePath storagePath = StoragePath.of(effectivePattern);

        if (storagePath.isPattern() == false) {
            return FileSet.UNRESOLVED;
        }

        StoragePath prefix = storagePath.patternPrefix();
        String glob = storagePath.globPart();
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
                }
            }
        }

        if (matched.isEmpty()) {
            return FileSet.EMPTY;
        }

        matched.sort(Comparator.comparing(e -> e.path().toString()));

        PartitionMetadata partitionMetadata = null;
        if (hivePartitioning) {
            partitionMetadata = HivePartitionDetector.detect(matched);
            if (partitionMetadata.isEmpty()) {
                partitionMetadata = null;
            }
        }

        return new FileSet(matched, pattern, partitionMetadata);
    }

    static FileSet expandCommaSeparated(String pathList, StorageProvider provider) throws IOException {
        return expandCommaSeparated(pathList, provider, null, true);
    }

    static FileSet expandCommaSeparated(
        String pathList,
        StorageProvider provider,
        @Nullable List<PartitionFilterHint> hints,
        boolean hivePartitioning
    ) throws IOException {
        if (pathList == null) {
            throw new IllegalArgumentException("pathList cannot be null");
        }
        if (provider == null) {
            throw new IllegalArgumentException("provider cannot be null");
        }

        String[] segments = pathList.split(",");
        List<StorageEntry> allEntries = new ArrayList<>();

        for (String segment : segments) {
            String trimmed = segment.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            StoragePath segmentPath = StoragePath.of(trimmed);
            if (segmentPath.isPattern()) {
                FileSet expanded = expandGlob(trimmed, provider, hints, hivePartitioning);
                if (expanded.isResolved()) {
                    allEntries.addAll(expanded.files());
                }
            } else {
                if (provider.exists(segmentPath)) {
                    var obj = provider.newObject(segmentPath);
                    allEntries.add(new StorageEntry(segmentPath, obj.length(), obj.lastModified()));
                }
            }
        }

        if (allEntries.isEmpty()) {
            return FileSet.EMPTY;
        }

        allEntries.sort(Comparator.comparing(e -> e.path().toString()));

        PartitionMetadata partitionMetadata = null;
        if (hivePartitioning) {
            partitionMetadata = HivePartitionDetector.detect(allEntries);
            if (partitionMetadata.isEmpty()) {
                partitionMetadata = null;
            }
        }

        return new FileSet(allEntries, pathList, partitionMetadata);
    }

    static String rewriteGlobWithHints(String pattern, List<PartitionFilterHint> hints) {
        Map<String, PartitionFilterHint> rewritableHints = indexRewritableHints(hints);
        if (rewritableHints.isEmpty()) {
            return pattern;
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

    private static Map<String, PartitionFilterHint> indexRewritableHints(List<PartitionFilterHint> hints) {
        Map<String, PartitionFilterHint> byColumn = new HashMap<>();
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
