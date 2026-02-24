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
        Map<String, PartitionFilterHint> hintsByColumn = new HashMap<>();
        for (PartitionFilterHint hint : hints) {
            if ("=".equals(hint.operator()) || "IN".equals(hint.operator())) {
                hintsByColumn.putIfAbsent(hint.columnName(), hint);
            }
        }

        if (hintsByColumn.isEmpty()) {
            return pattern;
        }

        StringBuilder result = new StringBuilder();
        String[] parts = pattern.split("/");
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                result.append('/');
            }
            String part = parts[i];
            int eqIdx = part.indexOf('=');
            if (eqIdx > 0 && eqIdx < part.length() - 1) {
                String key = part.substring(0, eqIdx);
                String valuePart = part.substring(eqIdx + 1);
                PartitionFilterHint hint = hintsByColumn.get(key);
                if (hint != null && isWildcard(valuePart)) {
                    if ("=".equals(hint.operator()) && hint.values().size() == 1) {
                        result.append(key).append('=').append(escapeGlobMeta(String.valueOf(hint.values().get(0))));
                    } else if ("IN".equals(hint.operator()) && hint.values().size() > 1) {
                        result.append(key).append("={");
                        for (int j = 0; j < hint.values().size(); j++) {
                            if (j > 0) {
                                result.append(',');
                            }
                            result.append(escapeGlobMeta(String.valueOf(hint.values().get(j))));
                        }
                        result.append('}');
                    } else if ("IN".equals(hint.operator()) && hint.values().size() == 1) {
                        result.append(key).append('=').append(escapeGlobMeta(String.valueOf(hint.values().get(0))));
                    } else {
                        result.append(part);
                    }
                    continue;
                }
            }
            result.append(part);
        }

        return result.toString();
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

    private static boolean isWildcard(String value) {
        return "*".equals(value);
    }
}
