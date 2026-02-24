/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Expands glob patterns and comma-separated path lists into resolved {@link FileSet} instances.
 * Delegates to {@link StorageProvider#listObjects} for directory listing and uses {@link GlobMatcher}
 * for filtering results against the glob pattern.
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
        if (pattern == null) {
            throw new IllegalArgumentException("pattern cannot be null");
        }
        if (provider == null) {
            throw new IllegalArgumentException("provider cannot be null");
        }

        StoragePath storagePath = StoragePath.of(pattern);

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
                // Compute the relative path by stripping the prefix
                String entryPath = entry.path().toString();
                String relativePath;
                if (entryPath.startsWith(prefixStr)) {
                    relativePath = entryPath.substring(prefixStr.length());
                } else {
                    // Fall back to using just the object name
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
        return new FileSet(matched, pattern);
    }

    static FileSet expandCommaSeparated(String pathList, StorageProvider provider) throws IOException {
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
                FileSet expanded = expandGlob(trimmed, provider);
                if (expanded.isResolved()) {
                    allEntries.addAll(expanded.files());
                }
            } else {
                // Literal path -- verify existence
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
        return new FileSet(allEntries, pathList);
    }
}
