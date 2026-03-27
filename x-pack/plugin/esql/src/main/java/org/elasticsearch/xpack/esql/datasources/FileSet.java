/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.List;
import java.util.Objects;

/**
 * Represents a set of files resolved from a glob pattern or comma-separated path list.
 * Uses identity-comparable sentinels for unresolved and empty states.
 */
public final class FileSet {

    /** Single-file path, no glob applied yet. */
    public static final FileSet UNRESOLVED = new FileSet(List.of(), null);

    /** Glob matched zero files. */
    public static final FileSet EMPTY = new FileSet(List.of(), null);

    private final List<StorageEntry> files;
    private final String originalPattern;

    public FileSet(List<StorageEntry> files, String originalPattern) {
        if (files == null) {
            throw new IllegalArgumentException("files cannot be null");
        }
        this.files = List.copyOf(files);
        this.originalPattern = originalPattern;
    }

    public List<StorageEntry> files() {
        return files;
    }

    public String originalPattern() {
        return originalPattern;
    }

    public int size() {
        return files.size();
    }

    public boolean isUnresolved() {
        return this == UNRESOLVED;
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    public boolean isResolved() {
        return this != UNRESOLVED && this != EMPTY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileSet other = (FileSet) o;
        // Sentinels use identity comparison only (this != o already checked above)
        if (this == UNRESOLVED || this == EMPTY || other == UNRESOLVED || other == EMPTY) {
            return false;
        }
        return Objects.equals(files, other.files) && Objects.equals(originalPattern, other.originalPattern);
    }

    @Override
    public int hashCode() {
        if (this == UNRESOLVED || this == EMPTY) {
            return System.identityHashCode(this);
        }
        return Objects.hash(files, originalPattern);
    }

    @Override
    public String toString() {
        if (this == UNRESOLVED) {
            return "FileSet[UNRESOLVED]";
        }
        if (this == EMPTY) {
            return "FileSet[EMPTY]";
        }
        return "FileSet[" + files.size() + " files, pattern=" + originalPattern + "]";
    }
}
