/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.FileFingerprint;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;

import java.util.List;

/**
 * Indexed view over a resolved set of files from an external data source.
 * Implementations are package-private within the {@code datasources.glob} package;
 * consumers should depend only on this interface.
 * <p>
 * Two sentinel instances encode distinct states:
 * <ul>
 *   <li>{@link #UNRESOLVED} — glob not yet expanded ({@code isResolved() == false, isEmpty() == false})</li>
 *   <li>{@link #EMPTY} — glob expanded but matched zero files ({@code isResolved() == true, isEmpty() == true})</li>
 * </ul>
 */
public interface FileList {

    /** Sentinel: single-file path, glob not yet applied ({@code isResolved() == false}). */
    FileList UNRESOLVED = new FileList() {
        @Override
        public int fileCount() {
            return 0;
        }

        @Override
        public StoragePath path(int i) {
            return null;
        }

        @Override
        public long size(int i) {
            return 0;
        }

        @Override
        public long lastModifiedMillis(int i) {
            return 0;
        }

        @Override
        public String originalPattern() {
            return null;
        }

        @Override
        public PartitionMetadata partitionMetadata() {
            return null;
        }

        @Override
        public boolean isResolved() {
            return false;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public long estimatedBytes() {
            return 0;
        }

        @Override
        public String toString() {
            return "FileList[UNRESOLVED]";
        }
    };

    /** Sentinel: glob expanded but matched zero files ({@code isResolved() == true, isEmpty() == true}). */
    FileList EMPTY = new FileList() {
        @Override
        public int fileCount() {
            return 0;
        }

        @Override
        public StoragePath path(int i) {
            return null;
        }

        @Override
        public long size(int i) {
            return 0;
        }

        @Override
        public long lastModifiedMillis(int i) {
            return 0;
        }

        @Override
        public String originalPattern() {
            return null;
        }

        @Override
        public PartitionMetadata partitionMetadata() {
            return null;
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public long estimatedBytes() {
            return 0;
        }

        @Override
        public String toString() {
            return "FileList[EMPTY]";
        }
    };

    int fileCount();

    StoragePath path(int i);

    long size(int i);

    long lastModifiedMillis(int i);

    @Nullable
    String originalPattern();

    @Nullable
    PartitionMetadata partitionMetadata();

    boolean isResolved();

    boolean isEmpty();

    long estimatedBytes();

    /**
     * Whether listing stopped at a caller-supplied bound rather than reaching the end of the glob, so this
     * list is a prefix of the files the pattern matches and {@link #fileCount()} is a floor, not a total.
     * <p>
     * Only a schema discovery resolution ever asks for a bound. Two invariants keep a truncated list away from
     * everything else: it is never written to the shared listing cache, where a reading query would later find
     * it and scan a fraction of the dataset, and it is never built for a query that reads rows. Both are
     * {@code ExternalSourceResolver#listingBoundFor}'s alone — the cache itself does not check, and omitting the
     * fingerprint and refusing to compact do not prevent caching. A new call site asking for a bound must
     * establish both for itself; nothing downstream catches a mistake, and a reader reached by a truncated list
     * returns silently wrong results. Correctness invariants, not optimisations.
     */
    default boolean isTruncated() {
        return false;
    }

    /**
     * The 128-bit fingerprint identifying the resolved file SET: a commutative fold over every file's
     * {@code (path, mtime, size)} plus the file count, computed once when the listing is built. The same
     * set listed in any order yields the same fingerprint; any file added, removed, or modified (mtime
     * or size) yields a different one. This makes the fingerprint a content-addressed cache key for
     * dataset-level derived state (e.g. the warm COUNT(*) aggregate): keys derived from it are
     * correct-or-miss by construction, with no separate invalidation protocol — and they survive listing
     * refreshes (the 30s listing TTL) as long as the underlying files are unchanged, because the
     * fingerprint derives from listing CONTENT, not listing object identity.
     * <p>
     * {@code null} for the sentinels and for implementations that do not compute one.
     */
    @Nullable
    default FileSetFingerprint fileSetFingerprint() {
        return null;
    }

    /**
     * The identity of file {@code i}: its path, modification time and size, hashed exactly as
     * {@link #fileSetFingerprint()} hashes each member of the set, so folding every file's fingerprint reproduces
     * the set's.
     * <p>
     * Computed from {@link #path}, {@link #lastModifiedMillis} and {@link #size} rather than stored, so every
     * implementation answers it — the compacted forms included. That is the point: the listings a resolve holds are
     * usually compacted, and a per-file identity only the uncompacted form could produce would be absent exactly
     * where it is needed.
     */
    default FileFingerprint fileFingerprint(int i) {
        return FileFingerprint.of(path(i).toString(), lastModifiedMillis(i), size(i));
    }

    /**
     * Notices raised while this listing was built: {@code file_exclusions} drops and reserved partition-name
     * renames. Empty when neither happened. Nothing is emitted from here; cached listings carry these so a cache
     * hit hands the resolver the same notices as a cold expand.
     */
    default List<String> listingWarnings() {
        return List.of();
    }

    default long listingWarningBytes() {
        List<String> warnings = listingWarnings();
        long bytes = 0;
        for (int i = 0; i < warnings.size(); i++) {
            bytes += HeapEstimates.stringBytes(warnings.get(i));
        }
        return bytes;
    }
}
