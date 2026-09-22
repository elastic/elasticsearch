/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;
import java.util.Objects;

/**
 * Represents a set of files resolved from a glob pattern or comma-separated path list.
 * Optionally carries {@link PartitionMetadata} detected from Hive-style file paths.
 */
final class GenericFileList implements FileList {

    private final List<StorageEntry> files;
    private final String originalPattern;
    private final PartitionMetadata partitionMetadata;
    @Nullable
    private final FileSetFingerprint fileSetFingerprint;
    private final List<String> listingWarnings;
    private final boolean truncated;

    GenericFileList(List<StorageEntry> files, String originalPattern) {
        this(files, originalPattern, null);
    }

    GenericFileList(List<StorageEntry> files, String originalPattern, @Nullable PartitionMetadata partitionMetadata) {
        this(files, originalPattern, partitionMetadata, List.of());
    }

    GenericFileList(
        List<StorageEntry> files,
        String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        List<String> listingWarnings
    ) {
        this(files, originalPattern, partitionMetadata, listingWarnings, false);
    }

    /**
     * @param truncated whether listing stopped at a bound before the end of the glob, so {@code files} is a
     *                  prefix of what the pattern matches. See {@link FileList#isTruncated()} for the
     *                  invariants that keep such a list away from the listing cache and from row reads.
     */
    GenericFileList(
        List<StorageEntry> files,
        String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        List<String> listingWarnings,
        boolean truncated
    ) {
        if (files == null) {
            throw new IllegalArgumentException("files cannot be null");
        }
        this.files = files;
        this.originalPattern = originalPattern;
        this.partitionMetadata = partitionMetadata;
        // The fingerprint only ever keys a dataset aggregate, which requires a multi-file listing
        // (see ExternalSourceResolver#datasetAggregateKey — fileCount >= 2). Skip the Murmur3 fold for
        // single-file listings so the common single-file resolve does not pay for machinery it cannot use.
        // Computed eagerly (once per listing build) rather than lazily: consumers need it O(1) at resolve
        // time, and construction is the one place the entry walk is already paid.
        // A truncated listing gets no fingerprint. The fingerprint identifies a file SET and keys
        // dataset-level derived state such as the warm COUNT(*) aggregate; folded over a prefix it would
        // name the whole dataset while describing a fraction of it, and the aggregate cached under it would
        // be silently wrong. Absent is correct-or-miss; present-and-partial is not.
        this.fileSetFingerprint = truncated == false && files.size() >= 2 ? FileSetFingerprints.compute(files) : null;
        this.truncated = truncated;
        this.listingWarnings = listingWarnings == null || listingWarnings.isEmpty() ? List.of() : List.copyOf(listingWarnings);
    }

    List<StorageEntry> files() {
        return files;
    }

    @Override
    public String originalPattern() {
        return originalPattern;
    }

    @Override
    @Nullable
    public PartitionMetadata partitionMetadata() {
        return partitionMetadata;
    }

    int size() {
        return files.size();
    }

    @Override
    public int fileCount() {
        return files.size();
    }

    @Override
    public StoragePath path(int i) {
        return files.get(i).path();
    }

    @Override
    public long size(int i) {
        return files.get(i).length();
    }

    @Override
    public long lastModifiedMillis(int i) {
        return files.get(i).lastModified().toEpochMilli();
    }

    @Override
    public long estimatedBytes() {
        // 64B object header + ~700B per StorageEntry (path String + Instant + long)
        return 64 + files.size() * 700L + listingWarningBytes();
    }

    @Override
    public List<String> listingWarnings() {
        return listingWarnings;
    }

    @Override
    public boolean isTruncated() {
        return truncated;
    }

    @Override
    @Nullable
    public FileSetFingerprint fileSetFingerprint() {
        return fileSetFingerprint;
    }

    @Override
    public boolean isResolved() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return files.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericFileList other = (GenericFileList) o;
        return truncated == other.truncated
            && Objects.equals(files, other.files)
            && Objects.equals(originalPattern, other.originalPattern)
            && Objects.equals(partitionMetadata, other.partitionMetadata)
            && Objects.equals(listingWarnings, other.listingWarnings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(files, originalPattern, partitionMetadata, listingWarnings, truncated);
    }

    @Override
    public String toString() {
        return "GenericFileList[" + files.size() + " files, pattern=" + originalPattern + "]";
    }
}
