/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.util.List;
import java.util.Set;

/**
 * What phase 2 will allocate for a listing, in bytes, before it allocates it.
 * <p>
 * Here rather than with either charger because two of them count over the same files and must not drift. The
 * coordinator charges before discovery from the list the plan carries; the provider charges for the file set it
 * discovered itself, when that list was only a prefix. {@link FileList#isTruncated()} decides which of the two
 * runs, and {@link #bytesFor} returning zero for a truncated list is how it says so - a prefix's structures are
 * never built, because discovery replaces the list before anything is allocated over it.
 */
public final class Phase2Reservation {

    /**
     * Not measured deep sizes. {@link #SHELL_BYTES} is one split shell per file. A map of {@code k} keys is
     * {@link #MAP_OVERHEAD_BYTES} {@code +} {@link #ENTRY_BYTES} {@code * k}. An empty map is free: the real hold
     * is {@link java.util.Map#of()}. {@link #VIEW_BYTES} is the overlay wrapper, billed only when both layers are
     * non-empty; the wrapper walks the shared tuple instead of copying its entries, so 64 bytes bounds it.
     * <p>
     * Counted per file, not per split - a text or compressed file can become many splits, and that count is only
     * known after the discovery this reservation precedes, so those files are under-charged.
     */
    public static final long SHELL_BYTES = 160L;
    public static final long ENTRY_BYTES = 64L;
    public static final long MAP_OVERHEAD_BYTES = 552L;
    public static final long VIEW_BYTES = 64L;

    private Phase2Reservation() {}

    /**
     * Survivor-map bytes plus one shell per file. Directory-constant keys are billed once per shared partition
     * row. No metadata, or one row per file, bills those keys per file (an upper bound: filters may drop files
     * after this charge). A source that retains nothing bills shells only. Zero for a listing nothing will be
     * built over: absent, unresolved, empty, or a prefix.
     */
    public static long bytesFor(List<Attribute> output, @Nullable FileList list) {
        if (list == null || list.isResolved() == false || list.isTruncated()) {
            return 0L;
        }
        return bytesForFileSet(
            ExternalSchema.dataAttributesOf(output),
            ExternalMetadataColumns.metadataNames(output),
            list,
            list.fileCount()
        );
    }

    /**
     * As {@link #bytesFor}, for a file set discovery listed itself. Takes the count rather than reading it off
     * {@code list}, so a caller holding a listing whose count it has already established cannot disagree with it,
     * and skips the truncation test: this is the charge for a set that replaced a prefix.
     */
    public static long bytesForDiscovered(ExternalSchema dataSchema, Set<String> metadataNames, FileList list, int files) {
        return bytesForFileSet(dataSchema, metadataNames, list, files);
    }

    private static long bytesForFileSet(ExternalSchema dataSchema, Set<String> metadataNames, @Nullable FileList list, int files) {
        if (files <= 0) {
            return 0L;
        }
        PartitionMetadata metadata = list == null ? null : list.partitionMetadata();
        Set<String> retained = PartitionValueLayout.retainedKeys(dataSchema, metadata, metadataNames);
        PartitionValueLayout layout = PartitionValueLayout.of(retained, metadata);
        int directories = files;
        if (metadata != null
            && metadata.isEmpty() == false
            && metadata.fileCount() == files
            && metadata.rowCount() > 0
            && metadata.rowCount() < files) {
            directories = metadata.rowCount();
        }
        return mapAndShell(files, directories, layout.directoryKeys().size(), layout.perFileKeys().size());
    }

    /** {@code 0} keys is an empty map. Otherwise {@code 552 + 64 * keys}. */
    public static long perMap(int keys) {
        if (keys <= 0) {
            return 0L;
        }
        return MAP_OVERHEAD_BYTES + ENTRY_BYTES * (long) keys;
    }

    private static long mapAndShell(int files, int directories, int directoryKeys, int perFileKeys) {
        long mapBytes = perMap(directoryKeys) * directories + perMap(perFileKeys) * files;
        if (directoryKeys > 0 && perFileKeys > 0) {
            mapBytes += VIEW_BYTES * files;
        }
        return mapBytes + SHELL_BYTES * files;
    }
}
