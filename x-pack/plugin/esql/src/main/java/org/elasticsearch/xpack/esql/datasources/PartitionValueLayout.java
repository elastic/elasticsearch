/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Which partition-map keys a survivor keeps, split into the directory-constant tuple and the per-file overlay.
 * Discovery and the phase-2 charge both use this split so a projected key cannot be frozen on one side and billed
 * on the other.
 */
public final class PartitionValueLayout {

    private final List<String> directoryKeys;
    private final List<String> perFileKeys;
    private final boolean keepNulls;

    private PartitionValueLayout(List<String> directoryKeys, List<String> perFileKeys, boolean keepNulls) {
        this.directoryKeys = directoryKeys;
        this.perFileKeys = perFileKeys;
        this.keepNulls = keepNulls;
    }

    /**
     * {@code retained == null} is an unknown projection: every Hive column, plus {@code _file.size} and
     * {@code _file.modified}, with null values kept. {@code _file.path}, {@code _file.name}, and
     * {@code _file.directory} are derived from the file URI at read and are never layout keys.
     * An empty retain set keeps nothing. Otherwise Hive columns in the retain set stay directory-constant, in
     * partition-column order, and {@code _file.size} and {@code _file.modified} stay per file when retained.
     */
    public static PartitionValueLayout of(@Nullable Set<String> retained, @Nullable PartitionMetadata partitionInfo) {
        if (retained != null && retained.isEmpty()) {
            return new PartitionValueLayout(List.of(), List.of(), false);
        }
        boolean unknown = retained == null;
        List<String> directory = new ArrayList<>();
        if (partitionInfo != null && partitionInfo.isEmpty() == false) {
            for (String key : partitionInfo.partitionColumns().keySet()) {
                if (unknown || retained.contains(key)) {
                    directory.add(key);
                }
            }
        }
        List<String> perFile = new ArrayList<>();
        for (String name : FileMetadataColumns.NAMES) {
            if (name.equals(FileMetadataColumns.RECORD_REF) || FileMetadataColumns.LOCATION_NAMES.contains(name)) {
                continue;
            }
            if (unknown || retained.contains(name)) {
                perFile.add(name);
            }
        }
        return new PartitionValueLayout(List.copyOf(directory), List.copyOf(perFile), unknown);
    }

    /**
     * Keys the post-prune exec still needs on each split. Hive names are the query schema intersected with
     * the listing's partition columns. {@code _file.size} and {@code _file.modified} are included only when
     * bound as metadata. {@code _file.path}, {@code _file.name}, and {@code _file.directory} are derived from
     * the file URI at read and are not stored. {@code _file.record_ref} is composed per row and is not a map key.
     */
    public static Set<String> retainedKeys(
        ExternalSchema querySchema,
        @Nullable PartitionMetadata partitionInfo,
        Set<String> metadataColumnNames
    ) {
        Set<String> retained = new LinkedHashSet<>();
        if (partitionInfo != null) {
            Set<String> hive = partitionInfo.partitionColumns().keySet();
            for (String name : querySchema.names()) {
                if (hive.contains(name)) {
                    retained.add(name);
                }
            }
        }
        for (String name : FileMetadataColumns.NAMES) {
            if (name.equals(FileMetadataColumns.RECORD_REF) || FileMetadataColumns.LOCATION_NAMES.contains(name)) {
                continue;
            }
            if (metadataColumnNames.contains(name)) {
                retained.add(name);
            }
        }
        return Set.copyOf(retained);
    }

    /** Directory-constant keys in map order: Hive columns. Location keys are derived at read and are not included. */
    public List<String> directoryKeys() {
        return directoryKeys;
    }

    /**
     * Per-file keys in {@link FileMetadataColumns} order: size and modified. Location keys and
     * {@code _file.record_ref} are not included.
     */
    public List<String> perFileKeys() {
        return perFileKeys;
    }

    /** Unknown projection keeps explicit nulls. A known retain set drops them so an empty projection is {@link java.util.Map#of()}. */
    public boolean keepNulls() {
        return keepNulls;
    }

    public boolean isEmpty() {
        return directoryKeys.isEmpty() && perFileKeys.isEmpty();
    }
}
