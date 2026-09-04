/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.util.List;

/**
 * The dedicated metadata namespace, shared by every {@link PartitionDetector}: standard metadata
 * names ({@code _id}, {@code _index}, ...), the {@code _file.*} family, and reader-synthesized
 * channel names are reserved — a dataset layout cannot claim them, or {@code METADATA _index}
 * would silently return a layout value instead of its spec-defined meaning. Detectors surface a
 * colliding partition column under {@link #RESERVED_RENAME_PREFIX} and disclose each rename with
 * a {@code Warning} response header.
 * <p>
 * Every detector MUST route its surfaced column names through {@link #surface(String)}: the
 * downstream consumers ({@code VirtualColumnIterator} role dispatch, the analyzer's
 * metadata bind, the per-file constant merge) key reserved behavior on bare names and rely on
 * partition columns never carrying one.
 */
final class ReservedPartitionNames {

    /** Prefix applied to a partition column whose name collides with a dedicated metadata name. */
    static final String RESERVED_RENAME_PREFIX = "_partition.";

    private ReservedPartitionNames() {}

    /**
     * Whether a partition key collides with the dedicated metadata namespace. Uses
     * {@link ExternalMetadataColumns#RESERVED_NAMES} (build-mode-independent), not the bindable
     * {@code STANDARD_NAMES}, so a layout claiming a snapshot-gated name like {@code _tier} is
     * renamed in release builds too — reservation must not flip with build mode.
     */
    static boolean isReserved(String key) {
        return ExternalMetadataColumns.RESERVED_NAMES.contains(key)
            || FileMetadataColumns.NAMES.contains(key)
            || SyntheticColumns.NAMES.contains(key);
    }

    /** The name {@code key} surfaces under: itself when unreserved, the prefixed form otherwise. */
    static String surface(String key) {
        return isReserved(key) ? RESERVED_RENAME_PREFIX + key : key;
    }

    /**
     * Emit one {@code Warning} response header per renamed key (none when {@code renamed} is
     * empty). Callers pass the ORIGINAL key names that {@link #surface(String)} renamed.
     */
    static void warnRenamed(List<String> renamed) {
        if (renamed.isEmpty()) {
            return;
        }
        SkipWarnings warnings = new SkipWarnings(
            "Partition columns shadowing reserved metadata names were renamed;"
                + " reference them by the "
                + RESERVED_RENAME_PREFIX
                + "* name."
        );
        for (String key : renamed) {
            warnings.add("partition column [" + key + "] surfaced as [" + RESERVED_RENAME_PREFIX + key + "]");
        }
    }
}
