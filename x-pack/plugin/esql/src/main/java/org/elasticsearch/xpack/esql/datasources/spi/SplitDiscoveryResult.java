/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.List;

/**
 * Result of {@link SplitProvider#discoverSplits}: the discovered splits plus the post-prune
 * "scanned" accounting reported in the query profile.
 *
 * <p>{@code filesScanned} is the number of distinct files that survived coordinator-side pruning
 * and contributed at least one split. It is provider-specific: file-based sources report the real
 * count, while sources without a file concept (e.g. Arrow Flight) report {@code 0}. The other
 * scanned metrics surfaced in the profile — total split count and estimated bytes (sum of
 * {@link ExternalSplit#estimatedSizeInBytes()}, excluding splits that report an unknown size) —
 * are derived by {@code SplitDiscoveryPhase} from the {@link #splits()} list, so they are not
 * carried here.
 *
 * <p>{@code exhaustivelyPruned} is {@code true} only when {@link #splits()} is empty <em>because</em>
 * every file was eliminated by a row-count-preserving filter contradiction — a partition/metadata
 * predicate that evaluated to {@code false}, or a missing-column filter that is unsatisfiable in
 * {@code WHERE} (comparisons, {@code IN}, {@code IS NOT NULL}; {@code IS NULL} on a missing column
 * matches every row and is not a prune). Those cases emit zero rows on a full read too, so
 * {@code SplitDiscoveryPhase} may trust them as "read nothing". An empty result that is not a
 * proven filter contradiction — unresolved glob, empty file list, or a provider that cannot certify
 * the prune — reports {@code false} and must fall back to a full read.
 *
 * <p>{@code cpuNanos} is the CPU time (excluding IO wait) consumed by the split discovery phase,
 * accumulated across all files and any background threads. Zero when not measured or not supported.
 */
public record SplitDiscoveryResult(List<ExternalSplit> splits, int filesScanned, boolean exhaustivelyPruned, long cpuNanos) {

    public static final SplitDiscoveryResult EMPTY = new SplitDiscoveryResult(List.of(), 0, false, 0L);

    public SplitDiscoveryResult {
        splits = List.copyOf(splits);
    }

    /**
     * Convenience for providers that have no file-level accounting: carries the splits with a
     * {@code filesScanned} of {@code 0}.
     */
    public static SplitDiscoveryResult of(List<ExternalSplit> splits) {
        return splits.isEmpty() ? EMPTY : new SplitDiscoveryResult(splits, 0, false, 0L);
    }
}
