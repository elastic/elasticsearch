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
 * predicate that evaluated to {@code false}, or a filter over a column absent from the file (both
 * make the {@code WHERE} unsatisfiable, so a full read would emit zero rows too). It is deliberately
 * {@code false} when the empty result is a best-effort heuristic that a downstream read would have
 * disagreed with (e.g. a file dropped for having no column overlap with the query still contributes
 * rows to {@code COUNT(*)}). Only the former is safe for {@code SplitDiscoveryPhase} to trust as
 * "read nothing"; the latter must fall back to a full read so the row filter runs. An empty result
 * from an unresolved or empty file list is not a prune and reports {@code false}.
 */
public record SplitDiscoveryResult(List<ExternalSplit> splits, int filesScanned, boolean exhaustivelyPruned) {

    public static final SplitDiscoveryResult EMPTY = new SplitDiscoveryResult(List.of(), 0, false);

    public SplitDiscoveryResult {
        splits = List.copyOf(splits);
    }

    /** Splits with the given {@code filesScanned}; not an exhaustive prune (there are splits to read). */
    public SplitDiscoveryResult(List<ExternalSplit> splits, int filesScanned) {
        this(splits, filesScanned, false);
    }

    /**
     * Convenience for providers that have no file-level accounting: carries the splits with a
     * {@code filesScanned} of {@code 0}.
     */
    public static SplitDiscoveryResult of(List<ExternalSplit> splits) {
        return splits.isEmpty() ? EMPTY : new SplitDiscoveryResult(splits, 0);
    }
}
