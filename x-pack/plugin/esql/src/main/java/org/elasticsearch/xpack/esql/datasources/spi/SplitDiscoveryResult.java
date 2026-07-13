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
 */
public record SplitDiscoveryResult(List<ExternalSplit> splits, int filesScanned) {

    public static final SplitDiscoveryResult EMPTY = new SplitDiscoveryResult(List.of(), 0);

    public SplitDiscoveryResult {
        splits = List.copyOf(splits);
    }

    /**
     * Convenience for providers that have no file-level accounting: carries the splits with a
     * {@code filesScanned} of {@code 0}.
     */
    public static SplitDiscoveryResult of(List<ExternalSplit> splits) {
        return splits.isEmpty() ? EMPTY : new SplitDiscoveryResult(splits, 0);
    }
}
