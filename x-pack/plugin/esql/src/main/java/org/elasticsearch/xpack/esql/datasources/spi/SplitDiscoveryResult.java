/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;

import java.util.List;
import java.util.Map;

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
public record SplitDiscoveryResult(
    List<ExternalSplit> splits,
    int filesScanned,
    boolean exhaustivelyPruned,
    long cpuNanos,
    // The files this discovery actually resolved, when they are not the ones it was handed, and the per-file read
    // contracts over them. A provider that discovers its own file set returns it here so the plan carries the set
    // that was planned over rather than the listing resolution happened to hold. Null means "the handed one still
    // stands", which is what every provider that does not list says. Coordinator-local: nothing serializes this.
    @Nullable FileList fileSet,
    @Nullable Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
    // Anything discovery found that the query's author needs told - a partition value the dataset's own type
    // cannot hold, say. The node log cannot serve that: it is the answer that changed, so it belongs in the
    // response beside it. Empty for a provider with nothing to report.
    List<String> warnings
) {

    public static final SplitDiscoveryResult EMPTY = new SplitDiscoveryResult(List.of(), 0, false, 0L);

    /** As the full form, for a provider that reads the file set it was handed. */
    public SplitDiscoveryResult(List<ExternalSplit> splits, int filesScanned, boolean exhaustivelyPruned, long cpuNanos) {
        this(splits, filesScanned, exhaustivelyPruned, cpuNanos, null, null, List.of());
    }

    /** As the full form, for a provider that discovered its own files and has nothing to warn about. */
    public SplitDiscoveryResult(
        List<ExternalSplit> splits,
        int filesScanned,
        boolean exhaustivelyPruned,
        long cpuNanos,
        @Nullable FileList fileSet,
        @Nullable Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap
    ) {
        this(splits, filesScanned, exhaustivelyPruned, cpuNanos, fileSet, schemaMap, List.of());
    }

    public SplitDiscoveryResult {
        splits = List.copyOf(splits);
        warnings = List.copyOf(warnings);
    }

    /**
     * Convenience for providers that have no file-level accounting: carries the splits with a
     * {@code filesScanned} of {@code 0}.
     */
    public static SplitDiscoveryResult of(List<ExternalSplit> splits) {
        return splits.isEmpty() ? EMPTY : new SplitDiscoveryResult(splits, 0, false, 0L);
    }
}
