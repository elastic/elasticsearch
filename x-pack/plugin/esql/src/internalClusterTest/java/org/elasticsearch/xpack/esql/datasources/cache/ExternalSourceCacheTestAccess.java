/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Test-only bridge into {@link ExternalSourceCacheService}'s package-private schema cache, so
 * internal-cluster tests can surgically evict individual PER-FILE entries (deterministically simulating
 * LRU pressure) without widening the production API. Lives in the internalClusterTest source set only.
 */
public final class ExternalSourceCacheTestAccess {

    private ExternalSourceCacheTestAccess() {}

    /**
     * How many times a resolve has fallen back to the memoized dataset aggregate because the per-file merge came
     * back incomplete.
     * <p>
     * This is what a warm multi-file test needs beyond zero documents found. A warm {@code COUNT(*)} over many
     * files reports zero documents whether every per-file record carried its statistics or none of them did,
     * because the dataset aggregate answers the count either way — so that assertion alone cannot see per-file
     * records silently losing their measurements. Asserting this counter is unchanged across the warm query
     * says the answer came from the per-file records rather than from the fallback.
     */
    public static long datasetAggregateFallbacks(ExternalSourceCacheService service) {
        Object hits = service.usageStats().get("dataset_aggregate.hits");
        return hits instanceof Number n ? n.longValue() : 0L;
    }

    /**
     * How many per-file schema entries currently carry a harvested row count, for paths containing
     * {@code pathSubstring}. The exact signal for partial enrichment: a contribution refused for some files and
     * accepted for others leaves a count here below the file count, while every value assertion still passes.
     */
    public static int enrichedPerFileEntries(ExternalSourceCacheService service, String pathSubstring) {
        // Counted over the STATISTICS store: a harvested row count is a measurement, so it no longer sits on
        // the schema record. One address per (file, read), so a file read under two configurations contributes
        // two — which is what the enrichment question is actually about, and what the single-store shape could
        // not express.
        int[] enriched = { 0 };
        service.statisticsCache().forEach((key, record) -> {
            if (key.file().location().contains(pathSubstring)
                && record.measurements().containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT)) {
                enriched[0]++;
            }
        });
        return enriched[0];
    }

    /**
     * Sum of {@link SchemaCacheEntry#estimatedBytes()} over every entry currently in the schema cache.
     * Used by weight-accounting cluster tests to assert retained heap stays inside the schema budget slice.
     * <p>
     * Schema records only. Measurements live in their own store against their own budget, so a test that means
     * to bound total retained identity heap sums this and {@link #retainedStatisticsWeightBytes}, each against
     * its own slice — summing them against one budget would compare a total to a part.
     */
    public static long retainedSchemaWeightBytes(ExternalSourceCacheService service) {
        long[] total = { 0L };
        service.schemaCache().forEach((key, entry) -> total[0] += entry.estimatedBytes());
        return total[0];
    }

    /** Sum of {@link StatisticsRecord#estimatedBytes()} over every entry currently in the statistics cache. */
    public static long retainedStatisticsWeightBytes(ExternalSourceCacheService service) {
        long[] total = { 0L };
        service.statisticsCache().forEach((key, record) -> total[0] += record.estimatedBytes());
        return total[0];
    }

    /**
     * Invalidates every per-file schema-cache entry whose canonical path contains {@code pathSubstring}.
     * The dataset aggregate under test is untouched because it is not in this store: the surgical arms of the
     * warm-fold regression tests must remove FILE entries, and now they structurally cannot reach the fold.
     * Returns the number of entries invalidated.
     */
    public static int invalidatePerFileSchemaEntries(ExternalSourceCacheService service, String pathSubstring) {
        return invalidatePerFileSchemaEntries(service, pathSubstring, Integer.MAX_VALUE);
    }

    /**
     * Bounded variant of {@link #invalidatePerFileSchemaEntries(ExternalSourceCacheService, String)}:
     * invalidates at most {@code maxEntries} matching per-file entries. The deterministic PARTIAL
     * eviction arms use this to construct an exact missing subset — e.g. exactly ONE per-file entry,
     * the minimal trigger of the all-or-nothing multi-file stats merge — rather than sweeping
     * everything under a directory.
     */
    public static int invalidatePerFileSchemaEntries(ExternalSourceCacheService service, String pathSubstring, int maxEntries) {
        List<SchemaCacheKey> victims = new ArrayList<>();
        service.schemaCache().forEach((key, entry) -> {
            if (victims.size() < maxEntries && key.location().contains(pathSubstring)) {
                victims.add(key);
            }
        });
        for (SchemaCacheKey victim : victims) {
            service.schemaCache().invalidate(victim);
        }
        // Evicting "the file" means both kinds of fact about it. The arms that call this construct an exact
        // missing subset and then assert the dataset does not warm; leaving the measurements resident would
        // let the fold still answer and the arm would pass without testing anything.
        List<StatisticsKey> statsVictims = new ArrayList<>();
        service.statisticsCache().forEach((key, record) -> {
            for (SchemaCacheKey victim : victims) {
                if (key.file().equals(victim)) {
                    statsVictims.add(key);
                }
            }
        });
        for (StatisticsKey victim : statsVictims) {
            service.statisticsCache().invalidate(victim);
        }
        return victims.size();
    }

    /**
     * Simulates listing-cache TTL expiry without touching the dataset aggregate. File-set tests
     * assert the TTL-hot listing first, then call this so the next resolve re-lists and the
     * fingerprint can miss.
     */
    public static void invalidateListings(ExternalSourceCacheService service) {
        service.listingCache().invalidateAll();
    }
}
