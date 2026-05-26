/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.io.Closeable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Coordinator-only, in-memory cache service for external source metadata.
 * Maintains two independent caches:
 * <ul>
 *   <li>Schema cache (20% of budget, 5m TTL) — shared across users</li>
 *   <li>Listing cache (80% of budget, 30s TTL) — isolated by credential hash</li>
 * </ul>
 * Uses hard TTL via {@code setExpireAfterWrite} for the initial implementation.
 * Lazy TTL with ETag revalidation is deferred to a follow-up PR.
 */
public class ExternalSourceCacheService implements Closeable {

    private static final Logger logger = LogManager.getLogger(ExternalSourceCacheService.class);

    private final Cache<SchemaCacheKey, SchemaCacheEntry> schemaCache;
    private final Cache<ListingCacheKey, FileList> listingCache;
    private final long maxTotalBytes;
    private volatile boolean enabled;

    public ExternalSourceCacheService(Settings settings) {
        ByteSizeValue totalBudget = ExternalSourceCacheSettings.CACHE_SIZE.get(settings);
        this.maxTotalBytes = totalBudget.getBytes();
        this.enabled = ExternalSourceCacheSettings.CACHE_ENABLED.get(settings);

        TimeValue schemaTtl = ExternalSourceCacheSettings.SCHEMA_TTL.get(settings);
        TimeValue listingTtl = ExternalSourceCacheSettings.LISTING_TTL.get(settings);

        long schemaBudget = maxTotalBytes / 5; // 20%
        long listingBudget = maxTotalBytes - schemaBudget; // 80%

        this.schemaCache = CacheBuilder.<SchemaCacheKey, SchemaCacheEntry>builder()
            .setMaximumWeight(schemaBudget)
            .setExpireAfterWrite(schemaTtl)
            .weigher((key, value) -> value.estimatedBytes())
            .build();

        this.listingCache = CacheBuilder.<ListingCacheKey, FileList>builder()
            .setMaximumWeight(listingBudget)
            .setExpireAfterWrite(listingTtl)
            .weigher((key, value) -> value.estimatedBytes())
            .build();

        logger.info(
            "External source cache initialized: total=[{}], schema=[{}], listing=[{}], schemaTTL=[{}], listingTTL=[{}]",
            totalBudget,
            ByteSizeValue.ofBytes(schemaBudget),
            ByteSizeValue.ofBytes(listingBudget),
            schemaTtl,
            listingTtl
        );
    }

    /**
     * Returns a cached schema entry or computes it via the loader. The loader is only invoked
     * on a cache miss. When the cache is disabled, the loader is called directly (bypassing the cache).
     */
    public SchemaCacheEntry getOrComputeSchema(SchemaCacheKey key, CacheLoader<SchemaCacheKey, SchemaCacheEntry> loader) throws Exception {
        if (enabled == false) {
            return loader.load(key);
        }
        return schemaCache.computeIfAbsent(key, loader);
    }

    /**
     * Returns a cached file listing or stores the provided one. The loader is only invoked
     * on a cache miss. When the cache is disabled, the loader is called directly (bypassing the cache).
     */
    public FileList getOrComputeListing(ListingCacheKey key, CacheLoader<ListingCacheKey, FileList> loader) throws Exception {
        if (enabled == false) {
            return loader.load(key);
        }
        return listingCache.computeIfAbsent(key, loader);
    }

    /**
     * Coordinator-side entry point. Takes the {@code DriverCompletionInfo.capturedSourceMetadata}
     * payload — raw per-file contribution lists shipped back from every data node — merges each
     * list via {@code SourceStatisticsSerializer.mergeStatistics} (Parquet's existing multi-row-
     * group merge algorithm), then enriches the matching {@link SchemaCacheEntry} so the next
     * query's planning-time lookup short-circuits on the merged stats.
     */
    public void reconcileSourceStatsFromContributions(Map<String, java.util.List<Map<String, Object>>> contributionsPerFile) {
        if (enabled == false || contributionsPerFile == null || contributionsPerFile.isEmpty()) {
            return;
        }
        Map<String, Map<String, Object>> merged = new HashMap<>(contributionsPerFile.size());
        for (Map.Entry<String, java.util.List<Map<String, Object>>> e : contributionsPerFile.entrySet()) {
            java.util.List<Map<String, Object>> contributions = e.getValue();
            if (contributions == null || contributions.isEmpty()) {
                continue;
            }
            // Per-chunk safety: if any contribution is marked partial, only accept the merge when
            // the file also has a finalize marker. Otherwise drop the file's contributions — the
            // partial-only set risks under-counting rowCount and serving a wrong COUNT(*).
            // Any chunk-poison contribution unconditionally discards the file (a chunk hit
            // SKIP_ROW errors mid-scan — the merge would still under-count even with finalize).
            boolean anyPartial = false;
            boolean anyFinalize = false;
            boolean anyPoisoned = false;
            for (Map<String, Object> contribution : contributions) {
                if (Boolean.TRUE.equals(contribution.get(ExternalStatsCache.PARTIAL_CHUNK_KEY))) {
                    anyPartial = true;
                }
                if (Boolean.TRUE.equals(contribution.get(ExternalStatsCache.FINALIZE_CHUNKS_KEY))) {
                    anyFinalize = true;
                }
                if (Boolean.TRUE.equals(contribution.get(ExternalStatsCache.CHUNK_HAD_ERRORS_KEY))) {
                    anyPoisoned = true;
                }
            }
            if (anyPoisoned || (anyPartial && anyFinalize == false)) {
                continue;
            }
            // Strip the gate markers before merging so the well-known _stats.* keys are clean for
            // SourceStatisticsSerializer.mergeStatistics, AND drop the finalize-marker-only entry
            // (it carries no per-file stats of its own — just the completion signal).
            java.util.List<Map<String, Object>> cleaned = new java.util.ArrayList<>(contributions.size());
            for (Map<String, Object> contribution : contributions) {
                if (Boolean.TRUE.equals(contribution.get(ExternalStatsCache.FINALIZE_CHUNKS_KEY))
                    && contribution.containsKey(
                        org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.STATS_ROW_COUNT
                    ) == false) {
                    continue;
                }
                if (contribution.containsKey(ExternalStatsCache.PARTIAL_CHUNK_KEY)
                    || contribution.containsKey(ExternalStatsCache.FINALIZE_CHUNKS_KEY)
                    || contribution.containsKey(ExternalStatsCache.CHUNK_HAD_ERRORS_KEY)) {
                    Map<String, Object> stripped = new HashMap<>(contribution);
                    stripped.remove(ExternalStatsCache.PARTIAL_CHUNK_KEY);
                    stripped.remove(ExternalStatsCache.FINALIZE_CHUNKS_KEY);
                    stripped.remove(ExternalStatsCache.CHUNK_HAD_ERRORS_KEY);
                    cleaned.add(stripped);
                } else {
                    cleaned.add(contribution);
                }
            }
            if (cleaned.isEmpty()) {
                continue;
            }
            Map<String, Object> mergedForFile;
            if (cleaned.size() == 1) {
                mergedForFile = cleaned.get(0);
            } else {
                mergedForFile = org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.mergeStatistics(cleaned);
                // mergeStatistics rebuilds the map from scratch and only retains the well-known
                // _stats.row_count / _stats.size_bytes / _stats.columns.* keys. The reconciler
                // below relies on MTIME_MILLIS_KEY (to find the matching SchemaCacheEntry via
                // its lastModifiedEpochMillis axis); without re-attaching it the multi-chunk
                // merge would never commit. All chunks of a file share the same pinned mtime
                // (set at iterator open), so pulling it off any contribution is fine.
                if (mergedForFile != null) {
                    Object mtime = cleaned.get(0).get(ExternalStatsCache.MTIME_MILLIS_KEY);
                    if (mtime != null) {
                        mergedForFile.put(ExternalStatsCache.MTIME_MILLIS_KEY, mtime);
                    }
                    Object fingerprint = cleaned.get(0).get(ExternalStatsCache.CONFIG_FINGERPRINT_KEY);
                    if (fingerprint != null) {
                        mergedForFile.put(ExternalStatsCache.CONFIG_FINGERPRINT_KEY, fingerprint);
                    }
                }
            }
            if (mergedForFile != null && mergedForFile.isEmpty() == false) {
                merged.put(e.getKey(), mergedForFile);
            }
        }
        reconcileSourceStats(merged);
    }

    /**
     * Reconciles already-merged data-node-captured source stats into the schema cache. For each
     * {@code (path, mergedStats)} entry, finds the cached {@link SchemaCacheEntry} whose location
     * and mtime match and replaces it with a new entry whose {@code safeMetadata} folds in the
     * merged {@code _stats.*} keys. Entries with no cache match are ignored (the warm path will
     * just trigger a fresh metadata() call on the next query).
     */
    public void reconcileSourceStats(Map<String, Map<String, Object>> mergedStatsPerFile) {
        if (enabled == false || mergedStatsPerFile == null || mergedStatsPerFile.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Map<String, Object>> entry : mergedStatsPerFile.entrySet()) {
            String path = entry.getKey();
            Map<String, Object> mergedStats = entry.getValue();
            if (path == null || mergedStats == null || mergedStats.isEmpty()) {
                continue;
            }
            Object mtimeObj = mergedStats.get(ExternalStatsCache.MTIME_MILLIS_KEY);
            if (mtimeObj instanceof Number == false) {
                continue;
            }
            long mtimeMillis = ((Number) mtimeObj).longValue();
            for (SchemaCacheKey key : schemaCache.keys()) {
                if (path.equals(key.canonicalPath()) && key.lastModifiedEpochMillis() == mtimeMillis) {
                    SchemaCacheEntry existing = schemaCache.get(key);
                    if (existing == null) {
                        continue;
                    }
                    Map<String, Object> enriched = new HashMap<>(existing.safeMetadata());
                    enriched.putAll(mergedStats);
                    SchemaCacheEntry replaced = new SchemaCacheEntry(
                        existing.columnNames(),
                        existing.columnTypes(),
                        existing.columnNullabilities(),
                        existing.columnSynthetics(),
                        existing.sourceType(),
                        existing.location(),
                        enriched,
                        existing.connectorConfig(),
                        existing.cachedAtMillis()
                    );
                    schemaCache.put(key, replaced);
                }
            }
        }
    }

    public void setEnabled(boolean enabled) {
        if (enabled == false && this.enabled) {
            this.enabled = false;
            clearAll();
            logger.info("External source cache disabled and cleared");
        } else if (enabled && this.enabled == false) {
            this.enabled = true;
            logger.info("External source cache re-enabled");
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void clearAll() {
        schemaCache.invalidateAll();
        listingCache.invalidateAll();
    }

    public Map<String, Object> usageStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("enabled", enabled);
        stats.put("max_total_bytes", maxTotalBytes);

        stats.put("schema_cache.count", schemaCache.count());
        stats.put("schema_cache.hits", schemaCache.stats().getHits());
        stats.put("schema_cache.misses", schemaCache.stats().getMisses());
        stats.put("schema_cache.evictions", schemaCache.stats().getEvictions());

        stats.put("listing_cache.count", listingCache.count());
        stats.put("listing_cache.hits", listingCache.stats().getHits());
        stats.put("listing_cache.misses", listingCache.stats().getMisses());
        stats.put("listing_cache.evictions", listingCache.stats().getEvictions());

        return stats;
    }

    @Override
    public void close() {
        clearAll();
    }

    // Visible for testing
    Cache<SchemaCacheKey, SchemaCacheEntry> schemaCache() {
        return schemaCache;
    }

    // Visible for testing
    Cache<ListingCacheKey, FileList> listingCache() {
        return listingCache;
    }
}
