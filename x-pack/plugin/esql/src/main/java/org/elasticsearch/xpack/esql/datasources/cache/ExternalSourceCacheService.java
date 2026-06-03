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
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    public void reconcileSourceStatsFromContributions(Map<String, List<Map<String, Object>>> contributionsPerFile) {
        if (enabled == false || contributionsPerFile == null || contributionsPerFile.isEmpty()) {
            return;
        }
        Map<String, Map<String, Object>> merged = new HashMap<>(contributionsPerFile.size());
        for (Map.Entry<String, List<Map<String, Object>>> e : contributionsPerFile.entrySet()) {
            List<Map<String, Object>> contributions = e.getValue();
            if (contributions == null || contributions.isEmpty()) {
                continue;
            }
            // Classify each wire blob into a SourceStatsContribution, then route through an
            // exhaustive switch — a new contribution kind is a compile error here until its handling
            // is written, rather than a silent fall-through to the summable-partial default (the
            // class of bug that produced the SKIP_ROW-poison, fingerprint, and whole-file-double-count
            // regressions). WholeFile and PartialChunk carry stats; Poison and Finalize are gate-only.
            boolean poisoned = false;
            boolean finalized = false;
            List<Map<String, Object>> wholeFile = new ArrayList<>(contributions.size());
            List<Map<String, Object>> partials = new ArrayList<>(contributions.size());
            for (Map<String, Object> raw : contributions) {
                switch (SourceStatsContribution.classify(raw)) {
                    case SourceStatsContribution.Poison ignored -> poisoned = true;
                    case SourceStatsContribution.Finalize ignored -> finalized = true;
                    case SourceStatsContribution.WholeFile wf -> wholeFile.add(wf.stats());
                    case SourceStatsContribution.PartialChunk pc -> partials.add(pc.stats());
                }
            }
            // A poisoned file (a chunk dropped rows mid-scan) is discarded entirely. Partial chunks
            // partition the file, so they may only be summed once a finalize marker proves the whole
            // file completed — a partial-only set risks under-counting COUNT(*).
            if (poisoned || (partials.isEmpty() == false && finalized == false)) {
                continue;
            }
            Map<String, Object> mergedForFile = mergeContributions(wholeFile, partials);
            if (mergedForFile == null || mergedForFile.isEmpty()) {
                logger.debug("dropping captured stats for [{}]: {} partial(s) produced no merged output", e.getKey(), partials.size());
                continue;
            }
            merged.put(e.getKey(), mergedForFile);
        }
        reconcileSourceStats(merged);
    }

    /**
     * Combines a file's stats-bearing contributions into a single merged map, or {@code null} when
     * there is nothing to commit. A whole-file read is authoritative and never summed: each such
     * contribution already reports the file's full row count (text readers apply no scan-time filter,
     * and a whole-file read is the first+last split with no parallel slicing). Duplicate whole-file
     * contributions can legitimately differ on column-stats coverage, however — a schema-probe pass
     * may track a narrower set of columns than the data scan — so we union those keys via
     * {@link #mergeWholeFileContributions} rather than dropping the broader-coverage entry. Only
     * partial chunks, which partition the file, are summed via {@code mergeStatistics}; that
     * rebuilds the map from scratch retaining only the {@code _stats.*} keys, so MTIME_MILLIS_KEY
     * (to match the SchemaCacheEntry) and CONFIG_FINGERPRINT_KEY (to disambiguate
     * {@code WITH}-option variants) are re-attached from a chunk — all chunks of a file share one
     * pinned mtime and fingerprint.
     */
    private static Map<String, Object> mergeContributions(List<Map<String, Object>> wholeFile, List<Map<String, Object>> partials) {
        if (wholeFile.isEmpty() == false) {
            return mergeWholeFileContributions(wholeFile);
        }
        if (partials.isEmpty()) {
            return null;
        }
        if (partials.size() == 1) {
            return partials.get(0);
        }
        Map<String, Object> mergedForFile = SourceStatisticsSerializer.mergeStatistics(partials);
        if (mergedForFile != null) {
            Object mtime = partials.get(0).get(ExternalStats.MTIME_MILLIS_KEY);
            if (mtime != null) {
                mergedForFile.put(ExternalStats.MTIME_MILLIS_KEY, mtime);
            }
            Object fingerprint = partials.get(0).get(ExternalStats.CONFIG_FINGERPRINT_KEY);
            if (fingerprint != null) {
                mergedForFile.put(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint);
            }
        }
        return mergedForFile;
    }

    /**
     * Folds duplicate whole-file contributions for the same file into one map. Row count, mtime,
     * and config fingerprint must agree across entries (asserted) since each contribution already
     * covers the entire file under the same pinned config. Column-stats keys, however, may differ
     * between callers — a schema-probe pass typically projects fewer columns than the data scan —
     * so {@code _stats.columns.*} entries are unioned: for any key present in only one contribution
     * the unique value is taken; for keys present in multiple contributions the values must agree
     * (asserted) since they measure the same file under the same config.
     */
    private static Map<String, Object> mergeWholeFileContributions(List<Map<String, Object>> wholeFile) {
        if (wholeFile.size() == 1) {
            return wholeFile.get(0);
        }
        Map<String, Object> base = wholeFile.get(0);
        Map<String, Object> merged = new HashMap<>(base);
        for (int i = 1; i < wholeFile.size(); i++) {
            Map<String, Object> next = wholeFile.get(i);
            assert agreesWithBase(base, next)
                : "whole-file contributions for the same file must agree on row count, mtime, and config fingerprint: "
                    + base
                    + " vs "
                    + next;
            for (Map.Entry<String, Object> e : next.entrySet()) {
                if (e.getKey().startsWith(SourceStatisticsSerializer.STATS_COL_PREFIX)) {
                    Object prev = merged.putIfAbsent(e.getKey(), e.getValue());
                    assert prev == null || Objects.equals(prev, e.getValue())
                        : "whole-file contributions disagree on column stat [" + e.getKey() + "]: " + prev + " vs " + e.getValue();
                }
            }
        }
        return merged;
    }

    private static boolean agreesWithBase(Map<String, Object> a, Map<String, Object> b) {
        return sameNumericOrEqual(a.get(SourceStatisticsSerializer.STATS_ROW_COUNT), b.get(SourceStatisticsSerializer.STATS_ROW_COUNT))
            && sameNumericOrEqual(a.get(ExternalStats.MTIME_MILLIS_KEY), b.get(ExternalStats.MTIME_MILLIS_KEY))
            && Objects.equals(a.get(ExternalStats.CONFIG_FINGERPRINT_KEY), b.get(ExternalStats.CONFIG_FINGERPRINT_KEY));
    }

    private static boolean sameNumericOrEqual(Object a, Object b) {
        if (a instanceof Number na && b instanceof Number nb) {
            return na.longValue() == nb.longValue();
        }
        return Objects.equals(a, b);
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
            Object mtimeObj = mergedStats.get(ExternalStats.MTIME_MILLIS_KEY);
            if (mtimeObj instanceof Number == false) {
                continue;
            }
            long mtimeMillis = ((Number) mtimeObj).longValue();
            // Enrich the schema entry whose config matches the contribution. SchemaCacheKey is keyed on
            // path + mtime + formatType + formatConfig + endpoint + region, so the SAME file can have
            // several entries — one per (formatType, formatConfig) tuple (e.g. WITH {"header_row": true}
            // vs {"header_row": false} count rows differently). The config fingerprint disambiguates
            // them, and it is node-stable: both the data node's contribution and the coordinator's entry
            // derive it from SchemaCacheKey.buildFormatConfig of the same logical config, so the guard
            // holds across JVMs (coordinator != data node) — the warm short-circuit's whole point.
            Object contributionFingerprint = mergedStats.get(ExternalStats.CONFIG_FINGERPRINT_KEY);
            // Cache.forEach iterates each segment's HashMap under the segment's readLock,
            // making it safe against concurrent LRU mutations: promote() (called by get(),
            // computeIfAbsent, etc. on any thread) acquires only lruLock, not the segment
            // readLock, so it cannot corrupt the forEach traversal. Cache.keys() and
            // Cache.values() walk the LRU doubly-linked list with no locks and are therefore
            // unsafe here. Do NOT call get() or put() inside the forEach consumer — the
            // segment readLock is not reentrant and put() acquires the segment writeLock.
            List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>> matchingEntries = new ArrayList<>();
            schemaCache.forEach((key, existing) -> {
                if (path.equals(key.canonicalPath()) == false || key.lastModifiedEpochMillis() != mtimeMillis) {
                    return;
                }
                Object existingFingerprint = existing.safeMetadata().get(ExternalStats.CONFIG_FINGERPRINT_KEY);
                if (Objects.equals(existingFingerprint, contributionFingerprint) == false) {
                    return;
                }
                matchingEntries.add(Map.entry(key, existing));
            });
            for (Map.Entry<SchemaCacheKey, SchemaCacheEntry> match : matchingEntries) {
                SchemaCacheKey key = match.getKey();
                SchemaCacheEntry existing = match.getValue();
                Map<String, Object> enriched = new HashMap<>(existing.safeMetadata());
                enriched.putAll(mergedStats);
                schemaCache.put(
                    key,
                    new SchemaCacheEntry(
                        existing.columnNames(),
                        existing.columnTypes(),
                        existing.columnNullabilities(),
                        existing.columnSynthetics(),
                        existing.sourceType(),
                        existing.location(),
                        enriched,
                        existing.connectorConfig(),
                        existing.cachedAtMillis()
                    )
                );
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
