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
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

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
            // is written, rather than a silent fall-through. WholeFile and PartialChunk carry stats;
            // Poison is gate-only.
            boolean poisoned = false;
            List<SourceStatsContribution.WholeFile> wholeFile = new ArrayList<>(contributions.size());
            // Coverage-addressed partial chunks. Each carries the file byte-range it observed; the
            // reconciler unions them by range, so disjoint ranges (parallel chunks, macro-splits,
            // splits across nodes) sum while a range re-observed by another scan of the same file (a
            // sibling FORK branch, a schema-probe pass, a retry) is counted once. No scan/finalize
            // counting — "which bytes" is intrinsic, "how many reads" is an implementation detail.
            List<SourceStatsContribution.PartialChunk> partials = new ArrayList<>(contributions.size());
            for (Map<String, Object> raw : contributions) {
                switch (SourceStatsContribution.classify(raw)) {
                    case SourceStatsContribution.Poison ignored -> poisoned = true;
                    case SourceStatsContribution.WholeFile wf -> wholeFile.add(wf);
                    case SourceStatsContribution.PartialChunk pc -> partials.add(pc);
                }
            }
            // A poisoned file (a chunk dropped rows mid-scan) is discarded entirely.
            if (poisoned) {
                continue;
            }
            Map<String, Object> mergedForFile = mergeContributions(wholeFile, partials);
            if (mergedForFile == null || mergedForFile.isEmpty()) {
                logger.debug("dropping captured stats for [{}]: no complete cover after coverage union", e.getKey());
                continue;
            }
            merged.put(e.getKey(), mergedForFile);
        }
        reconcileSourceStats(merged);
    }

    /**
     * Combines a file's stats-bearing contributions into a single merged map, or {@code null} when
     * there is nothing committable. A whole-file read is authoritative and never summed: each already
     * reports the file's full row count, so duplicates (e.g. a schema-probe pass that tracks fewer
     * columns than the data scan) are unioned via {@link #mergeWholeFileContributions}. Partial chunks
     * are reconciled by <em>coverage</em>: {@link #foldCoveredPartials} unions them by byte range, so
     * a range re-observed by another scan of the same file is counted once while disjoint ranges sum —
     * and only commits when the ranges tile {@code [0, end)} with a flagged tail.
     */
    private static Map<String, Object> mergeContributions(
        List<SourceStatsContribution.WholeFile> wholeFile,
        List<SourceStatsContribution.PartialChunk> partials
    ) {
        if (wholeFile.isEmpty() == false) {
            // A whole-file read is authoritative for the whole file; partial chunks (if any arrived
            // alongside) add nothing and must not be summed on top.
            return mergeWholeFileContributions(wholeFile);
        }
        return foldCoveredPartials(partials);
    }

    /**
     * Re-serializes a typed contribution back to the flat {@code _stats.*} wire map. This is the one
     * boundary where the reconciler hands typed statistics to the shared, cross-format map-based
     * merger ({@link SourceStatisticsSerializer#mergeStatistics}) and to the schema cache, both of
     * which speak the flat map. Re-attaches the keying fields (mtime, config fingerprint) that live
     * outside {@link SourceStatistics}.
     */
    private static Map<String, Object> toFlatMap(SourceStatistics stats, long mtimeMillis, String configFingerprint) {
        Map<String, Object> base = new HashMap<>();
        if (mtimeMillis >= 0) {
            base.put(ExternalStats.MTIME_MILLIS_KEY, mtimeMillis);
        }
        if (configFingerprint != null) {
            base.put(ExternalStats.CONFIG_FINGERPRINT_KEY, configFingerprint);
        }
        return stats == null ? base : SourceStatisticsSerializer.embedStatistics(base, stats);
    }

    /**
     * Reconciles partial chunks by coverage. Deduplicates contributions that observed the same byte
     * range (a range seen by more than one scan of the file is counted once), verifies the distinct
     * ranges tile {@code [0, end)} contiguously with the final range flagged last (an incomplete or
     * overlapping cover is not cacheable, so the warm query just re-scans — never a wrong answer),
     * then folds the disjoint ranges: {@code mergeStatistics} sums row/null/byte counts and takes the
     * extreme for min/max. Returns {@code null} when the cover is incomplete or un-addressable.
     */
    private static Map<String, Object> foldCoveredPartials(List<SourceStatsContribution.PartialChunk> partials) {
        if (partials.isEmpty()) {
            return null;
        }
        // Union by start offset. Two partials sharing a start must share an end (same range, a
        // re-scan duplicate) — a different end is an ambiguous overlap, so bail rather than guess.
        TreeMap<Long, SourceStatsContribution.PartialChunk> byStart = new TreeMap<>();
        for (SourceStatsContribution.PartialChunk pc : partials) {
            if (pc.hasCoverage() == false) {
                return null; // un-addressable partial (older node) — cannot prove a complete cover
            }
            SourceStatsContribution.PartialChunk existing = byStart.putIfAbsent(pc.start(), pc);
            if (existing != null && existing.end() != pc.end()) {
                return null;
            }
        }
        // Completeness: ranges must tile [0, end) with no gap or overlap, and the final range must be
        // flagged last (it observed end-of-input). Intermediate last-flags — e.g. the tail segment of a
        // non-final macro-split — are harmless; only the highest-offset range's flag is read.
        long expectedStart = 0;
        List<SourceStatsContribution.PartialChunk> distinct = new ArrayList<>(byStart.size());
        for (SourceStatsContribution.PartialChunk pc : byStart.values()) {
            if (pc.start() != expectedStart) {
                return null; // gap or overlap — not a clean tiling
            }
            expectedStart = pc.end();
            distinct.add(pc);
        }
        SourceStatsContribution.PartialChunk lastRange = distinct.get(distinct.size() - 1);
        if (lastRange.last() == false) {
            return null; // never observed end-of-input — partial cover, do not cache
        }
        return foldDistinctRanges(distinct);
    }

    /**
     * Folds the disjoint, distinct coverage ranges of one complete cover into a single merged map.
     * The sum/extreme arithmetic is delegated to the shared {@link SourceStatisticsSerializer#mergeStatistics}
     * (the same algorithm Parquet's multi-row-group merge uses), so each range's typed statistics are
     * re-serialized to the flat wire map here — the one place the reconciler touches that map.
     */
    private static Map<String, Object> foldDistinctRanges(List<SourceStatsContribution.PartialChunk> distinct) {
        if (distinct.isEmpty()) {
            return null;
        }
        SourceStatsContribution.PartialChunk first = distinct.get(0);
        if (distinct.size() == 1) {
            return toFlatMap(first.stats(), first.mtimeMillis(), first.configFingerprint());
        }
        List<Map<String, Object>> maps = new ArrayList<>(distinct.size());
        for (SourceStatsContribution.PartialChunk pc : distinct) {
            maps.add(toFlatMap(pc.stats(), pc.mtimeMillis(), pc.configFingerprint()));
        }
        Map<String, Object> mergedForFile = SourceStatisticsSerializer.mergeStatistics(maps);
        if (mergedForFile != null) {
            // mergeStatistics rebuilds from the _stats.* keys only; re-attach the keying fields that
            // identify the matching schema-cache entry (all ranges of a file share one mtime+fingerprint).
            if (first.mtimeMillis() >= 0) {
                mergedForFile.put(ExternalStats.MTIME_MILLIS_KEY, first.mtimeMillis());
            }
            if (first.configFingerprint() != null) {
                mergedForFile.put(ExternalStats.CONFIG_FINGERPRINT_KEY, first.configFingerprint());
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
    private static Map<String, Object> mergeWholeFileContributions(List<SourceStatsContribution.WholeFile> wholeFile) {
        // The whole-file column-union below keys off the flat _stats.* layout, so re-serialize the
        // typed contributions to the wire map at this boundary (mirrors foldDistinctRanges).
        List<Map<String, Object>> maps = new ArrayList<>(wholeFile.size());
        for (SourceStatsContribution.WholeFile wf : wholeFile) {
            maps.add(toFlatMap(wf.stats(), wf.mtimeMillis(), wf.configFingerprint()));
        }
        if (maps.size() == 1) {
            return maps.get(0);
        }
        Map<String, Object> base = maps.get(0);
        Map<String, Object> merged = new HashMap<>(base);
        for (int i = 1; i < maps.size(); i++) {
            Map<String, Object> next = maps.get(i);
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
