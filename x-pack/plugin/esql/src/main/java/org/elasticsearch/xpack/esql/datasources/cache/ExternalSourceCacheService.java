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
            // Canonical-stripe fragments. Each carries the byte range it observed plus its stripe
            // addressing (grid + head flag); the reconciler folds fragments per stripe — identical
            // ranges from sibling scans of the same file (FORK branches, schema probes, retries)
            // dedup by identity, fragments of one stripe tile its span — and commits complete
            // stripes idempotently. Whole-file completeness is a cache-side predicate (stripes
            // {@code 0..K} present + EOF marker), assembled across queries if need be, never a
            // per-query whole-file tiling.
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
            if (wholeFile.isEmpty() == false) {
                // A whole-file read is authoritative for the whole file; fragments (if any arrived
                // alongside) add nothing and must not be summed on top.
                Map<String, Object> mergedForFile = mergeWholeFileContributions(wholeFile);
                if (mergedForFile != null && mergedForFile.isEmpty() == false) {
                    merged.put(e.getKey(), mergedForFile);
                }
                continue;
            }
            StripeDelta delta = foldStripeFragments(partials);
            if (delta == null) {
                logger.debug("dropping captured stats for [{}]: no complete stripe among fragments", e.getKey());
                continue;
            }
            commitStripeDelta(e.getKey(), delta);
        }
        reconcileSourceStats(merged);
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
     * One query's per-stripe fold for one file: the flat stats map of every stripe this query's
     * fragments proved complete, plus the file-EOF stripe ordinal when this query observed
     * end-of-input ({@code -1} otherwise). Committed idempotently into the schema cache; whole-file
     * eligibility (stripes {@code 0..lastStripeOrdinal} all committed + marker known) is evaluated
     * against the accumulated cache state, so partial knowledge composes across queries.
     */
    private record StripeDelta(Map<Long, Map<String, Object>> stripes, long lastStripeOrdinal, long mtimeMillis, String fingerprint) {}

    /**
     * Folds canonical-stripe fragments into per-stripe stats. Within a stripe, fragments dedup by
     * identity — two scans of the same file produce byte-identical fragments for the same stripe
     * (the segmentator cuts at content-determined boundaries), so a re-observed range is counted
     * once while a same-start-different-end pair is an ambiguous overlap and aborts the fold. A
     * stripe is complete when its fragments start at the stripe's head cut (or offset 0), tile
     * contiguously, and close at the next stripe cut or end-of-input. Incomplete stripes are simply
     * skipped — never a wrong answer, just a miss for that stripe. Returns {@code null} when any
     * fragment is not stripe-addressed (older node, non-striped read path) or no stripe completes.
     */
    private static StripeDelta foldStripeFragments(List<SourceStatsContribution.PartialChunk> partials) {
        if (partials.isEmpty()) {
            return null;
        }
        long stripeSize = -1L;
        long mtime = -1L;
        String fingerprint = null;
        Map<Long, TreeMap<Long, SourceStatsContribution.PartialChunk>> byStripe = new HashMap<>();
        for (SourceStatsContribution.PartialChunk pc : partials) {
            if (pc.stripeAddressed() == false) {
                return null; // un-addressable fragment — this path's contributions are not cacheable
            }
            if (stripeSize < 0) {
                stripeSize = pc.stripeSize();
                mtime = pc.mtimeMillis();
                fingerprint = pc.configFingerprint();
            } else if (stripeSize != pc.stripeSize()) {
                return null; // mixed grids (mid-upgrade settings skew) — bail rather than guess
            }
            TreeMap<Long, SourceStatsContribution.PartialChunk> frags = byStripe.computeIfAbsent(pc.stripeOrdinal(), k -> new TreeMap<>());
            SourceStatsContribution.PartialChunk existing = frags.putIfAbsent(pc.start(), pc);
            if (existing != null && existing.end() != pc.end()) {
                return null;
            }
        }
        Map<Long, Map<String, Object>> complete = new HashMap<>();
        long lastOrdinal = -1L;
        for (Map.Entry<Long, TreeMap<Long, SourceStatsContribution.PartialChunk>> e : byStripe.entrySet()) {
            long ordinal = e.getKey();
            TreeMap<Long, SourceStatsContribution.PartialChunk> frags = e.getValue();
            SourceStatsContribution.PartialChunk head = frags.firstEntry().getValue();
            if (head.stripeHead() == false && head.start() != 0) {
                continue; // head fragment missing — incomplete stripe
            }
            long expected = head.start();
            SourceStatsContribution.PartialChunk tail = null;
            boolean contiguous = true;
            for (SourceStatsContribution.PartialChunk pc : frags.values()) {
                if (pc.start() != expected) {
                    contiguous = false;
                    break;
                }
                expected = pc.end();
                tail = pc;
            }
            if (contiguous == false || tail == null) {
                continue;
            }
            // Closed = the tail reached the next stripe cut (its end crossed or landed on the next
            // nominal line) or observed end-of-input. An open tail means the stripe's remaining
            // fragments are missing from this query — skip it.
            boolean closed = tail.last() || tail.end() / stripeSize > ordinal;
            if (closed == false) {
                continue;
            }
            Map<String, Object> folded = foldFragments(frags.values(), mtime, fingerprint);
            if (folded == null || folded.isEmpty()) {
                continue;
            }
            complete.put(ordinal, folded);
            if (tail.last()) {
                lastOrdinal = ordinal;
            } else {
                // A tail that crossed more than one nominal line (a record larger than the stripe)
                // absorbs the intermediate ordinals: their nominal lines fall inside that record, so
                // they are legitimately empty stripes, committed as zero-row entries. All of the
                // fragment's rows are counted in ITS stripe's fold, so the whole-file sum stays
                // exact. Pruning-phase precondition: a grow-path fragment can carry rows past a
                // nominal line (per-stripe ATTRIBUTION, not the total, then depends on the producer
                // path) — per-stripe min/max must not be used for stripe skipping until the grow
                // path emits boundary-trimmed fragments.
                long endOrdinal = tail.end() / stripeSize;
                for (long empty = ordinal + 1; empty < endOrdinal; empty++) {
                    complete.putIfAbsent(empty, emptyStripe(mtime, fingerprint));
                }
            }
        }
        if (complete.isEmpty()) {
            return null;
        }
        return new StripeDelta(complete, lastOrdinal, mtime, fingerprint);
    }

    /**
     * Folds one stripe's contiguous fragments into a single flat stats map. The sum/extreme
     * arithmetic is delegated to the shared {@link SourceStatisticsSerializer#mergeStatistics} (the
     * same algorithm Parquet's multi-row-group merge uses), so each fragment's typed statistics are
     * re-serialized to the flat wire map here.
     */
    private static Map<String, Object> foldFragments(
        Iterable<SourceStatsContribution.PartialChunk> fragments,
        long mtimeMillis,
        String fingerprint
    ) {
        List<Map<String, Object>> maps = new ArrayList<>();
        for (SourceStatsContribution.PartialChunk pc : fragments) {
            maps.add(toFlatMap(pc.stats(), pc.mtimeMillis(), pc.configFingerprint()));
        }
        Map<String, Object> folded = maps.size() == 1 ? maps.get(0) : SourceStatisticsSerializer.mergeStatistics(maps);
        if (folded != null && maps.size() > 1) {
            // mergeStatistics rebuilds from the _stats.* keys only; re-attach the keying fields.
            if (mtimeMillis >= 0) {
                folded.put(ExternalStats.MTIME_MILLIS_KEY, mtimeMillis);
            }
            if (fingerprint != null) {
                folded.put(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint);
            }
        }
        return folded;
    }

    /** A zero-row stripe whose nominal span fell entirely inside one oversized record. */
    private static Map<String, Object> emptyStripe(long mtimeMillis, String fingerprint) {
        Map<String, Object> empty = new HashMap<>();
        empty.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 0L);
        if (mtimeMillis >= 0) {
            empty.put(ExternalStats.MTIME_MILLIS_KEY, mtimeMillis);
        }
        if (fingerprint != null) {
            empty.put(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint);
        }
        return empty;
    }

    /**
     * Commits one query's complete stripes into every matching schema-cache entry, idempotently:
     * a stripe key re-committed by a sibling scan overwrites with identical content (the fold is a
     * pure function of the stripe's bytes), so double-counting is unrepresentable. When the
     * accumulated entry holds stripes {@code 0..K} and the EOF marker, their fold is written as the
     * whole-file {@code _stats.*} keys — the optimizer's existing warm short-circuit input —
     * making the short-circuit deterministic: complete knowledge implies enrichment, possibly
     * assembled across queries.
     */
    private void commitStripeDelta(String path, StripeDelta delta) {
        if (enabled == false || path == null) {
            return;
        }
        if (delta.mtimeMillis() < 0) {
            return; // no freshness key — cannot match an entry
        }
        // Same matching + concurrency discipline as reconcileSourceStats: collect under forEach
        // (segment readLock), mutate after (see that method's javadoc for the Cache.forEach contract).
        List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>> matchingEntries = new ArrayList<>();
        schemaCache.forEach((key, existing) -> {
            if (path.equals(key.canonicalPath()) == false || key.lastModifiedEpochMillis() != delta.mtimeMillis()) {
                return;
            }
            Object existingFingerprint = existing.safeMetadata().get(ExternalStats.CONFIG_FINGERPRINT_KEY);
            if (Objects.equals(existingFingerprint, delta.fingerprint()) == false) {
                return;
            }
            matchingEntries.add(Map.entry(key, existing));
        });
        for (Map.Entry<SchemaCacheKey, SchemaCacheEntry> match : matchingEntries) {
            SchemaCacheKey key = match.getKey();
            SchemaCacheEntry existing = match.getValue();
            Map<String, Object> enriched = new HashMap<>(existing.safeMetadata());
            for (Map.Entry<Long, Map<String, Object>> stripe : delta.stripes().entrySet()) {
                enriched.put(ExternalStats.STRIPE_ENTRY_PREFIX + stripe.getKey(), stripe.getValue());
            }
            if (delta.lastStripeOrdinal() >= 0) {
                enriched.put(ExternalStats.STRIPE_LAST_INDEX_KEY, delta.lastStripeOrdinal());
            }
            Map<String, Object> wholeFile = foldCommittedStripes(enriched, delta);
            if (wholeFile != null) {
                enriched.putAll(wholeFile);
            }
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

    /**
     * Folds the committed stripes {@code 0..K} of an enriched metadata map into whole-file
     * {@code _stats.*} keys, or {@code null} when the marker is unknown or any ordinal is missing —
     * the deterministic completeness predicate replacing the old per-query whole-file tiling.
     */
    private static Map<String, Object> foldCommittedStripes(Map<String, Object> enriched, StripeDelta delta) {
        long lastIndex = enriched.get(ExternalStats.STRIPE_LAST_INDEX_KEY) instanceof Number n ? n.longValue() : -1L;
        if (lastIndex < 0) {
            return null;
        }
        List<Map<String, Object>> stripes = new ArrayList<>(Math.toIntExact(lastIndex) + 1);
        for (long k = 0; k <= lastIndex; k++) {
            if (enriched.get(ExternalStats.STRIPE_ENTRY_PREFIX + k) instanceof Map<?, ?> stripe) {
                @SuppressWarnings("unchecked")
                Map<String, Object> stripeMap = (Map<String, Object>) stripe;
                stripes.add(stripeMap);
            } else {
                return null; // ordinal missing — knowledge incomplete, keep accumulating
            }
        }
        Map<String, Object> whole = stripes.size() == 1
            ? new HashMap<>(stripes.get(0))
            : SourceStatisticsSerializer.mergeStatistics(stripes);
        if (whole != null) {
            if (delta.mtimeMillis() >= 0) {
                whole.put(ExternalStats.MTIME_MILLIS_KEY, delta.mtimeMillis());
            }
            if (delta.fingerprint() != null) {
                whole.put(ExternalStats.CONFIG_FINGERPRINT_KEY, delta.fingerprint());
            }
        }
        return whole;
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
