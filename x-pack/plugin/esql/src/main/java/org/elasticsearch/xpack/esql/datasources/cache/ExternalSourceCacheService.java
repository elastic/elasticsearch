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
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ColumnStatTypeSupport;
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

    /**
     * Per-file-path lock serializing the read-modify-write of a schema-cache entry's stripe metadata,
     * so concurrent commits of different stripes for the same file accumulate instead of the later
     * write dropping the earlier stripe (lost update). Coordinator-side commits are infrequent, so
     * this never contends the read path. {@link KeyedLock} allocates a lock per active key and frees
     * it once no thread holds it.
     */
    private final KeyedLock<String> stripeCommitLocks = new KeyedLock<>();

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
     * Returns a cached schema entry, or {@code null} on a miss (or when the cache is disabled).
     * Unlike {@link #getOrComputeSchema}, this never invokes a loader — it is the peek half of the
     * async resolve path, which fetches on a miss without holding an executor thread and then stores
     * the result via {@link #putSchema}. This trades strict thundering-herd coalescing (two
     * concurrent misses for the same key may both fetch) for the ability to resolve asynchronously.
     */
    public SchemaCacheEntry getSchemaIfPresent(SchemaCacheKey key) {
        if (enabled == false) {
            return null;
        }
        return schemaCache.get(key);
    }

    /** Stores a schema entry. No-op when the cache is disabled. Pairs with {@link #getSchemaIfPresent}. */
    public void putSchema(SchemaCacheKey key, SchemaCacheEntry entry) {
        if (enabled == false) {
            return;
        }
        schemaCache.put(key, entry);
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
            // is written, rather than a silent fall-through. WholeFile and StripeFragment carry stats;
            // Poison is gate-only.
            boolean poisoned = false;
            List<SourceStatsContribution.WholeFile> wholeFile = new ArrayList<>(contributions.size());
            // Orthogonal-model stripe fragments. Each is the records of one canonical stripe a chunk
            // observed, carrying the reader-assigned ordinal + record-canonical sub-range + tiling
            // anchors; the reconciler interval-covers fragments per stripe — misaligned tilings from
            // sibling scans (FORK branches, retries, different chunkings) fold to the same stripe
            // stats — and commits complete stripes idempotently. Whole-file completeness is a
            // cache-side predicate (stripes {@code 0..K} present + EOF marker), assembled across
            // queries if need be, never a per-query whole-file tiling.
            List<SourceStatsContribution.StripeFragment> fragments = new ArrayList<>(contributions.size());
            for (Map<String, Object> raw : contributions) {
                switch (SourceStatsContribution.classify(raw)) {
                    case SourceStatsContribution.Poison ignored -> poisoned = true;
                    case SourceStatsContribution.WholeFile wf -> wholeFile.add(wf);
                    case SourceStatsContribution.StripeFragment f -> fragments.add(f);
                }
            }
            // A poisoned file (a scan that did NOT complete cleanly -- error/truncation/cancel, so its
            // extent is not deterministic) is discarded entirely. A row DROPPED by the error policy is
            // not poison: survivors are deterministic and commit normally.
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
            StripeDelta delta = foldStripeFragments(fragments);
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
    private record StripeDelta(
        Map<Long, Map<String, Object>> stripes,
        long lastStripeOrdinal,
        long mtimeMillis,
        String fingerprint,
        long stripeSize
    ) {}

    /**
     * Folds orthogonal-model stripe fragments into per-stripe stats by interval-cover. Fragments are
     * grouped by their reader-assigned ordinal (NOT inferred from byte offset); within each stripe a
     * greedy interval-cover walks from the fragment covering the stripe's left grid line ({@code atStripeStart}
     * — a byte-cover predicate, not "the stripe's first record") along contiguous byte sub-ranges to the
     * fragment covering the right grid line ({@code atStripeEnd} / EOF).
     * <p>
     * The cover is robust to misaligned tilings not because the fragment endpoints match across scans (they
     * do not — a chunk boundary landing mid-stripe is a chunk/grid byte position, and different chunkings
     * split a stripe differently) but because per-stripe attribution is by record START: each record counts
     * into its own stripe ordinal regardless of chunking, so different scans produce different byte tilings of
     * the SAME stripe grid cell that fold to the same stripe stats. A FORK branch that covered a stripe in one
     * fragment and a sibling that split it at a different chunk boundary both produce a valid chain over that
     * cell. The greedy walk consumes one fragment
     * per position, so an alternative scan's overlapping fragments are simply skipped (no double-count;
     * a stripe is folded once). A stripe whose fragments leave a gap before reaching {@code atStripeEnd}
     * is incomplete and skipped — a safe miss, never wrong. Returns {@code null} when any fragment is
     * not stripe-addressed (older node, reader not yet emitting stripes) or no stripe completes.
     */
    private static StripeDelta foldStripeFragments(List<SourceStatsContribution.StripeFragment> fragments) {
        if (fragments.isEmpty()) {
            return null;
        }
        long stripeSize = -1L;
        long mtime = -1L;
        String fingerprint = null;
        // ordinal -> (start offset -> fragments starting there). Multiple fragments can share a start
        // (the same stripe prefix observed by two scans), so the value is a list.
        Map<Long, Map<Long, List<SourceStatsContribution.StripeFragment>>> byStripe = new HashMap<>();
        for (SourceStatsContribution.StripeFragment f : fragments) {
            if (f.stripeAddressed() == false) {
                return null; // un-addressable fragment — this path's contributions are not cacheable
            }
            if (stripeSize < 0) {
                stripeSize = f.stripeSize();
                mtime = f.mtimeMillis();
                fingerprint = f.configFingerprint();
            } else if (stripeSize != f.stripeSize()) {
                return null; // mixed grids (mid-upgrade settings skew) — bail rather than guess
            } else if (mtime != f.mtimeMillis() || Objects.equals(fingerprint, f.configFingerprint()) == false) {
                // Fragments for the same path observed at different mtimes (the file was modified between
                // sibling scans) or under different configs describe different file versions. Folding them
                // would mix versions and commit the result under the first fragment's freshness key — a
                // wrong stat. Bail rather than guess; the next query re-harvests against the live version.
                return null;
            }
            byStripe.computeIfAbsent(f.ordinal(), k -> new HashMap<>()).computeIfAbsent(f.start(), s -> new ArrayList<>()).add(f);
        }
        Map<Long, Map<String, Object>> complete = new HashMap<>();
        long lastOrdinal = -1L;
        for (Map.Entry<Long, Map<Long, List<SourceStatsContribution.StripeFragment>>> e : byStripe.entrySet()) {
            long ordinal = e.getKey();
            Map<Long, List<SourceStatsContribution.StripeFragment>> byStart = e.getValue();
            List<SourceStatsContribution.StripeFragment> chain = coverStripe(byStart);
            if (chain == null) {
                continue; // incomplete — gap before the stripe end; safe miss for this stripe
            }
            Map<String, Object> folded = foldFragments(chain, mtime, fingerprint);
            if (folded == null) {
                continue;
            }
            complete.put(ordinal, folded);
            if (chain.get(chain.size() - 1).eof()) {
                lastOrdinal = ordinal;
            }
        }
        if (complete.isEmpty()) {
            return null;
        }
        return new StripeDelta(complete, lastOrdinal, mtime, fingerprint, stripeSize);
    }

    /**
     * Greedy interval-cover of one stripe from the fragment covering its left grid line ({@code atStripeStart})
     * to the fragment covering its right grid line ({@code atStripeEnd}), picking one fragment per position.
     * Returns the covering chain, or {@code null} when no {@code atStripeStart} anchor exists or a gap is hit
     * before the stripe end.
     * Empty stripes (a record larger than the grid skips an ordinal entirely) arrive as a single
     * zero-length fragment flagged both start and end, which the walk accepts immediately.
     */
    private static List<SourceStatsContribution.StripeFragment> coverStripe(
        Map<Long, List<SourceStatsContribution.StripeFragment>> byStart
    ) {
        SourceStatsContribution.StripeFragment anchor = null;
        for (List<SourceStatsContribution.StripeFragment> bucket : byStart.values()) {
            for (SourceStatsContribution.StripeFragment f : bucket) {
                if (f.atStripeStart()) {
                    anchor = f;
                    break;
                }
            }
            if (anchor != null) {
                break;
            }
        }
        if (anchor == null) {
            return null; // never saw the stripe's first record — incomplete
        }
        List<SourceStatsContribution.StripeFragment> chain = new ArrayList<>();
        long pos = anchor.start();
        // Bounded by the total fragment count; each step advances pos or terminates.
        int guard = 0;
        int limit = 0;
        for (List<SourceStatsContribution.StripeFragment> bucket : byStart.values()) {
            limit += bucket.size();
        }
        while (guard++ <= limit) {
            List<SourceStatsContribution.StripeFragment> candidates = byStart.get(pos);
            if (candidates == null || candidates.isEmpty()) {
                return null; // gap — the stripe is not fully covered by this query's fragments
            }
            SourceStatsContribution.StripeFragment pick = candidates.get(0);
            for (SourceStatsContribution.StripeFragment c : candidates) {
                if (c.atStripeEnd()) {
                    pick = c; // prefer terminating the cover when we can
                    break;
                }
            }
            chain.add(pick);
            if (pick.atStripeEnd()) {
                return chain; // reached the stripe's last record
            }
            if (pick.end() <= pos) {
                return null; // malformed non-advancing fragment — bail rather than loop
            }
            pos = pick.end();
        }
        return null; // exceeded the fragment budget without closing — malformed
    }

    /**
     * Folds one stripe's contiguous fragments into a single flat stats map. The sum/extreme
     * arithmetic is delegated to the shared {@link SourceStatisticsSerializer#mergeStatistics} (the
     * same algorithm Parquet's multi-row-group merge uses), so each fragment's typed statistics are
     * re-serialized to the flat wire map here.
     */
    private static Map<String, Object> foldFragments(
        List<SourceStatsContribution.StripeFragment> chain,
        long mtimeMillis,
        String fingerprint
    ) {
        List<Map<String, Object>> maps = new ArrayList<>(chain.size());
        for (SourceStatsContribution.StripeFragment f : chain) {
            maps.add(toFlatMap(f.stats(), f.mtimeMillis(), f.configFingerprint()));
        }
        // false: text-only fold (see foldCommittedStripes) — an absent column is "not harvested", not all-null.
        Map<String, Object> folded = maps.size() == 1 ? maps.get(0) : SourceStatisticsSerializer.mergeStatistics(maps, false);
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
        // Serialize the read-modify-write per file path: concurrent commits of different stripes for the
        // same file would otherwise each copy the same entry snapshot and the later put would drop the
        // earlier stripe (lost update). Commits are coordinator-side and infrequent.
        try (Releasable ignored = stripeCommitLocks.acquire(path)) {
            applyStripeDelta(path, delta);
        }
    }

    /**
     * The locked read-modify-write of {@link #commitStripeDelta}: collect matching entries under the
     * {@code Cache.forEach} segment readLock (see {@code reconcileSourceStats} for that contract), then
     * enrich and re-put each. Must run holding the per-path {@link #stripeCommitLocks} lock.
     */
    private void applyStripeDelta(String path, StripeDelta delta) {
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
            // Grid identity gate: stripe ordinals are only comparable within one grid. If the entry's
            // committed stripe state was accumulated on a DIFFERENT grid (data nodes running different
            // stripe.size values — rolling restart, config drift), merging this delta's ordinals into it
            // would let the 0..K fold serve a silently wrong count over a "complete" cover. Clear the
            // stale-grid state and restart accumulation on this delta's grid (safe-miss, never mixed).
            // An entry with stripes but NO stamp predates the stamp — its grid is unknowable, same reset.
            Object entryGrid = enriched.get(ExternalStats.STRIPE_GRID_KEY);
            boolean gridMatches = entryGrid instanceof Number n && n.longValue() == delta.stripeSize();
            if (gridMatches == false) {
                enriched.keySet().removeIf(k -> k.startsWith(ExternalStats.STRIPE_ENTRY_PREFIX));
                enriched.remove(ExternalStats.STRIPE_LAST_INDEX_KEY);
            }
            enriched.put(ExternalStats.STRIPE_GRID_KEY, delta.stripeSize());
            for (Map.Entry<Long, Map<String, Object>> stripe : delta.stripes().entrySet()) {
                // Push the resolved column type down to each stripe's min/max before it is stored, so the
                // 0..K fold (foldCommittedStripes -> mergeStatistics) never folds a Long extremum against a
                // Double one for the same column. dropUnrepresentable=false: an unrepresentable value is left
                // for that fold's POISON to safe-miss the whole column (a per-stripe drop would fold a subset).
                Map<String, Object> stripeStats = coerceColumnStatsToResolvedTypes(
                    stripe.getValue(),
                    existing.columnNames(),
                    existing.columnTypes(),
                    false
                );
                enriched.put(ExternalStats.STRIPE_ENTRY_PREFIX + stripe.getKey(), stripeStats);
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
     * Normalizes a contribution's per-column {@code min}/{@code max} to the cache entry's RESOLVED column
     * type before it is merged in. This pushes the aggregate's type down to the stats layer: a column's
     * resolved {@link DataType} (the entry's {@code columnTypes}) is exactly the type the query's MIN/MAX
     * aggregate reads the column as ({@code af.dataType()}), so once every contribution's min/max is in that
     * type, the merge only ever folds same-typed values and the served value is bit-identical to a full scan.
     *
     * <p>Why this is needed: a sample-inferred reader (NDJSON typing a UInt64 column LONG on a read whose
     * sample saw only {@code <= Long.MAX}, DOUBLE on a read that hit a bigger value) can contribute a
     * {@code Long} extremum from one stripe and a {@code Double} from another for the SAME column. Folding a
     * {@code Long} with a {@code Double} hits {@code SplitStats}'s "intentionally incompatible above 2^53"
     * branch → POISON → the column's min/max is dropped → safe-miss → full scan. Coercing every contribution
     * to the resolved type up front removes the collision at its source.
     *
     * <p>Coercion preserves the extremum (it is NOT bit-exact above 2^53): IEEE round-to-nearest is monotonic,
     * so {@code (double) max(longs) == max((double) longs)} — an individual value may round, but a full scan
     * reads the column in the same resolved DOUBLE type and rounds it identically, so the served value equals
     * the scan's (result-parity, not bit-exactness).
     * When a value cannot be represented in the resolved type (the genuinely inconsistent case — e.g. a
     * {@code Double > Long.MAX} for a column the entry resolved to LONG), it is NOT coerced: in the stripe
     * path ({@code dropUnrepresentable=false}) it is left for the existing POISON fold to safe-miss the whole
     * column; in the whole-file path ({@code dropUnrepresentable=true}, no POISON fold) its min/max is dropped
     * so the serve safe-misses rather than coercing a wrong value (e.g. {@code ((Number) d).longValue()}
     * overflow garbage). Returns the input unchanged when no column needed coercion.
     */
    static Map<String, Object> coerceColumnStatsToResolvedTypes(
        Map<String, Object> statsMap,
        String[] columnNames,
        DataType[] columnTypes,
        boolean dropUnrepresentable
    ) {
        if (statsMap == null || statsMap.isEmpty() || columnNames == null || columnTypes == null) {
            return statsMap;
        }
        Map<String, Object> out = null; // copied lazily, only if some column actually needs a change
        int n = Math.min(columnNames.length, columnTypes.length);
        for (int i = 0; i < n; i++) {
            DataType type = columnTypes[i];
            if (isNumericStatType(type) == false) {
                continue; // BYTESREF / BOOLEAN min/max carry no Long-vs-Double ambiguity
            }
            for (String[] pair : List.of(
                new String[] {
                    SourceStatisticsSerializer.columnMinKey(columnNames[i]),
                    SourceStatisticsSerializer.columnMinUnservableKey(columnNames[i]) },
                new String[] {
                    SourceStatisticsSerializer.columnMaxKey(columnNames[i]),
                    SourceStatisticsSerializer.columnMaxUnservableKey(columnNames[i]) }
            )) {
                String key = pair[0];
                Object value = statsMap.get(key);
                if (value instanceof Number == false) {
                    continue;
                }
                Object coerced = coerceNumberToType((Number) value, type); // null => not representable in `type`
                if (coerced != null) {
                    if (coerced.equals(value)) {
                        continue; // already the resolved type — no change
                    }
                    if (out == null) {
                        out = new HashMap<>(statsMap);
                    }
                    out.put(key, coerced);
                } else if (dropUnrepresentable) {
                    if (out == null) {
                        out = new HashMap<>(statsMap);
                    }
                    // Not representable in the resolved type on the whole-file (last-writer-wins) path. Merely
                    // REMOVING the value would let a STALE committed extremum survive the reconcile's putAll
                    // overlay (enriched keeps the old value the removed key no longer overwrites); write the
                    // unservable MARKER instead (and drop the value) so marker-wins normalization in
                    // SplitStats.of forces a safe-miss over any stale value.
                    out.remove(key);
                    out.put(pair[1], Boolean.TRUE);
                }
                // else: not representable on the stripe path — leave it for the POISON fold to safe-miss.
            }
        }
        return out != null ? out : statsMap;
    }

    /** True for the types whose min/max is a numeric value that can flap Long/Double across sampled reads. */
    private static boolean isNumericStatType(DataType type) {
        // A type is numeric-coercible iff the shared table assigns it a non-NONE coercion. This is ORTHOGONAL
        // to servability — the counters are servable but NOT numeric-coercible (dispatching on servable() would
        // wrongly coerce them), while UNSIGNED_LONG is both servable and EXACT_LONG-coercible (it still needs the
        // stale-extremum drop) — so it dispatches on coercion(), not servable().
        ColumnStatTypeSupport support = ColumnStatTypeSupport.of(type);
        return support != null && support.coercion() != ColumnStatTypeSupport.StatCoercion.NONE;
    }

    /**
     * Coerces a min/max {@link Number} to the Java representation of the column's resolved {@link DataType},
     * or {@code null} when the value cannot be represented exactly in that type. DOUBLE accepts any number
     * (widening is exact for an extremum); LONG-family and INTEGER accept only values that round-trip exactly.
     */
    private static Object coerceNumberToType(Number value, DataType type) {
        return switch (ColumnStatTypeSupport.of(type).coercion()) {
            case WIDEN_DOUBLE -> value.doubleValue();
            case EXACT_LONG -> toExactLong(value);
            case EXACT_INT -> {
                Long l = toExactLong(value);
                yield (l != null && l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) ? Integer.valueOf(l.intValue()) : null;
            }
            // Unreachable: callers gate on isNumericStatType, so only non-NONE coercions reach here.
            case NONE -> throw new AssertionError("coerceNumberToType called for non-numeric stat type: " + type);
        };
    }

    /** The exact {@code long} value of {@code value}, or {@code null} if it has a fractional part or is out of long range. */
    private static Long toExactLong(Number value) {
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return value.longValue();
        }
        if (value instanceof Double || value instanceof Float) {
            double d = value.doubleValue();
            if (Double.isFinite(d) == false) {
                return null;
            }
            // Not DataTypeConverter.safeToLong: that rounds (Math.round) and throws out of range. An extremum
            // must be exact-or-miss, so we accept only a lossless round-trip and return null otherwise.
            long asLong = (long) d;
            return (double) asLong == d ? asLong : null;
        }
        return null;
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
        // implicitNullsForAbsentColumn=false: stripe deltas are text-only, and under the default PROJECTED
        // scope different cold queries harvest different columns, so an entry's stripes can carry non-uniform
        // column sets. A column absent from a stripe means "not harvested by that scan," NOT "all-null" — the
        // footer (true) contract would fold that stripe's rows into the column's null_count and under-count
        // COUNT(col) / serve a subset MIN/MAX. false drops a column missing from any stripe so it safe-misses,
        // matching the cross-file text merge in ExternalSourceResolver.
        Map<String, Object> whole = stripes.size() == 1
            ? new HashMap<>(stripes.get(0))
            : SourceStatisticsSerializer.mergeStatistics(stripes, false);
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
        // typed contributions to the wire map at this boundary (mirrors foldFragments).
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
            if (agreesWithBase(base, next) == false) {
                // Two whole-file scans of the SAME (path, config, mtime) file disagree on row count / mtime /
                // fingerprint — non-deterministic, so neither can be trusted. Assert-and-bail: fail fast in test
                // (an invariant violation), but in production SAFE-MISS (return null → the caller skips caching)
                // rather than silently first-wins-merging a stat that would then serve wrong warm answers.
                assert false
                    : "whole-file contributions for the same file must agree on row count, mtime, and config fingerprint: "
                        + base
                        + " vs "
                        + next;
                return null;
            }
            for (Map.Entry<String, Object> e : next.entrySet()) {
                if (e.getKey().startsWith(SourceStatisticsSerializer.STATS_COL_PREFIX)) {
                    Object prev = merged.putIfAbsent(e.getKey(), e.getValue());
                    if (prev != null && Objects.equals(prev, e.getValue()) == false) {
                        // Same-file column-stat disagreement — safe-miss for the same reason.
                        assert false
                            : "whole-file contributions disagree on column stat [" + e.getKey() + "]: " + prev + " vs " + e.getValue();
                        return null;
                    }
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
            // Serialize the read-modify-write per file path with the same lock commitStripeDelta uses:
            // this method and applyStripeDelta both collect matching entries under forEach, then enrich
            // and re-put each. A reconcile racing a stripe commit (or another reconcile) for the same
            // path would otherwise each snapshot the same entry and the later put would drop the
            // earlier's enrichment (lost update). The lock keyspace (canonical path) is shared, so the
            // two writers serialize against each other.
            try (Releasable ignored = stripeCommitLocks.acquire(path)) {
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
                    // Push the resolved column type down before enriching. This whole-file path is
                    // last-writer-wins (no POISON fold), so an unrepresentable value (e.g. a Double past
                    // Long.MAX for a LONG-resolved column) is DROPPED rather than stored — otherwise the
                    // serve would coerce it to the resolved type and produce a wrong value.
                    enriched.putAll(coerceColumnStatsToResolvedTypes(mergedStats, existing.columnNames(), existing.columnTypes(), true));
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
