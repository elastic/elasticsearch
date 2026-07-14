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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ColumnStatTypeSupport;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;

/**
 * Coordinator-only, in-memory cache service for external source metadata.
 * Maintains three independent caches:
 * <ul>
 *   <li>Schema cache (20% of budget, 5m TTL) — shared across users</li>
 *   <li>File-metadata cache (count-bounded, schema TTL) — {@code {length, mtime}} per path, shared across users</li>
 *   <li>Listing cache (80% of budget, 30s TTL) — isolated by credential hash</li>
 * </ul>
 * Uses hard TTL via {@code setExpireAfterWrite} for the initial implementation.
 * Lazy TTL with ETag revalidation is deferred to a follow-up PR.
 */
public class ExternalSourceCacheService implements Closeable {

    private static final Logger logger = LogManager.getLogger(ExternalSourceCacheService.class);

    private final Cache<SchemaCacheKey, SchemaCacheEntry> schemaCache;
    private final Cache<FileMetadataCacheKey, FileMetadata> fileMetadataCache;
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

    /**
     * A resolve-registered promise that a dataset-level aggregate should be materialized once the
     * in-flight cold scan's reconcile proves every file of the set whole-file complete. Registered by
     * the resolver when the multi-file per-file aggregate comes back incomplete (the cold query);
     * fulfilled — sum of per-file row counts committed under the dataset key — only when EVERY path
     * folded to whole-file completeness in one reconcile with the expected mtime and config
     * fingerprint. A poisoned, incomplete, or mid-flight-modified file leaves the descriptor
     * unfulfilled: correct-or-miss, exactly like the per-file rail.
     */
    private record PendingDatasetAggregate(
        SchemaCacheKey datasetKey,
        Map<String, Long> pathToMtimeMillis,
        String configFingerprint,
        String sourceType,
        String location,
        long registeredAtNanos
    ) {}

    /**
     * Bounded registry of pending dataset aggregates, insertion-ordered so overflow drops the oldest.
     * Bounded on TWO axes: descriptor count ({@link #MAX_PENDING_DATASET_AGGREGATES}) and total stored
     * paths across all descriptors ({@link #MAX_PENDING_TOTAL_PATHS}) — each descriptor copies a
     * path→mtime map for every file of its glob, so without the path budget 64 descriptors of
     * MAX_DISCOVERED_FILES-sized globs would pin unbounded coordinator heap outside any cache budget.
     * Guarded by its own monitor ({@code synchronized (pendingDatasetAggregates)}): registration is
     * per-resolve and fulfillment is per-reconcile, both rare, so a plain lock never contends.
     * Deliberately NOT a {@link CacheBuilder} cache: that offers a count bound or a weight bound but not
     * both at once, has no fulfill-then-remove semantics (a promise is consumed exactly once, not
     * evicted), and its {@code expireAfterWrite} TTL is the wrong clock — the promise horizon must be
     * decoupled from the schema TTL (see {@link #PENDING_DATASET_AGGREGATE_TTL_NANOS}).
     */
    private static final int MAX_PENDING_DATASET_AGGREGATES = 64;
    private static final int MAX_PENDING_TOTAL_PATHS = 65_536;

    /**
     * Entry-count cap for the file-metadata cache. Unlike the schema and listing caches (byte-weighted,
     * variable-size values), a {@link FileMetadata} is two {@code long}s behind a small path key, so the
     * cache is bounded by count rather than bytes — no per-entry byte weigher. 100k tiny entries is a few
     * tens of MB worst case, and, being hard-TTL-bounded by the schema TTL, the live set is normally far
     * smaller. Kept a constant rather than a cluster setting: no workload has needed to tune it, and a
     * public setting is a permanent support surface — it can be promoted to a setting later if a real need
     * appears.
     */
    private static final int FILE_METADATA_CACHE_MAX_ENTRIES = 100_000;
    private final LinkedHashMap<SchemaCacheKey, PendingDatasetAggregate> pendingDatasetAggregates = new LinkedHashMap<>();

    /**
     * Pending-descriptor expiry horizon. Deliberately a FIXED constant DECOUPLED from the schema TTL:
     * the promise is registered at resolve time (before the scan) and fulfilled at reconcile time
     * (after the scan), so it must comfortably outlive the longest realistic cold scan — a schema-TTL
     * horizon (5m default) would expire the promise of exactly the multi-minute scans this mechanism
     * exists for, before their own reconcile could fulfill it. A stale promise costs nothing: the
     * registry is tiny and doubly bounded, and fulfillment re-validates every path's mtime and config
     * fingerprint, so correctness never depends on this horizon. (Fulfillment writes a FRESH cache
     * entry whose own TTL starts at write time — the promise's age does not leak into the entry's.)
     */
    private static final long PENDING_DATASET_AGGREGATE_TTL_NANOS = TimeUnit.HOURS.toNanos(1);

    private final LongAdder datasetAggregateHits = new LongAdder();
    private final LongAdder datasetAggregateMisses = new LongAdder();
    private final LongAdder statsAggregateIncomplete = new LongAdder();

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

        // Shares the schema TTL: the metadata entry is the version token that rebuilds the schema key, so
        // its freshness horizon must match the schema entry it gates. No byte weigher — entries are tiny and
        // fixed-size, so the cache is bounded by a generous entry count instead of the byte budget.
        this.fileMetadataCache = CacheBuilder.<FileMetadataCacheKey, FileMetadata>builder()
            .setMaximumWeight(FILE_METADATA_CACHE_MAX_ENTRIES)
            .setExpireAfterWrite(schemaTtl)
            .build();

        this.listingCache = CacheBuilder.<ListingCacheKey, FileList>builder()
            .setMaximumWeight(listingBudget)
            .setExpireAfterWrite(listingTtl)
            .weigher((key, value) -> value.estimatedBytes())
            .build();

        logger.info(
            "External source cache initialized: total=[{}], schema=[{}], listing=[{}], fileMetadataMaxEntries=[{}], "
                + "schemaTTL=[{}], listingTTL=[{}]",
            totalBudget,
            ByteSizeValue.ofBytes(schemaBudget),
            ByteSizeValue.ofBytes(listingBudget),
            FILE_METADATA_CACHE_MAX_ENTRIES,
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
     * Returns cached {@link FileMetadata} or computes it via the loader. The loader — a single object
     * probe (mtime + length), on S3 one {@code bytes=-1} GET — is only invoked on a miss. When the cache
     * is disabled, the loader is called directly (bypassing the cache), so the probe still happens every
     * query. Mirrors {@link #getOrComputeSchema}: this is the amortization lever that removes the
     * per-query warm-path metadata probe for single-file sources.
     */
    public FileMetadata getOrComputeFileMetadata(FileMetadataCacheKey key, CacheLoader<FileMetadataCacheKey, FileMetadata> loader)
        throws Exception {
        if (enabled == false) {
            return loader.load(key);
        }
        return fileMetadataCache.computeIfAbsent(key, loader);
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
     * Returns the dataset-level aggregate stats stored under {@code key} (see
     * {@link SchemaCacheKey#forDatasetAggregate}), or {@code null} on a miss. The map carries only
     * dataset-INDEPENDENT-of-declaration keys — today just {@code _stats.row_count} — never per-column
     * stats, so serving it can never leak a wrongly-normalized MIN/MAX (those keep re-scanning until the
     * per-file rail serves them). A found entry is re-put to refresh the {@code expireAfterWrite} clock
     * and LRU position: the entry is the warm path's single survival dependency, so a hot dataset must not
     * decay mid-use while its per-file siblings churn (same revive the per-file reconcile applies to
     * recovered entries). Does NOT touch the hit/miss counters — those are resolver-driven at the serve
     * decision (see {@link #recordDatasetAggregateHit} / {@link #recordDatasetAggregateMiss}).
     */
    @Nullable
    public Map<String, Object> getDatasetAggregate(SchemaCacheKey key) {
        if (enabled == false || key == null) {
            return null;
        }
        SchemaCacheEntry entry = schemaCache.get(key);
        if (entry == null || entry.safeMetadata().get(SourceStatisticsSerializer.STATS_ROW_COUNT) instanceof Number == false) {
            return null;
        }
        // No hit/miss counting here: every resolve prefetches (including healthy warm resolves whose
        // per-file merge will succeed and never need the aggregate), so a get-side counter would count
        // non-events. The resolver counts a hit/miss only when the fallback was actually needed — see
        // recordDatasetAggregateHit / recordDatasetAggregateMiss.
        schemaCache.put(key, entry); // refresh write-time TTL + LRU recency
        return entry.safeMetadata();
    }

    /**
     * Stores the dataset-level row-count aggregate for one resolved file set. The entry is a synthetic
     * {@link SchemaCacheEntry} (no columns; {@code safeMetadata} = the row count) so all existing schema
     * cache plumbing — weigher, TTL, enable/disable, clearAll, usage stats — applies unchanged.
     */
    public void putDatasetAggregate(SchemaCacheKey key, long rowCount, String sourceType, String location) {
        if (enabled == false || key == null || rowCount < 0) {
            return;
        }
        SchemaCacheEntry entry = new SchemaCacheEntry(
            new String[0],
            new DataType[0],
            new Nullability[0],
            new boolean[0],
            sourceType,
            location,
            Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount),
            Map.of(),
            System.currentTimeMillis()
        );
        schemaCache.put(key, entry);
    }

    /**
     * Registers a {@link PendingDatasetAggregate} promise for the reconcile of the in-flight cold scan
     * to fulfill. Doubly bounded ({@value #MAX_PENDING_DATASET_AGGREGATES} descriptors /
     * {@value #MAX_PENDING_TOTAL_PATHS} total stored paths, oldest dropped) and expired on
     * {@link #PENDING_DATASET_AGGREGATE_TTL_NANOS}, so an abandoned promise (query failed, scan
     * partial) costs nothing. Re-registering the same dataset key replaces the previous promise
     * (same content, refreshed clock).
     * <p>
     * {@code expectedFileCount} is the resolved listing's file COUNT (a multiset count: a
     * comma-separated source list can name the same file twice, and the scan then counts its rows
     * twice). {@code pathToMtimeMillis} is a map and therefore deduplicates; when the two disagree the
     * promise-side sum over unique paths would publish an undercount relative to the scan, so
     * registration is refused — the dataset stays on the re-scan path (correct-or-miss).
     */
    public void registerPendingDatasetAggregate(
        SchemaCacheKey datasetKey,
        Map<String, Long> pathToMtimeMillis,
        int expectedFileCount,
        String configFingerprint,
        String sourceType,
        String location
    ) {
        if (enabled == false || datasetKey == null || pathToMtimeMillis == null || pathToMtimeMillis.size() < 2) {
            return;
        }
        if (pathToMtimeMillis.size() != expectedFileCount) {
            // Duplicate paths in the listing — a unique-path sum would undercount the multiset scan. Same
            // guard as ExternalSourceResolver#listingPathsAreDistinct on the write-through rail, encoded
            // here as size-vs-count because the map has already deduplicated.
            return;
        }
        if (pathToMtimeMillis.size() > MAX_PENDING_TOTAL_PATHS) {
            return; // one glob beyond the whole registry's path budget — safe-miss rather than pin the heap
        }
        PendingDatasetAggregate pending = new PendingDatasetAggregate(
            datasetKey,
            Map.copyOf(pathToMtimeMillis),
            configFingerprint,
            sourceType,
            location,
            System.nanoTime()
        );
        synchronized (pendingDatasetAggregates) {
            pendingDatasetAggregates.remove(datasetKey); // re-insert at tail so eviction order tracks freshness
            pendingDatasetAggregates.put(datasetKey, pending);
            Iterator<PendingDatasetAggregate> it = pendingDatasetAggregates.values().iterator();
            int totalPaths = 0;
            for (PendingDatasetAggregate p : pendingDatasetAggregates.values()) {
                totalPaths += p.pathToMtimeMillis().size();
            }
            while ((pendingDatasetAggregates.size() > MAX_PENDING_DATASET_AGGREGATES || totalPaths > MAX_PENDING_TOTAL_PATHS)
                && it.hasNext()) {
                PendingDatasetAggregate evicted = it.next();
                if (evicted == pending) {
                    break; // never evict the entry just registered; both bounds are already satisfied for it alone
                }
                totalPaths -= evicted.pathToMtimeMillis().size();
                it.remove();
            }
        }
    }

    /**
     * Counts a resolve whose per-file stats aggregate came back incomplete (observability). Only
     * aggregate-QUALIFYING resolves are counted (multi-file, text-format, file-set-fingerprint-bearing,
     * cacheable provider): the caller sits after the {@code datasetKey == null} early-return in
     * {@code ExternalSourceResolver#applyDatasetAggregate}, so non-qualifying incompletes never reach it.
     * <p>
     * Today {@code stats_aggregate.incomplete == dataset_aggregate.hits + dataset_aggregate.misses}
     * exactly (every incomplete then resolves to precisely one of hit/miss on the same needed path). Kept
     * as its own counter so the "the per-file merge was incomplete" signal survives a future serve path
     * that grows a third outcome and breaks that identity.
     */
    public void recordStatsAggregateIncomplete() {
        statsAggregateIncomplete.increment();
    }

    /**
     * Counts a dataset-aggregate fallback that was NEEDED (the per-file merge came back incomplete) AND
     * PRESENT — i.e. the memoized aggregate was actually served. Resolver-driven and symmetric with
     * {@link #recordDatasetAggregateMiss} so that {@code hits / (hits + misses)} is a true fallback hit
     * rate over the resolves that actually needed the aggregate.
     */
    public void recordDatasetAggregateHit() {
        datasetAggregateHits.increment();
    }

    /**
     * Counts a dataset-aggregate fallback that was NEEDED (the per-file merge came back incomplete) but
     * ABSENT. Deliberately resolver-driven rather than incremented inside {@link #getDatasetAggregate}:
     * every multi-file resolve prefetches the aggregate — including healthy warm resolves whose per-file
     * merge succeeds and never consumes it — so a get-side counter would count non-events. Its hit twin
     * ({@link #recordDatasetAggregateHit}) is counted at the same serve decision, so the two share a
     * denominator (resolves that needed the fallback) and their ratio is meaningful.
     */
    public void recordDatasetAggregateMiss() {
        datasetAggregateMisses.increment();
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
        // Snapshot the glob's entries BEFORE any commit writes: the first put() sweeps TTL-expired
        // entries, evicting files #2..N's entries before their deltas apply. See snapshotEntriesByPath.
        Map<String, List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>>> preCommitSnapshot = snapshotEntriesByPath(
            contributionsPerFile.keySet()
        );
        // LinkedHashMap, not HashMap: commit whole-file entries in the caller's contribution order so the
        // reconcile (and which sibling a capacity/TTL sweep hits during a commit) is deterministic rather
        // than dependent on path hashCode. Final per-entry state is order-independent (each path keys a
        // distinct entry; a swept sibling is recovered per key), so this only pins reproducibility.
        Map<String, Map<String, Object>> merged = new LinkedHashMap<>(contributionsPerFile.size());
        // Per-path whole-file stats this reconcile PROVED complete (a whole-file contribution, or a
        // stripe delta whose committed fold reached 0..K+EOF). Input to the pending dataset-aggregate
        // fulfillment below: a dataset promise is honored only when every one of its paths lands here.
        // Only worth tracking while a promise is actually pending — the common no-promise reconcile
        // skips the bookkeeping and the direct delta fold entirely. A promise registered concurrently
        // after this check belongs to a resolve whose own scan has not reconciled yet, so skipping it
        // this reconcile loses nothing (its own reconcile fulfills it).
        final boolean anyPendingDatasetAggregate;
        synchronized (pendingDatasetAggregates) {
            anyPendingDatasetAggregate = pendingDatasetAggregates.isEmpty() == false;
        }
        Map<String, Map<String, Object>> completedWholeFile = new HashMap<>(contributionsPerFile.size());
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
                    if (anyPendingDatasetAggregate) {
                        completedWholeFile.put(e.getKey(), mergedForFile);
                    }
                }
                continue;
            }
            StripeDelta delta = foldStripeFragments(fragments);
            if (delta == null) {
                logger.debug("dropping captured stats for [{}]: no complete stripe among fragments", e.getKey());
                continue;
            }
            Map<String, Object> foldedWholeFile = commitStripeDelta(e.getKey(), delta, preCommitSnapshot.get(e.getKey()));
            if (anyPendingDatasetAggregate) {
                if (foldedWholeFile == null) {
                    // The entry-side fold requires a matching schema entry to still be cached, but under the
                    // exact LRU pressure the dataset aggregate exists for, those entries are already
                    // weight-evicted at reconcile time. A full cold scan's delta is whole-file complete on
                    // its own (every stripe 0..EOF observed by THIS query), so fold it directly — the
                    // dataset promise must not depend on per-file cache survival.
                    foldedWholeFile = foldQueryDeltaStripes(delta);
                }
                if (foldedWholeFile != null) {
                    completedWholeFile.put(e.getKey(), foldedWholeFile);
                }
            }
        }
        reconcileSourceStats(merged, preCommitSnapshot);
        if (anyPendingDatasetAggregate) {
            fulfillPendingDatasetAggregates(completedWholeFile);
        }
    }

    /**
     * Honors every {@link PendingDatasetAggregate} promise this reconcile fully covers: each of the
     * promise's paths must appear in {@code completedWholeFile} with a numeric row count, the promised
     * mtime, and the promised config fingerprint. The sum is committed under the dataset key so the very
     * FIRST warm query survives per-file entry loss (eviction under pressure — the observed 20-minute
     * warm re-scan) without waiting for a second successful per-file merge. Expired promises are dropped
     * on the way through; an unfulfillable promise (poisoned file, cover gap, mid-flight modification)
     * stays registered until it expires — a later scan of the same set may still complete it.
     */
    private void fulfillPendingDatasetAggregates(Map<String, Map<String, Object>> completedWholeFile) {
        List<PendingDatasetAggregate> candidates;
        synchronized (pendingDatasetAggregates) {
            if (pendingDatasetAggregates.isEmpty()) {
                return;
            }
            long now = System.nanoTime();
            pendingDatasetAggregates.values().removeIf(p -> now - p.registeredAtNanos() > PENDING_DATASET_AGGREGATE_TTL_NANOS);
            candidates = List.copyOf(pendingDatasetAggregates.values());
        }
        for (PendingDatasetAggregate pending : candidates) {
            Long sum = sumIfFullyCovered(pending, completedWholeFile);
            if (sum != null) {
                putDatasetAggregate(pending.datasetKey(), sum, pending.sourceType(), pending.location());
                synchronized (pendingDatasetAggregates) {
                    pendingDatasetAggregates.remove(pending.datasetKey());
                }
                logger.debug(
                    "materialized dataset aggregate for [{}]: row_count=[{}] across [{}] files",
                    pending.location(),
                    sum,
                    pending.pathToMtimeMillis().size()
                );
            }
        }
    }

    /**
     * Folds ONE query's stripe delta to whole-file {@code _stats.*} keys, or {@code null} when the delta
     * alone is not whole-file complete (EOF unseen, or an ordinal missing). The stateless counterpart of
     * {@link #foldCommittedStripes}: no schema-cache entry involved, so it works for files whose entries
     * were already evicted — a full scan's delta carries every stripe by itself. Cross-query assembly is
     * deliberately NOT attempted here; a partial scan simply does not fulfill the dataset promise.
     */
    @Nullable
    private static Map<String, Object> foldQueryDeltaStripes(StripeDelta delta) {
        // TRIPWIRE — coercion asymmetry vs foldCommittedStripes: the stripes folded here are the delta's
        // RAW per-stripe stats, while foldCommittedStripes folds entry-committed stripes that went through
        // coerceColumnStatsToResolvedTypes. Safe today because this fold's only consumer is the dataset
        // aggregate (sumIfFullyCovered), which reads row_count/mtime/fingerprint — never per-column
        // min/max. If GA extends the dataset aggregate to MIN/MAX, this fold must coerce like the
        // committed path (or the two folds must share the coercion) before per-column keys are served.
        return foldStripes(delta.lastStripeOrdinal(), k -> delta.stripes().get(k), delta.mtimeMillis(), delta.fingerprint());
    }

    /**
     * The one owner of the whole-file stripe fold: walks stripes {@code 0..lastOrdinal} through
     * {@code stripeAt}, requiring every ordinal present (all-or-nothing — a gap means knowledge is
     * incomplete and the fold safe-misses with {@code null}), then merges and re-keys via
     * {@link #mergeStripesAndRekey}. {@link #foldCommittedStripes} walks the entry-committed
     * {@code _stats.stripe.<k>} sub-entries; {@link #foldQueryDeltaStripes} walks one query's raw delta.
     */
    @Nullable
    private static Map<String, Object> foldStripes(
        long lastOrdinal,
        LongFunction<Map<String, Object>> stripeAt,
        long mtimeMillis,
        String fingerprint
    ) {
        if (lastOrdinal < 0) {
            return null;
        }
        List<Map<String, Object>> stripes = new ArrayList<>(Math.toIntExact(lastOrdinal) + 1);
        for (long k = 0; k <= lastOrdinal; k++) {
            Map<String, Object> stripe = stripeAt.apply(k);
            if (stripe == null) {
                return null; // ordinal missing — knowledge incomplete, safe-miss
            }
            stripes.add(stripe);
        }
        return mergeStripesAndRekey(stripes, mtimeMillis, fingerprint);
    }

    /**
     * Merges a list of flat per-stripe stats maps into one whole map and re-attaches the keying fields
     * (mtime, config fingerprint) that {@code mergeStatistics} — which rebuilds from the {@code _stats.*}
     * keys only — would drop. {@code implicitNullsForAbsentColumn=false} always: every stripe fold is
     * text-only, and under the default PROJECTED scope different cold queries harvest different columns,
     * so a column absent from a stripe means "not harvested by that scan," NOT "all-null" — the footer
     * ({@code true}) contract would fold that stripe's rows into the column's null_count and under-count
     * {@code COUNT(col)} / serve a subset MIN/MAX. {@code false} drops a column missing from any stripe
     * so it safe-misses, matching the cross-file text merge in {@code ExternalSourceResolver}.
     */
    @Nullable
    private static Map<String, Object> mergeStripesAndRekey(List<Map<String, Object>> stripes, long mtimeMillis, String fingerprint) {
        Map<String, Object> whole = stripes.size() == 1
            ? new HashMap<>(stripes.get(0))
            : SourceStatisticsSerializer.mergeStatistics(stripes, false);
        if (whole != null) {
            if (mtimeMillis >= 0) {
                whole.put(ExternalStats.MTIME_MILLIS_KEY, mtimeMillis);
            }
            if (fingerprint != null) {
                whole.put(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint);
            }
        }
        return whole;
    }

    /**
     * The promise's row-count sum, or {@code null} unless EVERY promised path is whole-file complete in
     * this reconcile under the promised (mtime, config fingerprint). All-or-nothing at the dataset
     * level on purpose: a partial sum served as the dataset count would be a wrong answer, whereas a
     * miss merely re-scans.
     */
    @Nullable
    private static Long sumIfFullyCovered(PendingDatasetAggregate pending, Map<String, Map<String, Object>> completedWholeFile) {
        long sum = 0;
        for (Map.Entry<String, Long> expected : pending.pathToMtimeMillis().entrySet()) {
            Map<String, Object> stats = completedWholeFile.get(expected.getKey());
            if (stats == null) {
                return null;
            }
            Object rowCount = stats.get(SourceStatisticsSerializer.STATS_ROW_COUNT);
            Object mtime = stats.get(ExternalStats.MTIME_MILLIS_KEY);
            if (rowCount instanceof Number == false) {
                return null;
            }
            if (mtime instanceof Number == false || ((Number) mtime).longValue() != expected.getValue()) {
                return null; // file changed between the resolve's listing and the scan — different version, safe-miss
            }
            if (Objects.equals(stats.get(ExternalStats.CONFIG_FINGERPRINT_KEY), pending.configFingerprint()) == false) {
                return null; // harvested under a different row-interpretation config — not this promise's stats
            }
            sum += ((Number) rowCount).longValue();
        }
        return sum;
    }

    /**
     * Snapshots, per contribution path, every schema-cache entry whose canonical path matches — taken
     * BEFORE a reconcile's first commit write. A cold scan longer than the schema TTL reconciles into a
     * cache whose entries for the scanned glob have ALL expired: expired entries are still forEach-visible
     * (expiry evicts lazily), but the first {@code schemaCache.put()} prunes them from the LRU tail, so
     * file #1's commit would evict files #2..N's entries before their deltas apply — the deltas match
     * nothing, the all-or-nothing multi-file fold goes incomplete, and the warm aggregate re-scans the
     * whole source.
     * <p>
     * Only ever consulted (via {@link #collectMatchingEntries}'s {@code fallback}) to recover a SIBLING's
     * swept entry, so it is worth building only for a multi-path reconcile. A single-path reconcile has no
     * sibling to evict, and its lone path's live sweep runs before any {@code put()} — seeing the same
     * pre-write state the snapshot would — so it never consults the snapshot; we skip the sweep entirely
     * there (the common single-file reconcile, where a second full-cache forEach would just double the
     * hot-path scan cost). Freshness (mtime) and config-fingerprint discrimination are NOT applied here —
     * {@link #collectMatchingEntries} re-checks both, exactly as it does for live matches.
     */
    private Map<String, List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>>> snapshotEntriesByPath(Set<String> paths) {
        if (paths.size() < 2) {
            return Map.of(); // no sibling to evict — the fallback is never consulted; skip the whole-cache sweep
        }
        // One whole-cache forEach, filtered to the contribution paths. This cannot be a set of per-path
        // get()s: SchemaCacheKey is a 7-component record (path, mtime, formatType, formatConfig, endpoint,
        // region, fileSetFingerprint), so a contribution path alone does not reconstruct a key, and forEach
        // is the only path-agnostic enumeration the Cache exposes that is safe against concurrent LRU
        // mutation (keys()/values() walk the lock-free LRU list). The sweep is O(cache) for a multi-path
        // reconcile, but that is the price of capturing each sibling's pre-eviction entry before the first
        // commit's put() prunes the expired ones.
        Map<String, List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>>> byPath = new HashMap<>();
        schemaCache.forEach((key, entry) -> {
            if (paths.contains(key.canonicalPath())) {
                byPath.computeIfAbsent(key.canonicalPath(), p -> new ArrayList<>()).add(Map.entry(key, entry));
            }
        });
        return byPath;
    }

    /**
     * Collects every schema-cache entry a contribution for {@code (path, mtimeMillis, fingerprint)}
     * applies to: every live match, plus — per KEY, for keys the live sweep no longer has — the
     * pre-reconcile {@code fallback} snapshot (see {@link #snapshotEntriesByPath}), the case where a
     * sibling path's earlier commit swept this path's expired entry out of the cache before its stats
     * could be applied. The recovery is per key, not all-or-nothing on the live sweep: the same
     * {@code (path, mtime, fingerprint)} can live under several keys (endpoint/region are key
     * components but not fingerprint inputs), and a partial sweep evicting one twin must not forfeit
     * its delta just because another twin survived. A live entry always wins over its snapshot
     * version (it may carry a concurrent commit's enrichment). A fallback entry passes the same
     * mtime + fingerprint predicate as a live one, and re-putting it re-inserts the entry with a
     * fresh write time — the same revive a live expired match already gets. Must run holding the
     * per-path {@link #stripeCommitLocks} lock; callers mutate and re-put the returned entries after
     * this method returns.
     */
    private List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>> collectMatchingEntries(
        String path,
        long mtimeMillis,
        Object fingerprint,
        @Nullable List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>> fallback
    ) {
        List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>> matches = new ArrayList<>();
        // Cache.forEach iterates each segment's HashMap under the segment's readLock, making it safe
        // against concurrent LRU mutations: promote() (called by get(), computeIfAbsent, etc. on any
        // thread) acquires only lruLock, not the segment readLock, so it cannot corrupt the forEach
        // traversal. Cache.keys() and Cache.values() walk the LRU doubly-linked list with no locks and
        // are therefore unsafe here. Do NOT call get() or put() inside the forEach consumer — the
        // segment readLock is not reentrant and put() acquires the segment writeLock.
        schemaCache.forEach((key, existing) -> {
            if (matchesContribution(key, existing, path, mtimeMillis, fingerprint)) {
                matches.add(Map.entry(key, existing));
            }
        });
        if (fallback != null) {
            Set<SchemaCacheKey> liveKeys = new HashSet<>(matches.size());
            for (Map.Entry<SchemaCacheKey, SchemaCacheEntry> match : matches) {
                liveKeys.add(match.getKey());
            }
            int recovered = 0;
            for (Map.Entry<SchemaCacheKey, SchemaCacheEntry> candidate : fallback) {
                if (liveKeys.contains(candidate.getKey()) == false
                    && matchesContribution(candidate.getKey(), candidate.getValue(), path, mtimeMillis, fingerprint)) {
                    matches.add(candidate);
                    recovered++;
                }
            }
            if (recovered > 0) {
                logger.debug("recovering [{}] cache entries for [{}] swept by a sibling commit", recovered, path);
            }
        }
        return matches;
    }

    /**
     * True when the entry is for {@code path}, observed at the contribution's mtime, under the same format
     * config. Dataset-aggregate entries ({@link SchemaCacheKey#DATASET_AGGREGATE_MARKER}) are excluded
     * explicitly: their canonicalPath is a multi-file glob pattern and their mtime is 0, so a per-file
     * contribution can never match one structurally, but a per-file enrichment landing on a dataset entry
     * would corrupt its row-count-only contract — enforce it rather than rely on the structural accident.
     * (Strict-declared per-file entries, the other reserved suffix, MUST remain matchable.)
     */
    private static boolean matchesContribution(
        SchemaCacheKey key,
        SchemaCacheEntry entry,
        String path,
        long mtimeMillis,
        Object fingerprint
    ) {
        return key.isDatasetAggregate() == false
            && path.equals(key.canonicalPath())
            && key.lastModifiedEpochMillis() == mtimeMillis
            && Objects.equals(entry.safeMetadata().get(ExternalStats.CONFIG_FINGERPRINT_KEY), fingerprint);
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
        // Shared merge+rekey tail: for a single-fragment chain toFlatMap already attached the same
        // mtime/fingerprint (foldStripeFragments enforces they agree across the chain), so the rekey
        // is an idempotent overwrite.
        return mergeStripesAndRekey(maps, mtimeMillis, fingerprint);
    }

    /**
     * Commits one query's complete stripes into every matching schema-cache entry, idempotently:
     * a stripe key re-committed by a sibling scan overwrites with identical content (the fold is a
     * pure function of the stripe's bytes), so double-counting is unrepresentable. When the
     * accumulated entry holds stripes {@code 0..K} and the EOF marker, their fold is written as the
     * whole-file {@code _stats.*} keys — the optimizer's existing warm short-circuit input —
     * making the short-circuit deterministic: complete knowledge implies enrichment, possibly
     * assembled across queries.
     *
     * @return the whole-file {@code _stats.*} fold when this commit brought (any matching entry for)
     *         the file to whole-file completeness, else {@code null}. Input to the dataset-aggregate
     *         promise fulfillment: the fold is a pure function of the file's bytes at
     *         {@code (mtime, fingerprint)}, so whichever matching entry completed first is authoritative.
     */
    @Nullable
    private Map<String, Object> commitStripeDelta(
        String path,
        StripeDelta delta,
        @Nullable List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>> fallback
    ) {
        if (enabled == false || path == null) {
            return null;
        }
        if (delta.mtimeMillis() < 0) {
            return null; // no freshness key — cannot match an entry
        }
        // Serialize the read-modify-write per file path: concurrent commits of different stripes for the
        // same file would otherwise each copy the same entry snapshot and the later put would drop the
        // earlier stripe (lost update). Commits are coordinator-side and infrequent.
        try (Releasable ignored = stripeCommitLocks.acquire(path)) {
            return applyStripeDelta(path, delta, fallback);
        }
    }

    /**
     * The locked read-modify-write of {@link #commitStripeDelta}: collect matching entries via
     * {@link #collectMatchingEntries} (live cache, snapshot fallback), then enrich and re-put each.
     * Must run holding the per-path {@link #stripeCommitLocks} lock. Returns the first completed
     * whole-file fold (see {@link #commitStripeDelta}).
     */
    @Nullable
    private Map<String, Object> applyStripeDelta(
        String path,
        StripeDelta delta,
        @Nullable List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>> fallback
    ) {
        Map<String, Object> completedFold = null;
        List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>> matchingEntries = collectMatchingEntries(
            path,
            delta.mtimeMillis(),
            delta.fingerprint(),
            fallback
        );
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
                clearStripeState(enriched);
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
                clearStripeState(enriched); // compaction: the fold subsumes the stripes; entry weight back to O(1)
                enriched.putAll(wholeFile);
                if (completedFold == null) {
                    completedFold = wholeFile;
                }
            }
            schemaCache.put(key, existing.withSafeMetadata(enriched));
        }
        return completedFold;
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
                    // A non-null, non-Number extremum on a numeric-typed column is cross-declaration garbage — e.g. a
                    // keyword min reconciled (the reconcile matches on path+mtime+config, never the declared type) onto a
                    // column another dataset over the same file declares numeric. It can never be served: buildBlock
                    // casts the extremum to (Number) and would ClassCastException. Drop it and mark the extremum
                    // unservable so the serve safe-misses (re-scans) rather than crashing. (value == null is a genuinely
                    // absent stat — nothing to do.)
                    if (value != null) {
                        if (out == null) {
                            out = new HashMap<>(statsMap);
                        }
                        out.remove(key);
                        out.put(pair[1], Boolean.TRUE);
                    }
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
        // The stripes folded here went through coerceColumnStatsToResolvedTypes when committed — see the
        // TRIPWIRE on foldQueryDeltaStripes for the coercion asymmetry between the two foldStripes callers.
        return foldStripes(lastIndex, k -> {
            if (enriched.get(ExternalStats.STRIPE_ENTRY_PREFIX + k) instanceof Map<?, ?> stripe) {
                @SuppressWarnings("unchecked")
                Map<String, Object> stripeMap = (Map<String, Object>) stripe;
                return stripeMap;
            }
            return null; // ordinal missing — knowledge incomplete, keep accumulating
        }, delta.mtimeMillis(), delta.fingerprint());
    }

    /**
     * Drops the per-stripe accumulation state ({@code _stats.stripe.<k>} sub-entries, EOF marker, grid
     * stamp) from an entry's metadata, once the stripes can no longer contribute. Two call sites:
     * <ul>
     *   <li>After {@link #foldCommittedStripes} materializes the whole-file {@code _stats.*}, as weight
     *   compaction — the stripes' only purpose was composing partial knowledge until the file was fully
     *   covered. Retaining them makes the entry's cache weight O(stripe count), and a multi-file glob of
     *   many-stripe entries overflows the schema-cache budget; the LRU then evicts already-committed
     *   sibling entries and the all-or-nothing multi-file fold forces a full warm re-scan. Compacting is
     *   safe because the file is fully known: a later scan of the same (path, mtime, fingerprint)
     *   re-emits identical stripes idempotently, and a partial re-scan folds to null but leaves the
     *   committed whole-file stats in place.</li>
     *   <li>On the grid-mismatch reset in {@link #applyStripeDelta} — stale-grid ordinals are
     *   incomparable with the delta's, so accumulation restarts on the delta's grid.</li>
     * </ul>
     */
    private static void clearStripeState(Map<String, Object> enriched) {
        enriched.keySet().removeIf(ExternalStats::isStripeBookkeeping);
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
        reconcileSourceStats(mergedStatsPerFile, snapshotEntriesByPath(mergedStatsPerFile.keySet()));
    }

    /**
     * Snapshot-aware body of {@link #reconcileSourceStats}. {@code preCommitSnapshot} is the pre-reconcile
     * per-path entry snapshot (see {@link #snapshotEntriesByPath}), consulted only when the live cache
     * has no match — the sibling-commit expiry-sweep case described there. Same discipline as
     * {@link #applyStripeDelta}.
     */
    private void reconcileSourceStats(
        Map<String, Map<String, Object>> mergedStatsPerFile,
        Map<String, List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>>> preCommitSnapshot
    ) {
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
            // several entries — one per (formatType, formatConfig) tuple (e.g. header_row=true vs
            // header_row=false count rows differently). The config fingerprint disambiguates
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
                List<Map.Entry<SchemaCacheKey, SchemaCacheEntry>> matchingEntries = collectMatchingEntries(
                    path,
                    mtimeMillis,
                    contributionFingerprint,
                    preCommitSnapshot.get(path)
                );
                for (Map.Entry<SchemaCacheKey, SchemaCacheEntry> match : matchingEntries) {
                    SchemaCacheKey key = match.getKey();
                    SchemaCacheEntry existing = match.getValue();
                    Map<String, Object> enriched = new HashMap<>(existing.safeMetadata());
                    // Push the resolved column type down before enriching. This whole-file path is
                    // last-writer-wins (no POISON fold), so an unrepresentable value (e.g. a Double past
                    // Long.MAX for a LONG-resolved column) is DROPPED rather than stored — otherwise the
                    // serve would coerce it to the resolved type and produce a wrong value.
                    enriched.putAll(coerceColumnStatsToResolvedTypes(mergedStats, existing.columnNames(), existing.columnTypes(), true));
                    schemaCache.put(key, existing.withSafeMetadata(enriched));
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
        fileMetadataCache.invalidateAll();
        listingCache.invalidateAll();
        synchronized (pendingDatasetAggregates) {
            pendingDatasetAggregates.clear();
        }
    }

    public Map<String, Object> usageStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("enabled", enabled);
        stats.put("max_total_bytes", maxTotalBytes);

        stats.put("schema_cache.count", schemaCache.count());
        stats.put("schema_cache.hits", schemaCache.stats().getHits());
        stats.put("schema_cache.misses", schemaCache.stats().getMisses());
        stats.put("schema_cache.evictions", schemaCache.stats().getEvictions());

        stats.put("file_metadata_cache.count", fileMetadataCache.count());
        stats.put("file_metadata_cache.hits", fileMetadataCache.stats().getHits());
        stats.put("file_metadata_cache.misses", fileMetadataCache.stats().getMisses());
        stats.put("file_metadata_cache.evictions", fileMetadataCache.stats().getEvictions());

        stats.put("listing_cache.count", listingCache.count());
        stats.put("listing_cache.hits", listingCache.stats().getHits());
        stats.put("listing_cache.misses", listingCache.stats().getMisses());
        stats.put("listing_cache.evictions", listingCache.stats().getEvictions());

        stats.put("dataset_aggregate.hits", datasetAggregateHits.sum());
        stats.put("dataset_aggregate.misses", datasetAggregateMisses.sum());
        synchronized (pendingDatasetAggregates) {
            stats.put("dataset_aggregate.pending", pendingDatasetAggregates.size());
        }
        stats.put("stats_aggregate.incomplete", statsAggregateIncomplete.sum());

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
    Cache<FileMetadataCacheKey, FileMetadata> fileMetadataCache() {
        return fileMetadataCache;
    }

    // Visible for testing
    Cache<ListingCacheKey, FileList> listingCache() {
        return listingCache;
    }
}
