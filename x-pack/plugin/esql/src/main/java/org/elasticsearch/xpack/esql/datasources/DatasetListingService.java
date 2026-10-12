/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ListingCacheKey;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.glob.ListingExtents;
import org.elasticsearch.xpack.esql.datasources.glob.PlanningMemory;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * Lists a dataset's objects: the node's live listing caps, and the shared listing cache in front of them.
 * <p>
 * Two callers list the same datasets. Resolution lists for the schema, and under a mode whose schema comes from one
 * file that listing is a prefix; split discovery then lists the query's own file set. Both owe the same two things —
 * the caps as the cluster currently has them rather than as this node started with them, and a cache hit when another
 * query has already listed the same pattern — and before this existed only the first of them paid either.
 * <p>
 * The cache-key build and the compute that fills it are kept in one method on purpose: the discriminator folded into
 * the key must describe exactly the {@code (path, hints)} the compute expands, or a filtered query's narrowed listing
 * is served to a later unfiltered one. That pairing is the reason this is a type rather than two static helpers.
 * <p>
 * More than one of these may exist on a node. They are adapters over shared state — one
 * {@link ExternalSourceCacheService}, one set of watched cap values — so two instances hit each other's entries and
 * read each other's updates; what must not be duplicated is the key-and-compute pairing above, and that lives here.
 */
public final class DatasetListingService {

    @Nullable
    private final ExternalSourceCacheService cacheService;
    private final IntSupplier maxDiscoveredFiles;
    private final IntSupplier maxGlobExpansion;
    private final IntSupplier maxListedObjects;

    /**
     * @param cacheService the shared listing cache, or {@code null} to list live every time.
     * @param maxDiscoveredFiles live {@link ExternalSourceSettings#MAX_DISCOVERED_FILES} cap; {@code null} reads the
     *        node's own settings, which is what a test with no cluster state wants.
     * @param maxGlobExpansion live {@link ExternalSourceSettings#MAX_GLOB_EXPANSION} cap, same fallback.
     * @param maxListedObjects live {@link ExternalSourceSettings#MAX_LISTED_OBJECTS} cap, same fallback.
     */
    public DatasetListingService(
        Settings settings,
        @Nullable ExternalSourceCacheService cacheService,
        @Nullable IntSupplier maxDiscoveredFiles,
        @Nullable IntSupplier maxGlobExpansion,
        @Nullable IntSupplier maxListedObjects
    ) {
        Settings effective = settings != null ? settings : Settings.EMPTY;
        this.cacheService = cacheService;
        this.maxDiscoveredFiles = capOrSettings(maxDiscoveredFiles, ExternalSourceSettings.MAX_DISCOVERED_FILES, effective);
        this.maxGlobExpansion = capOrSettings(maxGlobExpansion, ExternalSourceSettings.MAX_GLOB_EXPANSION, effective);
        this.maxListedObjects = capOrSettings(maxListedObjects, ExternalSourceSettings.MAX_LISTED_OBJECTS, effective);
    }

    private static IntSupplier capOrSettings(@Nullable IntSupplier supplied, Setting<Integer> setting, Settings settings) {
        return supplied != null ? supplied : () -> setting.get(settings);
    }

    @Nullable
    public ExternalSourceCacheService cacheService() {
        return cacheService;
    }

    public int maxDiscoveredFiles() {
        return maxDiscoveredFiles.getAsInt();
    }

    public int maxGlobExpansion() {
        return maxGlobExpansion.getAsInt();
    }

    public int maxListedObjects() {
        return maxListedObjects.getAsInt();
    }

    /**
     * Whether a listing over this provider may be cached. Providers with no stable metadata (HTTP) are excluded:
     * mtime-based invalidation cannot be trusted for them.
     */
    public boolean isCacheable(StorageProvider provider) {
        return cacheService != null && cacheService.isEnabled() && provider.supportsStableMetadata();
    }

    /**
     * The failure the listing itself produced, rather than the cache's report of it.
     * <p>
     * {@code Cache#computeIfAbsent} wraps whatever the loader threw in an {@link ExecutionException}, which is a
     * checked exception no caller of a listing expects and which carries no status of its own. Handed on as-is it
     * answers 500: a cap the listing exceeded throws {@link IllegalArgumentException} with the text telling the user
     * which setting to raise, and that is a 400 the client can act on. Unwrapping here rather than at each caller is
     * what keeps a cacheable dataset and a non-cacheable one answering the same way.
     */
    private static Exception asListingFailure(ExecutionException wrapper) {
        Throwable cause = wrapper.getCause();
        if (cause instanceof Exception cachedFailure) {
            return cachedFailure;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return wrapper;
    }

    /** One live listing under the current caps. */
    public FileList expand(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        Map<String, Object> config,
        StoragePath storagePath,
        ListingExtents extents,
        PlanningMemory memory,
        BooleanSupplier cancelled
    ) throws Exception {
        return GlobExpander.expandAndCompact(
            path,
            provider,
            hints,
            config,
            storagePath,
            maxDiscoveredFiles.getAsInt(),
            maxGlobExpansion.getAsInt(),
            maxListedObjects.getAsInt(),
            extents,
            memory,
            cancelled
        );
    }

    /**
     * Async counterpart of {@link #expand}: a wide glob is listed as concurrent per-prefix lists on {@code executor}
     * rather than on the calling thread, which neither blocks nor waits on a pool that still has to run the lists.
     * {@code listener} fires on whichever thread finishes last.
     *
     * @param concurrency how many per-prefix lists may run at once; at most one disables the fan-out
     */
    public void expandAsync(
        String path,
        StorageProvider provider,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        Map<String, Object> config,
        StoragePath storagePath,
        ListingExtents extents,
        PlanningMemory memory,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor,
        ActionListener<FileList> listener
    ) {
        GlobExpander.expandAndCompactAsync(
            path,
            provider,
            hints,
            config,
            storagePath,
            maxDiscoveredFiles.getAsInt(),
            maxGlobExpansion.getAsInt(),
            maxListedObjects.getAsInt(),
            extents,
            memory,
            concurrency,
            cancelled,
            executor,
            listener
        );
    }

    /**
     * The shared listing-cache address. Built here rather than at each caller because the two identities are the
     * listing provider's own report of which of its settings name the same store twice and which are secret, and
     * re-deriving them would take a fresh lease and decrypt again.
     */
    private static ListingCacheKey listingCacheKey(
        String path,
        StoragePath storagePath,
        String storageIdentity,
        String secretIdentity,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        Map<String, Object> config
    ) {
        return ListingCacheKey.build(
            storagePath.scheme(),
            storagePath.host(),
            storagePath.path(),
            storageIdentity,
            secretIdentity,
            ExternalSourceResolver.storageConfig(config),
            // intentional raw config: only reads partition-filter keys, not auth/connection params from _datasource
            GlobExpander.listingCacheDiscriminator(path, hints, config)
        );
    }

    /**
     * A bounded listing, served from the shared cache when it holds a complete one and offered back to it when this
     * one turns out complete too.
     * <p>
     * A bound and the cache used to be mutually exclusive: {@code boundsFileSet()} picked the uncached rail outright,
     * so a bounded listing was recomputed on every execution however complete it was. That was sound while the bound
     * counted keys visited, because such a listing was a prefix of the dataset whenever the prefix held more keys
     * than the bound — which, for a selective glob, was always. Now that it counts matching files, a bounded listing
     * over a glob with fewer matches than the bound drains its whole prefix and is complete, and a complete listing
     * is the same artifact whoever asked for it. {@link ExternalSourceCacheService#putListingIfComplete} is what
     * keeps an incomplete one out.
     * <p>
     * An entry is served only when it holds no more files than the bound, which is a narrower rule than "the entry
     * is complete" and the difference is not about correctness. A listing carries the partition columns and types
     * {@code detectPartitions} folded out of the paths it held, so an entry listing more files than this bound was
     * typed over more paths than this query's own listing would have been. Both answers are defensible — the wider
     * one is strictly better informed — but they can differ, and then the schema a query reports depends on whether
     * some unrelated earlier query happened to warm the cache. Declining the wider entry costs a listing in a case
     * the bound was never helping anyway (the bound only pays off where the matches are few) and buys back an answer
     * that does not depend on traffic. It also keeps the planning reservation symmetric: this rail reserves what it
     * was served, so serving a wider listing warm than the bound permits cold would let a query trip the breaker on
     * a warm node and not on a cold one — the inverse of the rule {@link #cachedListing} reserves by.
     */
    /**
     * Synchronous counterpart of {@link #boundedListingAsync}, with the same serve rule and the same reservation.
     * For split discovery's first attempt, which lists on the calling thread.
     */
    public FileList boundedListing(
        String path,
        StoragePath storagePath,
        StorageProvider provider,
        String storageIdentity,
        String secretIdentity,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        Map<String, Object> config,
        ListingExtents extents,
        PlanningMemory memory,
        BooleanSupplier cancelled
    ) throws Exception {
        ListingCacheKey listingKey = listingCacheKey(path, storagePath, storageIdentity, secretIdentity, hints, config);
        FileList cached = cacheService.getListingIfPresent(listingKey);
        if (cached != null && cached.fileCount() <= extents.maxFiles()) {
            GlobExpander.checkDiscoveredFilesLimit(cached.fileCount(), maxDiscoveredFiles.getAsInt());
            memory.reserve(cached.fileCount() * FileList.LISTING_BYTES_PER_ENTRY);
            return cached;
        }
        FileList listing = expand(path, provider, hints, config, storagePath, extents, memory, cancelled);
        cacheService.putListingIfComplete(listingKey, listing);
        return listing;
    }

    public void boundedListingAsync(
        String path,
        StoragePath storagePath,
        StorageProvider provider,
        String storageIdentity,
        String secretIdentity,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        Map<String, Object> config,
        ListingExtents extents,
        PlanningMemory memory,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor,
        ActionListener<FileList> listener
    ) {
        ListingCacheKey listingKey = listingCacheKey(path, storagePath, storageIdentity, secretIdentity, hints, config);
        FileList cached = cacheService.getListingIfPresent(listingKey);
        if (cached != null && cached.fileCount() <= extents.maxFiles()) {
            try {
                // Caps are not part of the listing key; see cachedListing. A hit allocated nothing in the walk, so
                // it reserves what the walk would have, for the reasons cachedListing gives at length.
                GlobExpander.checkDiscoveredFilesLimit(cached.fileCount(), maxDiscoveredFiles.getAsInt());
                memory.reserve(cached.fileCount() * FileList.LISTING_BYTES_PER_ENTRY);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(cached);
            return;
        }
        expandAsync(
            path,
            provider,
            hints,
            config,
            storagePath,
            extents,
            memory,
            cancelled,
            concurrency,
            executor,
            listener.delegateFailureAndWrap((l, listing) -> {
                cacheService.putListingIfComplete(listingKey, listing);
                l.onResponse(listing);
            })
        );
    }

    /**
     * Async counterpart of {@link #cachedListing}, with the same key-and-compute pairing and the same reservation
     * rule. Concurrent cold misses for one key share one listing: the first query's listing is the one that runs, and
     * the others wait on it rather than each starting a fan-out. A query that waited allocated nothing in the walk, so
     * like a cache hit it reserves the listing's size here; a leader that was cancelled fails only itself, and the
     * others list again.
     */
    public void cachedListingAsync(
        String path,
        StoragePath storagePath,
        StorageProvider provider,
        String storageIdentity,
        String secretIdentity,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        Map<String, Object> config,
        PlanningMemory memory,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor,
        ActionListener<FileList> listener
    ) {
        ListingCacheKey listingKey = listingCacheKey(path, storagePath, storageIdentity, secretIdentity, hints, config);
        // Set by whichever thread runs the compute and read by whichever completes the listing.
        AtomicBoolean computedHere = new AtomicBoolean();
        cacheService.getOrComputeListingAsync(listingKey, innerListener -> {
            computedHere.set(true);
            expandAsync(
                path,
                provider,
                hints,
                config,
                storagePath,
                ListingExtents.UNBOUNDED,
                memory,
                cancelled,
                concurrency,
                executor,
                innerListener
            );
        }, ActionListener.wrap(listing -> {
            try {
                // Caps are not part of the listing key; see cachedListing.
                GlobExpander.checkDiscoveredFilesLimit(listing.fileCount(), maxDiscoveredFiles.getAsInt());
                if (computedHere.get() == false) {
                    memory.reserve(listing.fileCount() * FileList.LISTING_BYTES_PER_ENTRY);
                }
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(listing);
        }, listener::onFailure));
    }

    /**
     * The compacted listing for a cacheable provider, from the cache or computed into it. Always the whole pattern:
     * a bounded listing is a sample of a dataset rather than the dataset, so it must never become the answer another
     * query is served, and the refusal below is what holds that.
     * <p>
     * {@code memory} reserves the entries either way, once each, and the same figure either way: a miss reserves
     * through the walk as it grows, a hit reserves the equivalent here.
     */
    public FileList cachedListing(
        String path,
        StoragePath storagePath,
        StorageProvider provider,
        String storageIdentity,
        String secretIdentity,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        Map<String, Object> config,
        PlanningMemory memory,
        BooleanSupplier cancelled
    ) throws Exception {
        // The two identities are passed in rather than derived here: only the provider that would list this prefix
        // knows which of its settings name the same store twice and which of them are declared secret, and the
        // resolver already holds what it reported. Re-deriving would take a fresh lease and decrypt again.
        ListingCacheKey listingKey = listingCacheKey(path, storagePath, storageIdentity, secretIdentity, hints, config);
        FileList listing;
        boolean[] servedFromCacheHolder = { true };
        try {
            listing = cacheService.getOrComputeListing(listingKey, k -> {
                servedFromCacheHolder[0] = false;
                return expand(path, provider, hints, config, storagePath, ListingExtents.UNBOUNDED, memory, cancelled);
            });
        } catch (ExecutionException e) {
            throw asListingFailure(e);
        }
        // Caps are not part of the listing key: a raise must keep hitting. A later drop still has to fail closed, or
        // a cached FileList computed under a looser cap would bypass the setting until TTL. Expand already checked;
        // this re-check is for the hit path.
        GlobExpander.checkDiscoveredFilesLimit(listing.fileCount(), maxDiscoveredFiles.getAsInt());
        // A hit allocated nothing in the walk, so nothing was reserved there, and the caller holds a reference for as
        // long as its query runs. It reserves the same figure the walk would have - one allowance per entry - rather
        // than the compacted list's own size, for two reasons. The resolver's own top-up subtracts exactly that
        // figure to find what is left to charge (ExternalSourceResolver.chargeListingPlanning), so a hit reserving
        // less than the walk made that subtraction remove bytes nobody had reserved. And it is the same listing either
        // way: a query that trips the breaker on a cold node has to trip on a warm one, or the limit depends on who
        // ran first. Over-reserved against the compacted list, deliberately and in the safe direction.
        if (servedFromCacheHolder[0]) {
            memory.reserve(listing.fileCount() * FileList.LISTING_BYTES_PER_ENTRY);
        }
        return listing;
    }
}
