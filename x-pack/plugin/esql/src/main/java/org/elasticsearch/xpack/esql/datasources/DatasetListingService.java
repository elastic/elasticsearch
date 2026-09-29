/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ListingCacheKey;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.glob.ListingExtents;
import org.elasticsearch.xpack.esql.datasources.glob.ListingMemory;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
        ListingMemory memory
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
            memory
        );
    }

    /**
     * The compacted listing for a cacheable provider, from the cache or computed into it. Always the whole pattern:
     * a bounded listing is a sample of a dataset rather than the dataset, so it must never become the answer another
     * query is served, and the assertion below is what says so — the failure if that ever changes is silent.
     */
    public FileList cachedListing(
        String path,
        StoragePath storagePath,
        StorageProvider provider,
        @Nullable List<PartitionFilterHintExtractor.PartitionFilterHint> hints,
        Map<String, Object> config,
        ListingMemory memory
    ) throws Exception {
        ListingCacheKey listingKey = ListingCacheKey.build(
            storagePath.scheme(),
            storagePath.host(),
            storagePath.path(),
            ExternalSourceResolver.storageConfig(config),
            // intentional raw config: only reads partition-filter keys, not auth/connection params from _datasource
            GlobExpander.listingCacheDiscriminator(path, hints, config)
        );
        FileList listing;
        boolean[] servedFromCacheHolder = { true };
        try {
            listing = cacheService.getOrComputeListing(listingKey, k -> {
                servedFromCacheHolder[0] = false;
                return expand(path, provider, hints, config, storagePath, ListingExtents.UNBOUNDED, memory);
            });
        } catch (ExecutionException e) {
            throw asListingFailure(e);
        }
        assert listing.isTruncated() == false : "a truncated listing must never enter the shared listing cache: " + path;
        // Caps are not part of the listing key: a raise must keep hitting. A later drop still has to fail closed, or
        // a cached FileList computed under a looser cap would bypass the setting until TTL. Expand already checked;
        // this re-check is for the hit path.
        GlobExpander.checkDiscoveredFilesLimit(listing.fileCount(), maxDiscoveredFiles.getAsInt());
        // A hit allocated nothing in the walk, so nothing was reserved there; the caller still holds a reference
        // for as long as its query runs, and reserves for it here.
        if (servedFromCacheHolder[0]) {
            memory.reserve(listing.planningBytes());
        }
        return listing;
    }
}
