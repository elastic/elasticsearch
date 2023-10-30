/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * This package implements a generalized page cache that is used as a caching layer for
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} by the frozen tier.
 * <h2>High-level design</h2>
 *
 * The cache is implemented via two components. The shared cache file and its IO operations in
 * {@link org.elasticsearch.blobcache.shared.SharedBytes} and the logic for managing the mapping of its physical pages to
 * logical blob ranges in {@link org.elasticsearch.blobcache.shared.SharedBlobCacheService}.
 *
 * <h2>The SharedBytes single file cache</h2>
 *
 * All pages are cached in a single on-disk file that is accessed through a {@link org.elasticsearch.blobcache.shared.SharedBytes.IO}
 * per page that implements positional writing and reading through
 * {@link org.elasticsearch.blobcache.shared.SharedBytes.IO#write(java.nio.ByteBuffer, int)} and
 * {@link org.elasticsearch.blobcache.shared.SharedBytes.IO#read(java.nio.ByteBuffer, int)}.
 * It is important to note that the {@code SharedBytes.IO} provide no synchronization or consistency guarantees beyond what the operating
 * system's page cache provides. The only provided guarantee is that reads from a section of a page will always see the latest write to that
 * section. All other synchronization is done by the {@code SharedBlobCacheService} described below.
 *
 * <h2>The SharedBlobCacheService</h2>
 *
 * The {@code SharedBlobCacheService} manages the logical contents of pages in {@code SharedBytes}. It maintains a page table mapping
 * of blob byte-ranges to positions in the shared file described by their {@code SharedBytes.IO} instances.
 * All reads from the blob store that go through the shared blob cache service start by getting the logical page for the read
 * (and instance of {@link org.elasticsearch.blobcache.shared.SharedBlobCacheService.CacheFileRegion}) through a call
 * to {@link org.elasticsearch.blobcache.shared.SharedBlobCacheService#get}. This method will either fetch the logical page for the given
 * blob from the internal page table, or add a new page if there isn't one in the page table yet.
 * The {@code CacheFileRegion} then exposes two methods for reading from the blob store,
 * {@link org.elasticsearch.blobcache.shared.SharedBlobCacheService.CacheFileRegion#tryRead} and
 * {@link org.elasticsearch.blobcache.shared.SharedBlobCacheService.CacheFileRegion#populateAndRead}.
 * {@code tryRead} tries to read from the cache file region without any use of locking or allocations on a best effort basis.
 * If cache file region contained valid data at the start of the read and was not concurrently modified or evicted while during the read
 * then the method will return {@code true} and the read from the cache has been completed.
 * In case {@code tryRead} returns {@code false}, a page-fault occurs and the page has to be downloaded from the blobstore
 * through a call to {@code CacheFileRegion#populateAndRead}. This call will initiate downloading the page's contents from the blobstore
 * and once enough of the page to satisfy the original read has been downloaded, will return from the read call and complete the remaining
 * download in the background. Tracking of what regions of a page have been downloaded and are waiting for readers is implemented in
 * {@link org.elasticsearch.blobcache.common.SparseFileTracker}.
 * The synchronization provided by {@code SharedBlobCacheService} ensures that physical reads for the same data in the blob store are
 * deduplicated across multiple threads.
 *
 * <h3>Page eviction</h3>
 *
 * The {@code SharedBlobCacheService} manages the page eviction logic by tracking access per page and evicting according to
 * the access pattern. TODO: complete this once we have LRU caching implemented, no need to document the broken LFU approach
 */
package org.elasticsearch.blobcache;
