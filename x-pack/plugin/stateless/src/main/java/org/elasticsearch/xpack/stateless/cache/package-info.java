/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * <h2>Cache-region content timestamps on search shards</h2>
 *
 * On search shards we attach a single representative content timestamp (millis) to each cache region as it is populated. The eviction
 * policies can use this timestamp to keep recent ("boosted") data resident and evict older data first, which gives better performance
 * for time-based workloads.
 *
 * <h3>Where the timestamp comes from</h3>
 *
 * The timestamp originates from {@link org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit}, which carries a range of the
 * minimum and maximum {@code @timestamp} field values across all documents in that compound commit (CC). From that range we derive a
 * single value as its midpoint. The granularity of the underlying value is therefore per CC. The value is not known for commits created
 * before timestamp ranges were introduced, and for indices that have no {@code @timestamp} field.
 *
 * <h3>Physical layout</h3>
 *
 * CCs are packed into a batched compound commit (BCC), which is stored as a single blob. A BCC may span one or more cache regions, and
 * with stateless defaults a BCC can contain multiple CCs as long as they all begin in the first region. Because a region can hold bytes
 * from more than one CC, the single timestamp we assign to a region is necessarily an approximation of the region's contents.
 *
 * <h3>Where timestamps are stored</h3>
 *
 * Per-file timestamps live in {@link org.elasticsearch.xpack.stateless.lucene.SearchDirectory} inside its current metadata, as part of
 * each file's {@link org.elasticsearch.xpack.stateless.commits.BlobFileRanges}. They are retrieved together with the file ranges while
 * iterating over all referenced CCs during recovery and on new-commit notifications. The stored granularity is per CC: for every file in
 * the directory we keep its owning CC's timestamp, exposed via
 * {@link org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory}#getTimestampMillis.
 *
 * <h3>Enabling condition</h3>
 *
 * All the behavior below applies only when the cache-boost-preference setting is enabled, which also implies that replicated ranges are
 * enabled. When it is disabled, every region is populated with
 * {@link org.elasticsearch.blobcache.shared.SharedBlobCacheService#UNKNOWN_TIMESTAMP}.
 *
 * <h3>Entry points into the cache and their granularity</h3>
 *
 * The list below documents, for each way a search node populates the cache, which timestamp is used and at what granularity. This is the
 * current state and is expected to evolve.
 * <ol>
 *     <li>
 *         BCC metadata reads ({@code readBatchedCompoundCommitUsingCache} and {@code readReferencedCompoundCommitsUsingCache} in
 *         {@link org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService}) are not handled yet: these regions are populated with
 *         {@link org.elasticsearch.blobcache.shared.SharedBlobCacheService#UNKNOWN_TIMESTAMP}. See the TODOs at the call sites in
 *         {@link org.elasticsearch.xpack.stateless.StatelessIndexEventListener} and
 *         {@link org.elasticsearch.xpack.stateless.engine.SearchEngine}. These are also the header reads for which the set-once timestamp
 *         is intended to be backfilled later.
 *     </li>
 *     <li>
 *         Offline prewarming, driven by {@link org.elasticsearch.xpack.stateless.StatelessIndexEventListener} through
 *         {@link org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService#warmBlobOffsets}, uses a single timestamp per blob,
 *         applied uniformly to every warmed region of that blob (the whole range from the start of the blob to the computed end). The
 *         per-blob value is the most recent known timestamp among the CCs referenced in that blob, so it can over-approximate the age of
 *         older regions in the blob.
 *     </li>
 *     <li>
 *         Recovery header warming, also driven by {@link org.elasticsearch.xpack.stateless.StatelessIndexEventListener} through
 *         {@link org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService}, uses a single timestamp per Lucene file, resolved
 *         via {@link org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory#getTimestampMillis} and applied to all regions
 *         covering that file. Per-file timestamps come from parsing all the CCs while retrieving the replicated ranges. When several files
 *         share a region, the first file to populate the region sets its timestamp.
 *     </li>
 *     <li>
 *         Prefetching, in {@link org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher#getPendingRangesToPrefetch}, picks a single
 *         timestamp per blob, both for the blob containing the new commit and for other referenced blobs. Files internal to the new commit
 *         are stamped with the new commit's CC timestamp; referenced files are stamped with their per-file timestamp resolved from the
 *         search directory, that is, from blobs we have seen before. The per-blob value is the most recent known timestamp across all files
 *         the commit references in that blob, not only the files whose ranges are being fetched, so a known sibling timestamp is preferred
 *         over leaving the region unknown. If every file in a prefetched blob resolves to no timestamp (for example, when the blob only
 *         references a CC the directory does not know yet), the blob falls back to the new commit's timestamp. So prefetch never stamps
 *         a region unknown while the commit itself carries a timestamp.
 *     </li>
 *     <li>
 *         On-demand search reads, served through {@link org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory} for regions that
 *         were not prewarmed or that have been evicted, use the per-CC (per Lucene file) timestamp of the file being read.
 *     </li>
 *     <li>
 *         Online warming, in {@link org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService}, warms the first (and
 *         possibly second) region of the blob(s) holding the highest-offset segment-infos (SI) files, and stamps those one or two regions
 *         with the timestamp of the CC that contains the highest-offset SI file.
 *     </li>
 *     <li>
 *         PIT relocation is planned to use the per-CC (per Lucene file) timestamp, the same as on-demand search.
 *     </li>
 * </ol>
 */
package org.elasticsearch.xpack.stateless.cache;
