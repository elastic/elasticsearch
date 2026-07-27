/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/// ## Cache-region content timestamps on search shards
/// On search shards we attach a single representative content timestamp (millis) to each cache region as it is populated. The eviction
/// policies can use this timestamp to keep recent ("pinned") data resident and evict older data first, which gives better performance
/// for time-based workloads.
///
/// ### Where the timestamp comes from
/// The timestamp originates from [org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit], which carries a range of the
/// minimum and maximum `@timestamp` field values across all documents in that compound commit (CC). From that range we derive a
/// single value as its midpoint. The granularity of the underlying value is therefore per CC. The value is not known for commits created
/// before timestamp ranges were introduced, and for indices that have no `@timestamp` field.
///
/// ### Physical layout
/// CCs are packed into a batched compound commit (BCC), which is stored as a single blob. A BCC may span one or more cache regions, and
/// with stateless defaults a BCC can contain multiple CCs as long as they all begin in the first region. Because a region can hold bytes
/// from more than one CC, the single timestamp we assign to a region is necessarily an approximation of the region's contents.
///
/// ### Where timestamps are stored
/// Timestamps live in [org.elasticsearch.xpack.stateless.lucene.SearchDirectory] inside its current metadata, as part of
/// each file's [org.elasticsearch.xpack.stateless.commits.BlobFileRanges]. They are retrieved together with the file ranges while
/// iterating over all referenced CCs during recovery and on new-commit notifications. The stored granularity is per CC: for every file in
/// the directory we keep its owning CC's timestamp, exposed via
/// [org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory#getTimestampMillis].
///
/// Generational files are special as a generational file can appear in multiple BCCs. Search directory pins each generational
/// file to the first blob location it sees and keeps the timestamp of the CC that wrote it to that location.
/// See [org.elasticsearch.xpack.stateless.lucene.SearchDirectory#mergeMetadata] for exact details.
///
/// ### Entry points into the cache and their granularity
/// The list below documents, for each way a search node populates the cache, which timestamp is used and at what granularity. This is the
/// current state and is expected to evolve.
///
///   - BCC metadata reads (`readBatchedCompoundCommitUsingCache` and `readReferencedCompoundCommitsUsingCache` in
///     [org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService]) are not handled yet: these regions should be populated with
///     [org.elasticsearch.blobcache.shared.SharedBlobCacheService#BACKFILL_IN_PROGRESS_TIMESTAMP] and backfilled to a resolved value
///     after parsing. See the TODOs at the call sites in [org.elasticsearch.xpack.stateless.StatelessIndexEventListener] and
///     [org.elasticsearch.xpack.stateless.engine.SearchEngine].
///
///   - Offline prewarming, driven by [org.elasticsearch.xpack.stateless.StatelessIndexEventListener] through
///     [org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService#warmBlobOffsets], uses a single timestamp per blob,
///     applied uniformly to every warmed region of that blob (the whole range from the start of the blob to the computed end). The
///     per-blob value is the most recent known timestamp among the CCs referenced in that blob, so it can over-approximate the age of
///     older regions in the blob.
///
///   - Recovery header warming, also driven by [org.elasticsearch.xpack.stateless.StatelessIndexEventListener] through
///     [org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService], resolves a single per-CC timestamp for each
///     Lucene file being fetched and applies to all regions covering that file. When several files share a region, the first file to
///     populate the region sets its timestamp.
///
///   - Prefetching, in [org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher#computeTimestampPerBlob], picks a single
///     timestamp per blob for the blobs being prefetched, both for the blob containing the new commit and for other referenced blobs.
///     Blob containing the new commit gets the timestamp of the new commit, and other referenced blobs get the most recent known
///     timestamp among the CCs referenced in each blob.
///
///   - On-demand search reads, served through [org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory] for regions that
///     were not prewarmed or that have been evicted, use the per-CC timestamp of the file being read.
///
///   - Online warming, in [org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService], warms the first (and
///     possibly second) region of the blob(s) holding the highest-offset segment-infos (SI) files, and stamps those one or two regions
///     with the timestamp of the CC that contains the highest-offset SI file.
///
///   - PIT relocation is planned to use the per-CC timestamp, the same as on-demand search.
///
package org.elasticsearch.xpack.stateless.cache;
