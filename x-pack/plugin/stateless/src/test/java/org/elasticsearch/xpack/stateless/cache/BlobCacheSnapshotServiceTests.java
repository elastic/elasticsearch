/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService.RangeMissingHandler;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.StatelessThreadPools;
import org.elasticsearch.xpack.stateless.cache.BlobCacheSnapshotService.RestoreSummary;
import org.elasticsearch.xpack.stateless.cache.BlobCacheSnapshotService.SnapshotSummary;
import org.elasticsearch.xpack.stateless.cache.reader.SequentialRangeMissingHandler;
import org.elasticsearch.xpack.stateless.commits.BccBlob;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.lucene.IndexBlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobLocation;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BlobCacheSnapshotServiceTests extends ESTestCase {

    private static final long PRIMARY_TERM = 1L;
    private static final long PAGE = SharedBytes.PAGE_SIZE;
    private static final long REGION_SIZE = 4 * PAGE;

    private final Map<FileCacheKey, byte[]> blobContents = new HashMap<>();

    @Override
    public String[] tmpPaths() {
        // cache requires a single data path
        return new String[] { createTempDir().toAbsolutePath().toString() };
    }

    private FakeStatelessNode createNode() throws IOException {
        return createNode(50);
    }

    private FakeStatelessNode createNode(int cacheRegions) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), PRIMARY_TERM) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheRegions * REGION_SIZE))
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(REGION_SIZE))
                    .build();
            }
        };
    }

    private static RestoreSummary restoreSnapshot(BlobCacheSnapshotService service, String name) {
        return safeAwait(listener -> service.restore(name, listener));
    }

    private static BlobCacheSnapshotService serviceFor(FakeStatelessNode node) {
        return new BlobCacheSnapshotService(
            node.sharedCacheService,
            () -> Map.<ShardId, BlobStoreCacheDirectory>of(node.shardId, node.searchDirectory),
            (shardId, bcc) -> false,
            node.threadPool
        );
    }

    /**
     * Writes a blob of {@code regions} regions to the object store and marks it, and every blob before it, as uploaded.
     */
    private FileCacheKey writeUploadedBlob(FakeStatelessNode node, long generation, int regions) throws IOException {
        final String blobName = BccBlob.blobNameFromGeneration(generation);
        final byte[] bytes = randomByteArrayOfLength(Math.toIntExact(regions * REGION_SIZE));
        node.searchDirectory.getBlobContainer(PRIMARY_TERM)
            .writeBlob(OperationPurpose.INDICES, blobName, new ByteArrayInputStream(bytes), bytes.length, false);
        node.searchDirectory.updateLatestUploadedBcc(new PrimaryTermAndGeneration(PRIMARY_TERM, generation));
        final var key = new FileCacheKey(node.shardId, PRIMARY_TERM, blobName);
        blobContents.put(key, bytes);
        return key;
    }

    /**
     * Checks that the cache holds the same bytes as the blob for a range of a region.
     */
    private void assertCachedBytes(FakeStatelessNode node, FileCacheKey key, int region, ByteRange rangeInRegion) throws Exception {
        final byte[] blob = blobContents.get(key);
        final ByteRange range = rangeInRegion.shift(node.sharedCacheService.getRegionStart(region));
        final var cacheFile = node.sharedCacheService.getCacheFile(key, blob.length, SharedBlobCacheService.CacheMissHandler.NOOP, 0L);
        final byte[] cached = new byte[Math.toIntExact(range.length())];
        // reads the range from the cache, and fails if it has to be downloaded
        cacheFile.populateAndRead(range, range, (channel, channelPos, relativePos, length) -> {
            channel.read(ByteBuffer.wrap(cached, relativePos, length), channelPos);
            return length;
        },
            (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completionListener.onFailure(
                new IllegalStateException("range [" + range + "] of region [" + region + "] is not in the cache")
            ),
            "test"
        );
        assertArrayEquals(Arrays.copyOfRange(blob, Math.toIntExact(range.start()), Math.toIntExact(range.end())), cached);
    }

    /**
     * Fills a range of a region from the blob in the object store, as a warming task would.
     */
    private void fill(FakeStatelessNode node, FileCacheKey key, int region, ByteRange rangeInRegion) {
        fill(node, key, region, rangeInRegion, 0L);
    }

    private void fill(FakeStatelessNode node, FileCacheKey key, int region, ByteRange rangeInRegion, long timestampMillis) {
        final ByteRange range = rangeInRegion.shift(node.sharedCacheService.getRegionStart(region));
        final var reader = node.searchDirectory.getCacheBlobReaderForWarming(new BccBlob(key.fileName(), key.primaryTerm()));
        fetch(
            node,
            key,
            region,
            range,
            new SequentialRangeMissingHandler(
                this,
                key.fileName(),
                range,
                reader,
                () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE),
                bytes -> {},
                StatelessThreadPools.PREWARM_THREAD_POOL
            ),
            timestampMillis
        );
    }

    /**
     * Marks a range of a region as populated without reading the blob, which does not need to exist.
     */
    private void fillWithoutData(FakeStatelessNode node, FileCacheKey key, int region, ByteRange rangeInRegion) {
        final RangeMissingHandler writer = (
            channel,
            channelPos,
            streamFactory,
            relativePos,
            length,
            progressUpdater,
            completionListener) -> {
            progressUpdater.accept(length);
            completionListener.onResponse(null);
        };
        fetch(node, key, region, rangeInRegion.shift(node.sharedCacheService.getRegionStart(region)), writer, 0L);
    }

    private void fetch(
        FakeStatelessNode node,
        FileCacheKey key,
        int region,
        ByteRange range,
        RangeMissingHandler writer,
        long timestampMillis
    ) {
        final var future = new PlainActionFuture<Boolean>();
        node.sharedCacheService.fetchRange(
            key,
            region,
            range,
            (region + 1L) * REGION_SIZE,
            writer,
            node.threadPool.executor(StatelessThreadPools.PREWARM_THREAD_POOL),
            true,
            timestampMillis,
            true,
            future
        );
        safeGet(future);
    }

    public void testRestoreOfASnapshotThatFillsTheWholeCache() throws Exception {
        try (var node = createNode(3)) {
            final var service = serviceFor(node);
            final var key = writeUploadedBlob(node, 1L, 3);
            // two separate ranges per region, so that a restore fetches into each region more than once
            for (int region = 0; region < 3; region++) {
                fill(node, key, region, ByteRange.of(0, PAGE));
                fill(node, key, region, ByteRange.of(2 * PAGE, 3 * PAGE));
            }
            final var recorded = node.sharedCacheService.captureManifest(k -> true);
            service.snapshot("bench");
            node.sharedCacheService.forceEvict(k -> true);

            restoreSnapshot(service, "bench");

            assertThat(node.sharedCacheService.captureManifest(k -> true), equalTo(recorded));
        }
    }

    public void testRestoreBringsBackTheSnapshotState() throws Exception {
        try (var node = createNode()) {
            final var cache = node.sharedCacheService;
            final var service = serviceFor(node);
            final var key = writeUploadedBlob(node, 1L, 4);

            fill(node, key, 0, ByteRange.of(0, REGION_SIZE));
            fill(node, key, 1, ByteRange.of(0, 2 * PAGE));
            fill(node, key, 2, ByteRange.of(PAGE, 3 * PAGE));
            final var recorded = cache.captureManifest(k -> true);

            assertThat(service.snapshot("bench"), equalTo(new SnapshotSummary(1, 3, recorded.totalBytes())));

            // the cache drifts: a region is lost, a region gains data, and another region appears
            cache.forceEvict(node.shardId, (k, region) -> region == 0);
            fill(node, key, 1, ByteRange.of(2 * PAGE, REGION_SIZE));
            fill(node, key, 3, ByteRange.of(0, PAGE));

            final RestoreSummary restore = restoreSnapshot(service, "bench");

            // region 1 held more than the snapshot and region 3 is not in it, so both are evicted. Region 0 and the part of region 1
            // that was in the snapshot are downloaded again, and region 2 was already as recorded.
            assertThat(restore, equalTo(new RestoreSummary(2, 2, REGION_SIZE + 2 * PAGE, 0)));
            assertThat(cache.captureManifest(k -> true), equalTo(recorded));
            // the restored ranges hold the bytes of the blob, including ranges that do not start at the beginning of the blob
            assertCachedBytes(node, key, 0, ByteRange.of(0, REGION_SIZE));
            assertCachedBytes(node, key, 1, ByteRange.of(0, 2 * PAGE));
            assertCachedBytes(node, key, 2, ByteRange.of(PAGE, 3 * PAGE));
        }
    }

    public void testSnapshotCountsOnlyShardsWithRecordedRegions() throws Exception {
        try (var node = createNode()) {
            final var otherShard = new ShardId(node.shardId.getIndex(), node.shardId.id() + 1);
            final var service = new BlobCacheSnapshotService(
                node.sharedCacheService,
                () -> Map.<ShardId, BlobStoreCacheDirectory>of(node.shardId, node.searchDirectory, otherShard, node.searchDirectory),
                (shardId, bcc) -> false,
                node.threadPool
            );
            final var key = writeUploadedBlob(node, 1L, 1);
            fill(node, key, 0, ByteRange.of(0, PAGE));

            assertThat(service.snapshot("bench").shards(), equalTo(1));
        }
    }

    public void testSnapshotChecksUploadsOfIndexingShardsToo() throws Exception {
        try (var node = createNode()) {
            final var uploaded = new AtomicBoolean(false);
            final var indexingDirectory = IndexBlobStoreCacheDirectory.unwrapDirectory(node.indexingDirectory);
            final var service = new BlobCacheSnapshotService(
                node.sharedCacheService,
                () -> Map.<ShardId, BlobStoreCacheDirectory>of(node.shardId, indexingDirectory),
                (shardId, bcc) -> uploaded.get(),
                node.threadPool
            );
            final var key = new FileCacheKey(node.shardId, PRIMARY_TERM, BccBlob.blobNameFromGeneration(1L));
            // the cache of an indexing shard is warmed from the batched compound commit before it is uploaded
            fillWithoutData(node, key, 0, ByteRange.of(0, PAGE));

            final var e = expectThrows(IllegalStateException.class, () -> service.snapshot("bench"));
            assertThat(e.getMessage(), containsString(key.fileName()));

            uploaded.set(true);
            assertThat(service.snapshot("bench").regions(), equalTo(1));
        }
    }

    public void testRestoreReplacesARegionWithADifferentTimestamp() throws Exception {
        try (var node = createNode()) {
            final var service = serviceFor(node);
            final var key = writeUploadedBlob(node, 1L, 2);
            fill(node, key, 0, ByteRange.of(0, REGION_SIZE), 100L);
            fill(node, key, 1, ByteRange.of(0, REGION_SIZE), 100L);
            final var recorded = node.sharedCacheService.captureManifest(k -> true);
            service.snapshot("bench");

            // region 0 is filled again with the same ranges but another timestamp, which fetching into an existing region cannot change
            node.sharedCacheService.forceEvict(node.shardId, (k, region) -> region == 0);
            fill(node, key, 0, ByteRange.of(0, REGION_SIZE), 200L);

            final RestoreSummary restore = restoreSnapshot(service, "bench");

            assertThat(restore.regionsEvicted(), equalTo(1));
            assertThat(restore.rangesDownloaded(), equalTo(1));
            assertThat(node.sharedCacheService.captureManifest(k -> true), equalTo(recorded));
        }
    }

    public void testFailedRestoreReportsABoundedNumberOfFailures() throws Exception {
        try (var node = createNode()) {
            final var service = serviceFor(node);
            final var key = writeUploadedBlob(node, 1L, 3);
            // 6 ranges that all read from the same blob, which is more than a failed restore keeps
            for (int region = 0; region < 3; region++) {
                fill(node, key, region, ByteRange.of(0, PAGE));
                fill(node, key, region, ByteRange.of(2 * PAGE, 3 * PAGE));
            }
            service.snapshot("bench");
            node.sharedCacheService.forceEvict(k -> true);
            node.searchDirectory.getBlobContainer(PRIMARY_TERM)
                .deleteBlobsIgnoringIfNotExists(OperationPurpose.INDICES, List.of(key.fileName()).iterator());

            final var e = safeAwaitFailure(RestoreSummary.class, listener -> service.restore("bench", listener));

            assertThat(e.getMessage(), containsString("[6] ranges failed to download"));
            assertNotNull(e.getCause());
            assertThat("one cause and a bounded number of suppressed ones", e.getCause().getSuppressed().length, lessThanOrEqualTo(4));
        }
    }

    public void testRestoreWithoutDriftDownloadsNothing() throws Exception {
        try (var node = createNode()) {
            final var service = serviceFor(node);
            final var key = writeUploadedBlob(node, 1L, 2);
            fill(node, key, 0, ByteRange.of(0, REGION_SIZE));
            fill(node, key, 1, ByteRange.of(0, PAGE));
            final var recorded = node.sharedCacheService.captureManifest(k -> true);
            service.snapshot("bench");

            final RestoreSummary restore = restoreSnapshot(service, "bench");

            assertThat(restore, equalTo(new RestoreSummary(0, 0, 0L, 0)));
            assertThat(node.sharedCacheService.captureManifest(k -> true), equalTo(recorded));
        }
    }

    public void testASecondSnapshotWithTheSameNameReplacesTheFirst() throws Exception {
        try (var node = createNode()) {
            final var service = serviceFor(node);
            final var key = writeUploadedBlob(node, 1L, 3);
            fill(node, key, 0, ByteRange.of(0, REGION_SIZE));
            service.snapshot("bench");
            fill(node, key, 1, ByteRange.of(0, REGION_SIZE));
            final var recorded = node.sharedCacheService.captureManifest(k -> true);
            service.snapshot("bench");

            node.sharedCacheService.forceEvict(k -> true);
            restoreSnapshot(service, "bench");

            assertThat(node.sharedCacheService.captureManifest(k -> true), equalTo(recorded));
        }
    }

    public void testSnapshotFailsWhenABlobIsNotUploaded() throws Exception {
        try (var node = createNode()) {
            final var service = serviceFor(node);
            final var uploaded = writeUploadedBlob(node, 1L, 1);
            final var pending = new FileCacheKey(node.shardId, PRIMARY_TERM, BccBlob.blobNameFromGeneration(2L));
            fill(node, uploaded, 0, ByteRange.of(0, REGION_SIZE));
            fillWithoutData(node, pending, 0, ByteRange.of(0, PAGE));

            final var e = expectThrows(IllegalStateException.class, () -> service.snapshot("bench"));

            assertThat(e.getMessage(), containsString(pending.fileName()));
            // the failed snapshot recorded nothing
            final var restoreFailure = safeAwaitFailure(RestoreSummary.class, l -> service.restore("bench", l));
            assertThat(restoreFailure, instanceOf(ResourceNotFoundException.class));
        }
    }

    public void testSnapshotLeavesOutRegionsOfBlobsTheCurrentCommitDoesNotReference() throws Exception {
        try (var node = createNode()) {
            final var service = serviceFor(node);
            final var old = writeUploadedBlob(node, 1L, 1);
            final var current = writeUploadedBlob(node, 2L, 2);
            node.searchDirectory.updateCommit(
                new StatelessCompoundCommit(
                    node.shardId,
                    new PrimaryTermAndGeneration(PRIMARY_TERM, 2L),
                    1L,
                    node.node.getId(),
                    Map.of("_0.cfs", createBlobLocation(PRIMARY_TERM, 2L, 0L, REGION_SIZE)),
                    2 * REGION_SIZE,
                    Set.of("_0.cfs"),
                    0L,
                    InternalFilesReplicatedRanges.EMPTY,
                    Map.of(),
                    null
                )
            );
            fill(node, old, 0, ByteRange.of(0, REGION_SIZE));
            fill(node, current, 0, ByteRange.of(0, REGION_SIZE));
            fill(node, current, 1, ByteRange.of(0, PAGE));

            // the older blob is not referenced by the commit, so a restore could download its data but nothing would use it
            assertThat(service.snapshot("bench").regions(), equalTo(2));

            final RestoreSummary restore = restoreSnapshot(service, "bench");
            assertThat(restore.regionsEvicted(), equalTo(1));
            assertThat(node.sharedCacheService.captureManifest(key -> key.equals(old)).regionCount(), equalTo(0));
            assertThat(node.sharedCacheService.captureManifest(key -> key.equals(current)).regionCount(), equalTo(2));
        }
    }

    public void testRestoreFailsWhenABlobIsGone() throws Exception {
        try (var node = createNode()) {
            final var service = serviceFor(node);
            final var key = writeUploadedBlob(node, 1L, 2);
            fill(node, key, 0, ByteRange.of(0, REGION_SIZE));
            fill(node, key, 1, ByteRange.of(0, REGION_SIZE));
            service.snapshot("bench");

            node.sharedCacheService.forceEvict(k -> true);
            node.searchDirectory.getBlobContainer(PRIMARY_TERM)
                .deleteBlobsIgnoringIfNotExists(OperationPurpose.INDICES, List.of(key.fileName()).iterator());

            final var e = safeAwaitFailure(RestoreSummary.class, listener -> service.restore("bench", listener));

            assertThat(e, instanceOf(ElasticsearchException.class));
            assertThat(e.getMessage(), containsString("blob cache restore [bench] is incomplete"));
        }
    }

    public void testRestoreSkipsShardsThatAreNoLongerOnTheNode() throws Exception {
        try (var node = createNode()) {
            final var shards = new AtomicReference<Map<ShardId, BlobStoreCacheDirectory>>(Map.of(node.shardId, node.searchDirectory));
            final var service = new BlobCacheSnapshotService(
                node.sharedCacheService,
                shards::get,
                (shardId, bcc) -> false,
                node.threadPool
            );
            final var key = writeUploadedBlob(node, 1L, 1);
            fill(node, key, 0, ByteRange.of(0, REGION_SIZE));
            service.snapshot("bench");

            shards.set(Map.of());
            node.sharedCacheService.forceEvict(k -> true);
            final RestoreSummary restore = restoreSnapshot(service, "bench");

            assertThat(restore, equalTo(new RestoreSummary(0, 0, 0L, 1)));
            assertThat(node.sharedCacheService.captureManifest(k -> true).regionCount(), equalTo(0));
        }
    }

    public void testRestoreOfAnUnknownSnapshotFails() throws Exception {
        try (var node = createNode()) {
            final var service = serviceFor(node);

            final var e = safeAwaitFailure(RestoreSummary.class, listener -> service.restore("nope", listener));

            assertThat(e, instanceOf(ResourceNotFoundException.class));
        }
    }
}
