/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.cache.WarmingRatioProvider;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportGetShardWarmAmountShareAction.Response;
import org.junit.After;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetShardWarmAmountShareActionTests extends ESTestCase {

    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String OTHER_TARGET = "other-target";
    private static final WarmingRatioProvider UNIT_RATIO = (range, ts, now) -> 1.0d;

    private final Index index = new Index("idx", randomUUID());
    // A real IndicesService or ClusterService needs a running node. The action only calls getShardOrNull and state().
    private final IndicesService indicesService = mock(IndicesService.class);
    private final ClusterService clusterService = mock(ClusterService.class);
    private final List<ShardId> ratioProvidersCreatedFor = new CopyOnWriteArrayList<>();
    private final Map<ShardId, SearchDirectory> directories = new HashMap<>();
    private final List<Store> stores = new ArrayList<>();
    private volatile ClusterState state;
    private final TransportGetShardWarmAmountShareAction action;

    public TransportGetShardWarmAmountShareActionTests() {
        // A real pool starts threads. The constructor only reads generic() and the clock, and computeShare is called directly.
        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(clusterService.state()).thenAnswer(invocation -> state);
        action = new TransportGetShardWarmAmountShareAction(
            threadPool,
            // The constructor registers a handler. These tests never send a request, so the transport service is not used again.
            mock(TransportService.class),
            ActionFilters.EMPTY,
            clusterService,
            indicesService,
            shardId -> {
                ratioProvidersCreatedFor.add(shardId);
                return UNIT_RATIO;
            }
        );
    }

    @After
    public void closeStores() throws Exception {
        IOUtils.close(stores);
    }

    public void testShareAmongShardsCompetingForTarget() {
        final long startedAt = randomNonNegativeLong();
        state = sourceState(startedAt, relocating(0, TARGET), started(1), relocating(2, TARGET));
        withAmounts(600L, 300L, 100L);

        assertThat(action.computeShare(request(0, startedAt)), equalTo(response(600L, 1000L)));
        assertThat(action.computeShare(request(2, startedAt)), equalTo(response(100L, 1000L)));
    }

    public void testExcludesShardsRelocatingToOtherTargets() {
        final long startedAt = randomNonNegativeLong();
        state = sourceState(startedAt, relocating(0, TARGET), relocating(1, OTHER_TARGET), started(2));
        withAmounts(600L, 300L, 100L);

        // 300 goes to another target, so 600 of 700 competes for this one
        assertThat(action.computeShare(request(0, startedAt)), equalTo(response(600L, 700L)));
    }

    public void testRequestedShardRelocatingToOtherTargetIsNoShare() {
        final long startedAt = randomNonNegativeLong();
        state = sourceState(startedAt, relocating(0, OTHER_TARGET), relocating(1, TARGET));
        withAmounts(600L, 300L);

        assertThat(action.computeShare(request(0, startedAt)), equalTo(Response.NO_SHARE));
        assertThat(action.computeShare(request(1, startedAt)), equalTo(response(300L, 300L)));
    }

    public void testSharesAcrossIndicesWithSameShardNumber() {
        final long startedAt = randomNonNegativeLong();
        final Index otherIndex = new Index("other-idx", randomUUID());
        final ShardId shard0 = new ShardId(index, 0);
        final ShardId otherShard0 = new ShardId(otherIndex, 0);
        final ShardId otherShard1 = new ShardId(otherIndex, 1);
        state = sourceState(startedAt, relocating(shard0, TARGET), relocating(otherShard0, TARGET), started(otherShard1));
        withAmount(shard0, 600L);
        withAmount(otherShard0, 300L);
        withAmount(otherShard1, 100L);

        assertThat(action.computeShare(request(shard0, startedAt)), equalTo(response(600L, 1000L)));
        assertThat(action.computeShare(request(otherShard0, startedAt)), equalTo(response(300L, 1000L)));
        assertThat(action.computeShare(request(otherShard1, startedAt)), equalTo(response(100L, 1000L)));
        assertEstimated(shard0, 1);
        assertEstimated(otherShard0, 1);
        assertEstimated(otherShard1, 1);
    }

    public void testSumIsFreshButEstimatesAreMemoized() {
        final long startedAt = randomNonNegativeLong();
        state = sourceState(startedAt, relocating(0, TARGET), relocating(1, TARGET), started(2));
        withAmounts(600L, 300L, 100L);

        assertThat(action.computeShare(request(1, startedAt)), equalTo(response(300L, 1000L)));

        // shard 0 finished relocating and left the source
        state = sourceState(startedAt, relocating(1, TARGET), started(2));
        assertThat(action.computeShare(request(1, startedAt)), equalTo(response(300L, 400L)));
        assertThat(action.computeShare(request(2, startedAt)), equalTo(response(100L, 400L)));

        for (int shard = 0; shard < 3; shard++) {
            assertEstimated(shard, 1);
        }
        assertThat(ratioProvidersCreatedFor, containsInAnyOrder(new ShardId(index, 0), new ShardId(index, 1), new ShardId(index, 2)));
    }

    public void testNewShutdownSignalReestimates() {
        final long startedAt = randomLongBetween(0, Long.MAX_VALUE - 1);
        state = sourceState(startedAt, relocating(0, TARGET), started(1));
        withAmounts(600L, 300L);
        action.computeShare(request(0, startedAt));

        final long restartedAt = startedAt + 1;
        state = sourceState(restartedAt, relocating(0, TARGET), started(1));
        assertThat(action.computeShare(request(0, restartedAt)), equalTo(response(600L, 900L)));
        assertEstimated(0, 2);
        assertEstimated(1, 2);
    }

    public void testMismatchedSignalIsAnsweredButNotMemoized() {
        final long startedAt = randomLongBetween(1, Long.MAX_VALUE);
        state = sourceState(startedAt, relocating(0, TARGET), started(1));
        withAmounts(600L, 300L);

        // the target still acts on an earlier signal of this node
        final var staleRequest = request(0, startedAt - 1);
        assertThat(action.computeShare(staleRequest), equalTo(response(600L, 900L)));
        assertThat(action.computeShare(staleRequest), equalTo(response(600L, 900L)));
        assertEstimated(0, 2);

        // nor did it reset the memo of the current signal
        action.computeShare(request(0, startedAt));
        action.computeShare(staleRequest);
        action.computeShare(request(0, startedAt));
        assertEstimated(0, 4);
    }

    public void testAnswersWithoutMemoizingWhenNotShuttingDown() {
        state = sourceState(null, relocating(0, TARGET), started(1));
        withAmounts(600L, 300L);
        final var request = request(0, randomNonNegativeLong());

        assertThat(action.computeShare(request), equalTo(response(600L, 900L)));
        action.computeShare(request);
        assertEstimated(0, 2);
    }

    public void testMissingEstimateIsNoShareAndNotMemoized() {
        final long startedAt = randomNonNegativeLong();
        state = sourceState(startedAt, relocating(0, TARGET), started(1));
        withAmounts(600L, 300L);
        final var missingShardId = new ShardId(index, 1);
        when(indicesService.getShardOrNull(missingShardId)).thenReturn(null);

        assertThat(action.computeShare(request(0, startedAt)), equalTo(Response.NO_SHARE));

        withAmount(1, 300L);
        assertThat(action.computeShare(request(0, startedAt)), equalTo(response(600L, 900L)));
        assertEstimated(1, 1);
    }

    public void testShardNotOnSourceIsNoShare() {
        final long startedAt = randomNonNegativeLong();
        state = sourceState(startedAt, started(1));
        withAmount(1, 300L);

        assertThat(action.computeShare(request(0, startedAt)), equalTo(Response.NO_SHARE));
    }

    public void testNoLocalCopiesIsNoShare() {
        final long startedAt = randomNonNegativeLong();
        state = sourceState(startedAt);

        assertThat(action.computeShare(request(0, startedAt)), equalTo(Response.NO_SHARE));
    }

    public void testZeroAmountOfInitializingCopyIsNotMemoized() {
        final long startedAt = randomNonNegativeLong();
        state = sourceState(startedAt, relocating(0, TARGET), initializing(1));
        withAmounts(600L, 0L);

        assertThat(action.computeShare(request(0, startedAt)), equalTo(response(600L, 600L)));
        action.computeShare(request(0, startedAt));
        assertEstimated(0, 1);
        assertEstimated(1, 2);
    }

    public void testZeroAmountOfStartedCopyIsMemoized() {
        final long startedAt = randomNonNegativeLong();
        state = sourceState(startedAt, relocating(0, TARGET), started(1));
        withAmounts(0L, 0L);

        assertThat(action.computeShare(request(0, startedAt)), equalTo(Response.NO_SHARE));
        action.computeShare(request(0, startedAt));
        assertEstimated(0, 1);
        assertEstimated(1, 1);
    }

    public void testEstimateSkippedWhenStoreRefNotAcquired() throws Exception {
        final ShardId shardId = new ShardId(index, 0);
        final Store store = newStore(shardId, new ByteBuffersDirectory());
        store.close();
        final IndexShard shard = mockShard(shardId, store);

        assertThat(action.tryEstimateShardWarmAmount(shard, 0L), equalTo(OptionalLong.empty()));
    }

    public void testEstimateReleasesStoreRefWhenDirectoryThrows() {
        final ShardId shardId = new ShardId(index, 0);
        final Store store = spy(newStore(shardId, new ByteBuffersDirectory()));
        doThrow(new RuntimeException("directory failed")).when(store).directory();
        final IndexShard shard = mockShard(shardId, store);
        final int refsBefore = store.refCount();

        assertThat(action.tryEstimateShardWarmAmount(shard, 0L), equalTo(OptionalLong.empty()));
        assertThat(store.refCount(), equalTo(refsBefore));
    }

    public void testEstimateWarmAmountRatioZeroIsZero() {
        BlobFileRanges first = range("blob-a", 0, 10);
        BlobFileRanges second = range("blob-a", 5, 20);
        assertThat(estimateWarmAmount(List.of(first, second), 0.0d), equalTo(0L));
    }

    public void testEstimateWarmAmountRatioOneMatchesFullPrefix() {
        BlobFileRanges first = range("blob-a", 0, 10);
        BlobFileRanges second = range("blob-a", 5, 20);
        assertThat(estimateWarmAmount(List.of(first, second), 1.0d), equalTo(25L));
        assertThat(estimateWarmAmount(List.of(), 1.0d), equalTo(0L));
    }

    public void testEstimateWarmAmountFractionalRatio() {
        BlobFileRanges file = range("blob-a", 0, 100);
        assertThat(estimateWarmAmount(List.of(file), 0.4d), equalTo(40L));
    }

    public void testEstimateWarmAmountUsesPerCommitRatio() {
        var oldCommit = new StatelessCompoundCommit.TimestampFieldValueRange(1, 2);
        var recentCommit = new StatelessCompoundCommit.TimestampFieldValueRange(10, 20);
        BlobFileRanges oldRange = range("blob-a", 0, 40, oldCommit);
        BlobFileRanges recentRange = range("blob-a", 40, 60, recentCommit);
        WarmingRatioProvider provider = (tsRange, resolved, now) -> oldCommit.equals(tsRange) ? 1.0d : 0.5d;
        assertThat(estimateWarmAmount(shuffledList(List.of(oldRange, recentRange)), provider), equalTo(70L));
    }

    public void testEstimateWarmAmountFilesOfOneCommitFormOneExtent() {
        var commit = new StatelessCompoundCommit.TimestampFieldValueRange(1, 2);
        BlobFileRanges first = range("blob-a", 0, 30, commit);
        BlobFileRanges second = range("blob-a", 50, 50, commit);
        assertThat(estimateWarmAmount(List.of(first, second), 0.5d), equalTo(50L));
    }

    public void testEstimateWarmAmountMissingRangeUsesResolverFallback() {
        BlobFileRanges noRange = range("blob-a", 0, 50);
        long fallback = 999L;
        WarmingRatioProvider provider = (tsRange, resolved, now) -> {
            assertNull(tsRange);
            assertThat(resolved, equalTo(fallback));
            return 1.0d;
        };
        assertThat(TransportGetShardWarmAmountShareAction.estimateWarmAmount(List.of(noRange), provider, tsRange -> {
            assertNull(tsRange);
            return fallback;
        }, 0L), equalTo(50L));
    }

    public void testEstimateWarmAmountSeveralBlobsAreSummed() {
        BlobFileRanges blobA = range("blob-a", 0, 10);
        BlobFileRanges blobB = range("blob-b", 3, 7);
        assertThat(estimateWarmAmount(List.of(blobA, blobB), 1.0d), equalTo(20L));
    }

    /// One estimate over two blobs.
    ///
    /// The non-time-based blob has no timestamp range, so both of its files are one commit. A 100-byte header sits before the first file
    /// and a 60-byte gap sits between the files, so the commit extent is [100, 300). The ratio applies to those 200 bytes only.
    /// The amount is the warm end offset, so the header is included: ratio 0.5 reaches byte 200, covering the first file and the gap
    /// and stopping where the second file starts. The time-based blob has its own commit and ratio, a ratio of 0 drops a commit out,
    /// and the blob amounts are summed.
    public void testEstimateWarmAmountCombinesHeaderNonTimeBasedCommitAndTimeBasedCommit() {
        final long now = 10_000L;
        final var recent = new StatelessCompoundCommit.TimestampFieldValueRange(8_000L, 10_000L);
        final var old = new StatelessCompoundCommit.TimestampFieldValueRange(1L, 2L);
        // Non-time-based: header [0, 100), file [100, 140), gap [140, 200), file [200, 300). No timestamp range.
        final BlobFileRanges plainFirst = range("plain", 100, 40);
        final BlobFileRanges plainSecond = range("plain", 200, 100);
        // Time-based: header [0, 16), file [16, 100).
        final BlobFileRanges recentFile = range("metrics", 16, 84, recent);
        final BlobFileRanges oldFile = range("old-metrics", 32, 200, old);

        final WarmingRatioProvider provider = (timestampRange, resolvedTimestamp, nowMillis) -> {
            assertThat(nowMillis, equalTo(now));
            if (timestampRange == null) {
                // A non-time-based index resolves a missing timestamp range to the unknown-timestamp sentinel.
                assertThat(resolvedTimestamp, equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP));
                return 0.5d;
            }
            return timestampRange.equals(recent) ? 1.0d : 0.0d;
        };
        assertThat(
            TransportGetShardWarmAmountShareAction.estimateWarmAmount(
                shuffledList(List.of(plainFirst, recentFile, plainSecond, oldFile)),
                provider,
                TransportGetShardWarmAmountShareActionTests::resolvedTimestamp,
                now
            ),
            // plain warms to 200, recent warms to 100, old is dropped: 300
            equalTo(300L)
        );
    }

    /// What [org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory#resolveRegionTimestampMillis] returns without a directory:
    /// no timestamp range is unknown, and a range contributes its midpoint.
    private static long resolvedTimestamp(@Nullable StatelessCompoundCommit.TimestampFieldValueRange timestampRange) {
        return timestampRange == null
            ? SharedBlobCacheService.UNKNOWN_TIMESTAMP
            : Math.max(timestampRange.midpointMillis(), SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP);
    }

    private static Response response(long shardAmount, long competingSum) {
        return new Response(shardAmount, competingSum);
    }

    private TransportGetShardWarmAmountShareAction.NodeRequest request(int shard, long shutdownSignalTimestamp) {
        return request(new ShardId(index, shard), shutdownSignalTimestamp);
    }

    private static TransportGetShardWarmAmountShareAction.NodeRequest request(ShardId shardId, long shutdownSignalTimestamp) {
        return new TransportGetShardWarmAmountShareAction.NodeRequest(TARGET, shardId, shutdownSignalTimestamp);
    }

    /// Asserts that the current commit of `shard` of [#index] was read, i.e. its amount estimated, `times` times.
    private void assertEstimated(int shard, int times) {
        assertEstimated(new ShardId(index, shard), times);
    }

    private void assertEstimated(ShardId shardId, int times) {
        verify(directories.get(shardId), times(times)).getCurrentCommitBlobFileRanges();
    }

    /// Gives shard `i` of [#index] a current commit of `amounts[i]` bytes in one blob, so that with [#UNIT_RATIO] it estimates to
    /// `amounts[i]`.
    private void withAmounts(long... amounts) {
        for (int shard = 0; shard < amounts.length; shard++) {
            withAmount(shard, amounts[shard]);
        }
    }

    private void withAmount(int shard, long amount) {
        withAmount(new ShardId(index, shard), amount);
    }

    private void withAmount(ShardId shardId, long amount) {
        // A real SearchDirectory needs a shared blob cache. The estimate only reads the current commit's blob file ranges.
        final SearchDirectory directory = mock(SearchDirectory.class);
        final String blobName = "blob-" + shardId.getIndexName() + "-" + shardId.id();
        when(directory.getCurrentCommitBlobFileRanges()).thenReturn(amount > 0 ? List.of(range(blobName, 0, amount)) : List.of());
        directories.put(shardId, directory);
        final IndexShard indexShard = mockShard(shardId, newStore(shardId, directory));
        when(indicesService.getShardOrNull(shardId)).thenReturn(indexShard);
    }

    private Store newStore(ShardId shardId, org.apache.lucene.store.Directory directory) {
        final IndexMetadata indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(indexSettings(IndexVersion.current(), shardId.getIndex().getUUID(), 1, 1))
            .build();
        final Store store = new Store(shardId, new IndexSettings(indexMetadata, Settings.EMPTY), directory, new DummyShardLock(shardId));
        stores.add(store);
        return store;
    }

    private static IndexShard mockShard(ShardId shardId, Store store) {
        // A real IndexShard needs an engine. The estimate only reads shardId() and store().
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.store()).thenReturn(store);
        return indexShard;
    }

    private ShardRouting started(int shard) {
        return started(new ShardId(index, shard));
    }

    private ShardRouting relocating(int shard, String targetNodeId) {
        return relocating(new ShardId(index, shard), targetNodeId);
    }

    private ShardRouting initializing(int shard) {
        return searchCopy(new ShardId(index, shard), INITIALIZING, null);
    }

    private static ShardRouting started(ShardId shardId) {
        return searchCopy(shardId, STARTED, null);
    }

    private static ShardRouting relocating(ShardId shardId, String targetNodeId) {
        return searchCopy(shardId, RELOCATING, targetNodeId);
    }

    private static ShardRouting searchCopy(ShardId shardId, ShardRoutingState shardState, @Nullable String relocatingNodeId) {
        return TestShardRouting.shardRoutingBuilder(shardId, SOURCE, false, shardState)
            .withRelocatingNodeId(relocatingNodeId)
            .withRole(ShardRouting.Role.SEARCH_ONLY)
            .build();
    }

    /// The state as seen by the source, with `searchCopies` on it, and its shutdown signal started at `shutdownStartedAtMillis` (none if
    /// `null`). Shards of [#index] (and of any other index of `searchCopies`) without a copy on the source have already relocated to
    /// [#TARGET].
    private ClusterState sourceState(@Nullable Long shutdownStartedAtMillis, ShardRouting... searchCopies) {
        final String indexNodeId = "index-node";
        final Map<Index, Map<Integer, ShardRouting>> copiesByIndex = new HashMap<>();
        copiesByIndex.put(index, new HashMap<>());
        for (ShardRouting searchCopy : searchCopies) {
            copiesByIndex.computeIfAbsent(searchCopy.index(), i -> new HashMap<>()).put(searchCopy.shardId().id(), searchCopy);
        }
        final ProjectMetadata.Builder project = ProjectMetadata.builder(DEFAULT_PROJECT_ID);
        final RoutingTable.Builder routingTable = RoutingTable.builder();
        for (var indexCopies : copiesByIndex.entrySet()) {
            final Index copiesIndex = indexCopies.getKey();
            final Map<Integer, ShardRouting> copiesByShard = indexCopies.getValue();
            final int numShards = 1 + copiesByShard.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
            final IndexRoutingTable.Builder routingBuilder = IndexRoutingTable.builder(copiesIndex);
            for (int shard = 0; shard < numShards; shard++) {
                final ShardId shardId = new ShardId(copiesIndex, shard);
                final ShardRouting primary = TestShardRouting.shardRoutingBuilder(shardId, indexNodeId, true, STARTED)
                    .withRole(ShardRouting.Role.INDEX_ONLY)
                    .build();
                final ShardRouting searchCopy = copiesByShard.getOrDefault(
                    shard,
                    TestShardRouting.shardRoutingBuilder(shardId, TARGET, false, STARTED).withRole(ShardRouting.Role.SEARCH_ONLY).build()
                );
                routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(searchCopy));
            }
            routingTable.add(routingBuilder);
            project.put(
                IndexMetadata.builder(copiesIndex.getName())
                    .settings(indexSettings(IndexVersion.current(), copiesIndex.getUUID(), numShards, 1))
                    .build(),
                false
            );
        }
        final Metadata.Builder metadata = Metadata.builder().put(project);
        if (shutdownStartedAtMillis != null) {
            final SingleNodeShutdownMetadata shutdown = SingleNodeShutdownMetadata.builder()
                .setNodeId(SOURCE)
                .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                .setReason(getTestName())
                .setStartedAtMillis(shutdownStartedAtMillis)
                .setNodeSeen(true)
                .build();
            metadata.putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of(SOURCE, shutdown)));
        }
        return ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create(SOURCE))
                    .localNodeId(SOURCE)
                    .masterNodeId(SOURCE)
                    .add(DiscoveryNodeUtils.create(TARGET))
                    .add(DiscoveryNodeUtils.create(OTHER_TARGET))
                    .add(DiscoveryNodeUtils.create(indexNodeId))
            )
            .metadata(metadata)
            .routingTable(GlobalRoutingTable.builder().put(DEFAULT_PROJECT_ID, routingTable.build()).build())
            .build();
    }

    private static long estimateWarmAmount(List<BlobFileRanges> ranges, double ratio) {
        return estimateWarmAmount(ranges, (tsRange, resolved, now) -> ratio);
    }

    private static long estimateWarmAmount(List<BlobFileRanges> ranges, WarmingRatioProvider ratioProvider) {
        return TransportGetShardWarmAmountShareAction.estimateWarmAmount(ranges, ratioProvider, tsRange -> 0L, 0L);
    }

    private static BlobFileRanges range(String blobName, long offset, long length) {
        return range(blobName, offset, length, null);
    }

    private static BlobFileRanges range(
        String blobName,
        long offset,
        long length,
        @Nullable StatelessCompoundCommit.TimestampFieldValueRange timestampRange
    ) {
        return new BlobFileRanges(
            new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1, -1)), offset, length),
            timestampRange
        );
    }
}
