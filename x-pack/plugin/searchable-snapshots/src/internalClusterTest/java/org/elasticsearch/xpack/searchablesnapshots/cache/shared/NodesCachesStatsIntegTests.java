/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.shared;

import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.BaseFrozenSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction.NodeCachesStatsResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction.NodesCachesStatsResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction.NodesRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class NodesCachesStatsIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {
    private static final Logger logger = LogManager.getLogger(NodesCachesStatsIntegTests.class);

    public void testNodesCachesStats() throws Exception {
        final String[] nodeNames = internalCluster().getNodeNames();
        // here to ensure no shard relocations after the snapshot is mounted,
        // since this test verifies the cache stats on specific nodes
        ensureStableCluster(nodeNames.length);

        final String index = randomIdentifier();
        final int shardCount = between(1, 4);
        createIndex(
            index,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, shardCount).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        final int nbDocs = randomIntBetween(500, 1000);
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, client(), nbDocs, between(2, 5), false, null)) {
            if (shardCount > 1) {
                // Round-robin documents across shards so every shard holds documents.
                indexer.setDocumentIdGenerator(counter -> {
                    final int targetShard = (int) (counter % shardCount);
                    String id = counter + "";
                    int iteration = 0;
                    while (Math.floorMod(Murmur3HashFunction.hash(id), shardCount) != targetShard) {
                        id = counter + "-" + iteration++;
                    }
                    return id;
                });
            }
            indexer.start(nbDocs);
            waitForDocs(nbDocs, indexer);
        }
        refresh(index);

        final String repository = "repository";
        createRepository(repository, FsRepository.TYPE);

        final String snapshot = "snapshot";
        createFullSnapshot(repository, snapshot);

        assertAcked(indicesAdmin().prepareDelete(index));

        final String mountedIndex = "mounted-index";
        mountSnapshot(repository, snapshot, index, mountedIndex, Settings.EMPTY, Storage.SHARED_CACHE);
        ensureYellowAndNoInitializingShards(mountedIndex);

        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);

        final NodesCachesStatsResponse cachesStatsResponse = client().execute(
            TransportSearchableSnapshotsNodeCachesStatsAction.TYPE,
            new NodesRequest(Strings.EMPTY_ARRAY)
        ).actionGet();
        assertThat(cachesStatsResponse.getNodes(), hasSize(internalCluster().numDataNodes()));
        assertThat(cachesStatsResponse.hasFailures(), equalTo(false));

        for (NodeCachesStatsResponse nodeCachesStats : cachesStatsResponse.getNodes()) {
            final String nodeId = nodeCachesStats.getNode().getId();
            final String nodeName = nodeCachesStats.getNode().getName();

            final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
            final long totalFsSize = clusterAdmin().prepareNodesStats(nodeId)
                .clear()
                .setFs(true)
                .get()
                .getNodesMap()
                .get(nodeId)
                .getFs()
                .getTotal()
                .getTotal()
                .getBytes();

            final long cacheSize = SharedBlobCacheService.calculateCacheSize(clusterService.getSettings(), totalFsSize);
            assertThat(nodeCachesStats.getSize(), equalTo(cacheSize));

            final long regionSize = SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.get(clusterService.getSettings()).getBytes();

            assertThat(nodeCachesStats.getRegionSize(), equalTo(regionSize));
            assertThat(nodeCachesStats.getNumRegions(), equalTo(Math.toIntExact(cacheSize / regionSize)));
            assertThat(nodeCachesStats.getWrites(), equalTo(0L));
            assertThat(nodeCachesStats.getBytesWritten(), equalTo(0L));
            assertThat(nodeCachesStats.getReads(), equalTo(0L));
            assertThat(nodeCachesStats.getBytesRead(), equalTo(0L));
            assertThat(nodeCachesStats.getEvictions(), equalTo(0L));
        }

        for (int i = 0; i < 20; i++) {
            // The first search uses rangeQuery("id").gte(0) to guarantee a full search on every shard regardless of
            // doc distribution, ensuring SharedBlobCacheService writes are produced on every node holding a frozen shard.
            prepareSearch(mountedIndex).setQuery(
                i == 0 ? QueryBuilders.rangeQuery("id").gte(0)
                    : randomBoolean() ? QueryBuilders.rangeQuery("id").gte(randomIntBetween(0, 1000))
                    : QueryBuilders.termQuery("test", "value" + randomIntBetween(0, 1000))
            ).setSize(randomIntBetween(0, 1000)).get().decRef();
        }

        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);

        final BroadcastResponse clearCacheResponse = client().execute(
            ClearSearchableSnapshotsCacheAction.INSTANCE,
            new ClearSearchableSnapshotsCacheRequest(mountedIndex)
        ).actionGet();
        assertThat(clearCacheResponse.getSuccessfulShards(), greaterThan(0));
        assertThat(clearCacheResponse.getFailedShards(), equalTo(0));

        final String[] nodesWithFrozenShards = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable()
            .index(mountedIndex)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(ShardRouting::assignedToNode)
            .map(ShardRouting::currentNodeId)
            .collect(toSet())
            .toArray(String[]::new);

        final var response = client().execute(
            TransportSearchableSnapshotsNodeCachesStatsAction.TYPE,
            new NodesRequest(nodesWithFrozenShards)
        ).actionGet();
        assertThat(
            response.getNodes().stream().map(r -> r.getNode().getId()).collect(Collectors.toList()),
            containsInAnyOrder(nodesWithFrozenShards)
        );
        assertThat(response.hasFailures(), equalTo(false));

        for (NodeCachesStatsResponse nodeCachesStats : response.getNodes()) {
            final var nodeName = nodeCachesStats.getNode().getName();
            final var numRegions = nodeCachesStats.getNumRegions();
            final var writes = nodeCachesStats.getWrites();
            final var bytesWritten = nodeCachesStats.getBytesWritten();
            final var reads = nodeCachesStats.getReads();
            final var bytesRead = nodeCachesStats.getBytesRead();
            final var evictions = nodeCachesStats.getEvictions();
            logger.info(
                "nodeCachesStats for node [{}]: numRegions [{}], writes [{}], "
                    + "bytes written [{}], reads [{}], bytes read [{}], evictions [{}]",
                nodeName,
                numRegions,
                writes,
                bytesWritten,
                reads,
                bytesRead,
                evictions
            );
        }

        for (NodeCachesStatsResponse nodeCachesStats : response.getNodes()) {
            if (nodeCachesStats.getNumRegions() > 0) {
                assertThat(nodeCachesStats.getWrites(), greaterThan(0L));
                assertThat(nodeCachesStats.getBytesWritten(), greaterThan(0L));
                assertThat(nodeCachesStats.getReads(), greaterThan(0L));
                assertThat(nodeCachesStats.getBytesRead(), greaterThan(0L));
                assertThat(nodeCachesStats.getEvictions(), greaterThan(0L));
            } else {
                assertThat(nodeCachesStats.getWrites(), equalTo(0L));
                assertThat(nodeCachesStats.getBytesWritten(), equalTo(0L));
                assertThat(nodeCachesStats.getReads(), equalTo(0L));
                assertThat(nodeCachesStats.getBytesRead(), equalTo(0L));
                assertThat(nodeCachesStats.getEvictions(), equalTo(0L));
            }
        }
    }

    /// Demonstrates how the `writes > 0` assertion in testNodesCachesStats can fail.
    /// For a query with no matches, only readWithBlobCache will be called (the BKD traversal short-circuits?).
    /// But when the query matches something, the search uses readWithoutBlobCache (to read leaf data), which uses
    /// the SharedBlobCacheService and increments the write count.
    ///
    /// To make this deterministic we control routing and queries explicitly.
    /// Documents with id-field values `1..splitPoint` are routed to shard 0 and the rest to shard 1.
    /// We only do searches that match doc ranges on shard 1.
    /// TODO: remove this test before merging PR # 147708
    public void testNodesCachesStatsZeroWritesRepro() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        ensureStableCluster(internalCluster().getNodeNames().length);

        final int numShards = 2;
        final String index = randomIdentifier();
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                // In EsIntegTestCase: between(minimumNumberOfShards(), maximumNumberOfShards())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .build()
        );

        // Route docs with id-field values [1, splitPoint] to shard 0, the rest to shard 1.
        // Shard formula is: Math.floorMod(Murmur3HashFunction.hash(_id), numShards).
        // We iterate candidate _id strings until we find one that hashes to the target shard.
        final int nbDocs = 500;
        final int splitPoint = nbDocs / 2;
        final Function<Long, String> docIdGenerator = counter -> {
            final int targetShard = counter <= splitPoint ? 0 : 1;
            for (long suffix = 0;; suffix++) {
                final String candidate = counter + (suffix == 0L ? "" : "-" + suffix);
                if (Math.floorMod(Murmur3HashFunction.hash(candidate), numShards) == targetShard) {
                    return candidate;
                }
            }
        };

        try (BackgroundIndexer indexer = new BackgroundIndexer(index, client(), nbDocs, 1, false, null)) {
            indexer.setDocumentIdGenerator(docIdGenerator);
            indexer.start(nbDocs);
            waitForDocs(nbDocs, indexer);
        }
        refresh(index);

        final String repository = "repository-pruned";
        createRepository(repository, FsRepository.TYPE);

        final String snapshot = "snapshot-pruned";
        createFullSnapshot(repository, snapshot);

        assertAcked(indicesAdmin().prepareDelete(index));

        final String mountedIndex = "mounted-index-pruned";
        mountSnapshot(repository, snapshot, index, mountedIndex, Settings.EMPTY, Storage.SHARED_CACHE);
        ensureYellowAndNoInitializingShards(mountedIndex);

        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);

        // Identify which node holds shard 0 and which holds shard 1.
        final Map<Integer, String> shardToNodeId = new HashMap<>();
        for (final ShardRouting sr : clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable()
            .index(mountedIndex)
            .shardsWithState(ShardRoutingState.STARTED)) {
            if (sr.assignedToNode()) {
                shardToNodeId.put(sr.shardId().id(), sr.currentNodeId());
            }
        }
        final String shard0NodeId = shardToNodeId.get(0);
        final String shard1NodeId = shardToNodeId.get(1);
        assumeTrue("both shards must be on different nodes", shard0NodeId.equals(shard1NodeId) == false);

        // Every search queries id >= splitPoint + 1.
        for (int i = 0; i < 20; i++) {
            logger.info(" ---> search query # {}", i);
            prepareSearch(mountedIndex).setQuery(
                // randomIntBetween(0, 1000) in original test
                QueryBuilders.rangeQuery("id").gte(randomIntBetween(splitPoint + 1, 1000))
            ).setSize(randomIntBetween(0, 1000)).get().decRef();
        }

        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);

        // Logic from original test
        final BroadcastResponse clearCacheResponse = client().execute(
            ClearSearchableSnapshotsCacheAction.INSTANCE,
            new ClearSearchableSnapshotsCacheRequest(mountedIndex)
        ).actionGet();
        assertThat(clearCacheResponse.getSuccessfulShards(), greaterThan(0));
        assertThat(clearCacheResponse.getFailedShards(), equalTo(0));

        final String[] nodesFrozenShards = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable()
            .index(mountedIndex)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(ShardRouting::assignedToNode)
            .map(ShardRouting::currentNodeId)
            .collect(toSet())
            .toArray(String[]::new);

        final NodesCachesStatsResponse response = client().execute(
            TransportSearchableSnapshotsNodeCachesStatsAction.TYPE,
            new NodesRequest(nodesFrozenShards)
        ).actionGet();
        assertThat(
            response.getNodes().stream().map(r -> r.getNode().getId()).collect(Collectors.toList()),
            containsInAnyOrder(nodesFrozenShards)
        );

        assertThat(response.hasFailures(), equalTo(false));
        for (NodeCachesStatsResponse nodeCachesStats : response.getNodes()) {
            final var nodeName = nodeCachesStats.getNode().getName();
            final var numRegions = nodeCachesStats.getNumRegions();
            final var writes = nodeCachesStats.getWrites();
            final var bytesWritten = nodeCachesStats.getBytesWritten();
            final var reads = nodeCachesStats.getReads();
            final var bytesRead = nodeCachesStats.getBytesRead();
            final var evictions = nodeCachesStats.getEvictions();
            logger.info(
                "nodeCachesStats for node [{}]: numRegions [{}], writes [{}], "
                    + "bytes written [{}], reads [{}], bytes read [{}], evictions [{}]",
                nodeName,
                numRegions,
                writes,
                bytesWritten,
                reads,
                bytesRead,
                evictions
            );
        }

        for (NodeCachesStatsResponse nodeCachesStats : response.getNodes()) {
            if (nodeCachesStats.getNumRegions() > 0) {
                assertThat(nodeCachesStats.getWrites(), greaterThan(0L));
                assertThat(nodeCachesStats.getBytesWritten(), greaterThan(0L));
                assertThat(nodeCachesStats.getReads(), greaterThan(0L));
                assertThat(nodeCachesStats.getBytesRead(), greaterThan(0L));
                assertThat(nodeCachesStats.getEvictions(), greaterThan(0L));
            } else {
                assertThat(nodeCachesStats.getWrites(), equalTo(0L));
                assertThat(nodeCachesStats.getBytesWritten(), equalTo(0L));
                assertThat(nodeCachesStats.getReads(), equalTo(0L));
                assertThat(nodeCachesStats.getBytesRead(), equalTo(0L));
                assertThat(nodeCachesStats.getEvictions(), equalTo(0L));
            }
        }
    }
}
