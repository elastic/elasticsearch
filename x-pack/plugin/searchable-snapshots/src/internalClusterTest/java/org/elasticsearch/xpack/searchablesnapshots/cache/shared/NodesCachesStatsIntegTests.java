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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.search.aggregations.AggregationBuilders;
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

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class NodesCachesStatsIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    public void testNodesCachesStats() throws Exception {
        final String[] nodeNames = internalCluster().getNodeNames();
        // here to ensure no shard relocations after the snapshot is mounted,
        // since this test verifies the cache stats on specific nodes
        ensureStableCluster(nodeNames.length);

        final String index = randomIdentifier();
        createIndex(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());

        final int nbDocs = randomIntBetween(1_000, 10_000);
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, client(), nbDocs)) {
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
        }

        // Make sure we have at least one query that matches docs on all shards.
        prepareSearch(mountedIndex).addAggregation(
            AggregationBuilders.global("all").subAggregation(AggregationBuilders.histogram("by_id").field("id").interval(100))
        ).setSize(0).get().decRef();

        for (int i = 0; i < 20; i++) {
            prepareSearch(mountedIndex).setQuery(
                randomBoolean()
                    ? QueryBuilders.rangeQuery("id").gte(randomIntBetween(0, 1000))
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
        assertThat(response.getNodes().stream().map(r -> r.getNode().getId()).toList(), containsInAnyOrder(nodesWithFrozenShards));
        assertThat(response.hasFailures(), equalTo(false));

        // include all nodes' stats in the assertion message so that any failure
        // provides enough context to debug edge cases not captured in #129863
        final var allNodesStats = String.join(
            ",",
            response.getNodes()
                .stream()
                .map(
                    nodeStats -> format(
                        "[node=[%s], numRegions=[%d], writes=[%d], bytesWritten=[%d], reads=[%d], bytesRead=[%d], evictions=[%d]]",
                        nodeStats.getNode().getName(),
                        nodeStats.getNumRegions(),
                        nodeStats.getWrites(),
                        nodeStats.getBytesWritten(),
                        nodeStats.getReads(),
                        nodeStats.getBytesRead(),
                        nodeStats.getEvictions()
                    )
                )
                .toList()
        );
        response.getNodes().forEach(nodeStats -> {
            final var nodeName = nodeStats.getNode().getName();
            assertThat(
                format("unexpected write stats for node [%s], all stats: [%s]", nodeName, allNodesStats),
                nodeStats.getWrites(),
                nodeStats.getNumRegions() > 0 ? greaterThan(0L) : equalTo(0L)
            );
            assertThat(
                format("unexpected bytes written stats for node [%s], all stats: [%s]", nodeName, allNodesStats),
                nodeStats.getBytesWritten(),
                nodeStats.getNumRegions() > 0 ? greaterThan(0L) : equalTo(0L)
            );
            assertThat(
                format("unexpected read stats for node [%s], all stats: [%s]", nodeName, allNodesStats),
                nodeStats.getReads(),
                nodeStats.getNumRegions() > 0 ? greaterThan(0L) : equalTo(0L)
            );
            assertThat(
                format("unexpected bytes read stats for node [%s], all stats: [%s]", nodeName, allNodesStats),
                nodeStats.getBytesRead(),
                nodeStats.getNumRegions() > 0 ? greaterThan(0L) : equalTo(0L)
            );
            assertThat(
                format("unexpected evictions stats for node [%s], all stats: [%s]", nodeName, allNodesStats),
                nodeStats.getEvictions(),
                nodeStats.getNumRegions() > 0 ? greaterThan(0L) : equalTo(0L)
            );
        });
    }
}
