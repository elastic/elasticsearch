/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.shared;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.xpack.searchablesnapshots.BaseFrozenSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheAction;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.ClearSearchableSnapshotsCacheResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction.NodeCachesStatsResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction.NodesCachesStatsResponse;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotsNodeCachesStatsAction.NodesRequest;

import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class NodesCachesStatsIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    public void testNodesCachesStats() throws Exception {
        final String[] nodeNames = internalCluster().getNodeNames();
        // here to ensure no shard relocations after the snapshot is mounted,
        // since this test verifies the cache stats on specific nodes
        ensureStableCluster(nodeNames.length);

        final String index = getTestName().toLowerCase(Locale.ROOT);
        createIndex(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());

        final int nbDocs = randomIntBetween(1_000, 10_000);
        try (BackgroundIndexer indexer = new BackgroundIndexer(index, "_doc", client(), nbDocs)) {
            waitForDocs(nbDocs, indexer);
        }
        refresh(index);

        final String repository = "repository";
        createRepository(repository, FsRepository.TYPE);

        final String snapshot = "snapshot";
        createFullSnapshot(repository, snapshot);

        assertAcked(client().admin().indices().prepareDelete(index));

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
            final long totalFsSize = client().admin()
                .cluster()
                .prepareNodesStats(nodeId)
                .clear()
                .addMetric(NodesStatsRequest.Metric.FS.metricName())
                .get()
                .getNodesMap()
                .get(nodeId)
                .getFs()
                .getTotal()
                .getTotal()
                .getBytes();

            final long cacheSize = FrozenCacheService.calculateCacheSize(clusterService.getSettings(), totalFsSize);
            assertThat(nodeCachesStats.getSize(), equalTo(cacheSize));

            final long regionSize = FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.get(clusterService.getSettings()).getBytes();
            assertThat(nodeCachesStats.getRegionSize(), equalTo(regionSize));

            assertThat(nodeCachesStats.getNumRegions(), equalTo(Math.toIntExact(cacheSize / regionSize)));
            assertThat(nodeCachesStats.getWrites(), equalTo(0L));
            assertThat(nodeCachesStats.getBytesWritten(), equalTo(0L));
            assertThat(nodeCachesStats.getReads(), equalTo(0L));
            assertThat(nodeCachesStats.getBytesRead(), equalTo(0L));
            assertThat(nodeCachesStats.getEvictions(), equalTo(0L));
        }

        for (int i = 0; i < 20; i++) {
            client().prepareSearch(mountedIndex)
                .setQuery(
                    randomBoolean()
                        ? QueryBuilders.rangeQuery("id").gte(randomIntBetween(0, 1000))
                        : QueryBuilders.termQuery("test", "value" + randomIntBetween(0, 1000))
                )
                .setSize(randomIntBetween(0, 1000))
                .get();
        }

        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);

        final ClearSearchableSnapshotsCacheResponse clearCacheResponse = client().execute(
            ClearSearchableSnapshotsCacheAction.INSTANCE,
            new ClearSearchableSnapshotsCacheRequest(mountedIndex)
        ).actionGet();
        assertThat(clearCacheResponse.getSuccessfulShards(), greaterThan(0));
        assertThat(clearCacheResponse.getFailedShards(), equalTo(0));

        final String[] dataNodesWithFrozenShards = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .routingTable()
            .index(mountedIndex)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(ShardRouting::assignedToNode)
            .map(ShardRouting::currentNodeId)
            .distinct()
            .toArray(String[]::new);

        final NodesCachesStatsResponse response = client().execute(
            TransportSearchableSnapshotsNodeCachesStatsAction.TYPE,
            new NodesRequest(dataNodesWithFrozenShards)
        ).actionGet();
        assertThat(
            response.getNodes().stream().map(r -> r.getNode().getId()).collect(Collectors.toList()),
            containsInAnyOrder(dataNodesWithFrozenShards)
        );
        assertThat(response.hasFailures(), equalTo(false));

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
