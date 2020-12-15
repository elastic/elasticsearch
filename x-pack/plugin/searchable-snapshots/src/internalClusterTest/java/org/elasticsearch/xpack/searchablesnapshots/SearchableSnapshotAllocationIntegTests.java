/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchableSnapshotAllocationIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                // ensure the cache is definitely used
                .put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(1L, ByteSizeUnit.GB))
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockRepository.Plugin.class);
    }

    public void testAllocatesToBestAvailableNodeOnRestart() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String firstDataNode = internalCluster().startDataOnlyNode();
        final String index = "test-idx";
        createIndexWithContent(index, indexSettingsNoReplicas(1).put(INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String snapshotName = "test-snapshot";
        createSnapshot(repoName, snapshotName, List.of(index));
        assertAcked(client().admin().indices().prepareDelete(index));
        final String restoredIndex = mountSnapshot(repoName, snapshotName, index, Settings.EMPTY);
        ensureGreen(restoredIndex);
        internalCluster().startDataOnlyNodes(randomIntBetween(1, 4));

        assertAcked(client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(
                        EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(),
                        EnableAllocationDecider.Allocation.NONE.name()
                    )
            )
            .get());

        final CacheService cacheService = internalCluster().getInstance(CacheService.class, firstDataNode);
        cacheService.synchronizeCache();
        internalCluster().restartNode(firstDataNode);
        ensureStableCluster(internalCluster().numDataAndMasterNodes());

        assertAcked(client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(
                        EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(),
                        EnableAllocationDecider.Allocation.ALL.name()
                    )
                    .build()
            )
            .get());
        ensureGreen(restoredIndex);

        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertEquals(
            state.nodes().resolveNode(firstDataNode).getId(),
            state.routingTable().index(restoredIndex).shard(0).primaryShard().currentNodeId()
        );
    }
}
