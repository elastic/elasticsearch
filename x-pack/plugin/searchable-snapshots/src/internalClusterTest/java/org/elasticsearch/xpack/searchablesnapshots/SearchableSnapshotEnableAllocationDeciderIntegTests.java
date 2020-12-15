/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SearchableSnapshotEnableAllocationDeciderIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Use an unbound cache so we can recover the searchable snapshot completely all the times
            .put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES))
            .build();
    }

    public void testAllocationDisabled() throws Exception {
        final String restoredIndexName = setupMountedIndex();
        int numPrimaries = getNumShards(restoredIndexName).numPrimaries;
        setEnableAllocation(EnableAllocationDecider.Allocation.PRIMARIES);
        if (randomBoolean()) {
            setSearchableSnapshotPrimariesAllocation(EnableAllocationDecider.Allocation.NONE);
        }
        Set<String> indexNodes = internalCluster().nodesInclude(restoredIndexName);
        for (String indexNode : indexNodes) {
            internalCluster().restartNode(indexNode);
        }

        assertBusy(() -> {
            ClusterHealthResponse response =
                client().admin().cluster().health(Requests.clusterHealthRequest(restoredIndexName)).actionGet();
            assertThat(response.getUnassignedShards(), Matchers.equalTo(numPrimaries));
        });

        setSearchableSnapshotPrimariesAllocation(EnableAllocationDecider.Allocation.PRIMARIES);
        ensureGreen(restoredIndexName);
    }

    public void testAllocationEnabled() throws Exception {
        final String restoredIndexName = setupMountedIndex();
        if (randomBoolean()) {
            setEnableAllocation(EnableAllocationDecider.Allocation.PRIMARIES);
        }
        setSearchableSnapshotPrimariesAllocation(EnableAllocationDecider.Allocation.PRIMARIES);
        Set<String> indexNodes = internalCluster().nodesInclude(restoredIndexName);
        for (String indexNode : indexNodes) {
            internalCluster().restartNode(indexNode);
        }

        ensureGreen(restoredIndexName);
    }

    private String setupMountedIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(indexName, Settings.builder());

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "mock");

        final SnapshotId snapshotId = createSnapshot(repositoryName, "snapshot-1", List.of(indexName)).snapshotId();
        assertAcked(client().admin().indices().prepareDelete(indexName));
        return mountSnapshot(repositoryName, snapshotId.getName(), indexName, Settings.EMPTY);
    }

    public void setEnableAllocation(EnableAllocationDecider.Allocation allocation) {
        setAllocation(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING, allocation);
    }

    public void setSearchableSnapshotPrimariesAllocation(EnableAllocationDecider.Allocation allocation) {
        setAllocation(SearchableSnapshotEnableAllocationDecider.SEARCHABLE_SNAPSHOTS_ALLOCATION_ENABLE_PRIMARIES_SETTING, allocation);
    }

    private void setAllocation(Setting<EnableAllocationDecider.Allocation> setting, EnableAllocationDecider.Allocation allocation) {
        logger.info("--> setting allocation to [{}]", allocation);
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(setting.getKey(), allocation.name())
                        .build()
                )
                .get()
        );
    }
}
