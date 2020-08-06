/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import com.carrotsearch.hppc.ObjectContainer;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Stream;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class SearchableSnapshotRecoveryStateIntegrationTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        builder.put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES));

        return builder.build();
    }

    public void testRecoveryStateRecoveredBytesMatchPhysicalCacheState() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepo(fsRepoName);

        final Settings.Builder originalIndexSettings = Settings.builder();
        originalIndexSettings.put(INDEX_SOFT_DELETES_SETTING.getKey(), true);
        originalIndexSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);

        createAndPopulateIndex(indexName, originalIndexSettings);

        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(fsRepoName, snapshotName)
            .setWaitForCompletion(true)
            .get();

        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotInfo.snapshotId().getName(),
            indexName,
            Settings.EMPTY,
            Strings.EMPTY_ARRAY,
            true
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(restoredIndexName);

        final Index restoredIndex = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .get()
            .getState()
            .metadata()
            .index(restoredIndexName)
            .getIndex();

        assertExecutorIsIdle(SearchableSnapshotsConstants.CACHE_PREWARMING_THREAD_POOL_NAME);
        assertExecutorIsIdle(SearchableSnapshotsConstants.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);

        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(restoredIndexName).get();
        Map<String, List<RecoveryState>> shardRecoveries = recoveryResponse.shardRecoveryStates();
        assertThat(shardRecoveries.containsKey(restoredIndexName), equalTo(true));
        List<RecoveryState> recoveryStates = shardRecoveries.get(restoredIndexName);
        assertThat(recoveryStates.size(), equalTo(1));
        RecoveryState recoveryState = recoveryStates.get(0);

        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));

        long recoveredBytes = recoveryState.getIndex().recoveredBytes();
        long physicalCacheSize = getPhysicalCacheSize(restoredIndex, snapshotInfo.snapshotId().getUUID());

        assertThat("Physical cache size doesn't match with recovery state data", physicalCacheSize, equalTo(recoveredBytes));
        assertThat("Expected to recover 100% of files", recoveryState.getIndex().recoveredBytesPercent(), equalTo(100.0f));
    }

    @SuppressForbidden(reason = "Uses FileSystem APIs")
    private long getPhysicalCacheSize(Index index, String snapshotUUID) throws Exception {
        final ObjectContainer<DiscoveryNode> dataNodes = getDiscoveryNodes().getDataNodes().values();

        assertThat(dataNodes.size(), equalTo(1));

        final String dataNode = dataNodes.iterator().next().value.getName();

        final IndexService indexService = internalCluster().getInstance(IndicesService.class, dataNode).indexService(index);
        final Path shardCachePath = CacheService.getShardCachePath(indexService.getShard(0).shardPath());

        long physicalCacheSize;
        try (Stream<Path> files = Files.list(shardCachePath.resolve(snapshotUUID))) {
            physicalCacheSize = files.map(Path::toFile).mapToLong(File::length).sum();
        }
        return physicalCacheSize;
    }

    private void assertExecutorIsIdle(String executorName) throws Exception {
        assertBusy(() -> {
            for (DiscoveryNode node : getDiscoveryNodes()) {
                ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, node.getName());
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) threadPool.executor(executorName);
                assertThat(threadPoolExecutor.getQueue().size(), equalTo(0));
                assertThat(threadPoolExecutor.getActiveCount(), equalTo(0));
            }
        });
    }

    private DiscoveryNodes getDiscoveryNodes() {
        return client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes();
    }
}
