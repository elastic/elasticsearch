/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.elasticsearch.xpack.snapshotbasedrecoveries.SnapshotBasedRecoveriesPlugin;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.license.LicenseSettings.SELF_GENERATED_LICENSE_TYPE;
import static org.elasticsearch.test.NodeRoles.addRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.pageAligned;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = false, numClientNodes = 0)
public abstract class BaseSearchableSnapshotsIntegTestCase extends AbstractSnapshotIntegTestCase {
    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(BlobCachePlugin.class);
        plugins.add(LocalStateSearchableSnapshots.class);
        plugins.add(LicensedSnapshotBasedRecoveriesPlugin.class);
        plugins.add(ForbiddenActionsPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings settings;
        {
            final Settings initialSettings = super.nodeSettings(nodeOrdinal, otherSettings);
            if (DiscoveryNode.canContainData(otherSettings)) {
                settings = addRoles(initialSettings, Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE));
            } else {
                settings = initialSettings;
            }
        }
        final Settings.Builder builder = Settings.builder().put(settings).put(SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        if (randomBoolean()) {
            builder.put(
                CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(),
                rarely()
                    ? ByteSizeValue.of(randomIntBetween(4, 1024), ByteSizeUnit.KB)
                    : ByteSizeValue.of(randomIntBetween(1, 10), ByteSizeUnit.MB)
            );
        }
        if (DiscoveryNode.canContainData(otherSettings) && randomBoolean()) {
            builder.put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ZERO.getStringRep());
        }
        builder.put(
            SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
            rarely()
                ? pageAligned(ByteSizeValue.of(randomIntBetween(4, 1024), ByteSizeUnit.KB))
                : pageAligned(ByteSizeValue.of(randomIntBetween(1, 10), ByteSizeUnit.MB))
        );
        if (randomBoolean()) {
            builder.put(
                SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(),
                rarely()
                    ? pageAligned(ByteSizeValue.of(randomIntBetween(4, 1024), ByteSizeUnit.KB))
                    : pageAligned(ByteSizeValue.of(randomIntBetween(1, 10), ByteSizeUnit.MB))
            );
        }
        if (randomBoolean()) {
            builder.put(
                SharedBlobCacheService.SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(),
                rarely()
                    ? pageAligned(ByteSizeValue.of(randomIntBetween(4, 1024), ByteSizeUnit.KB))
                    : pageAligned(ByteSizeValue.of(randomIntBetween(1, 10), ByteSizeUnit.MB))
            );
        }
        return builder.build();
    }

    @After
    public void waitForBlobCacheFillsToComplete() {
        for (BlobStoreCacheService blobStoreCacheService : internalCluster().getDataNodeInstances(BlobStoreCacheService.class)) {
            assertTrue(blobStoreCacheService.waitForInFlightCacheFillsToComplete(30L, TimeUnit.SECONDS));
        }
    }

    @Override
    protected void createRepository(String repoName, String type, Settings.Builder settings, boolean verify) {
        // add use for peer recovery setting randomly to verify that these features work together.
        Settings.Builder newSettings = randomBoolean()
            ? settings
            : Settings.builder().put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), true).put(settings.build());
        super.createRepository(repoName, type, newSettings, verify);
    }

    protected String mountSnapshot(String repositoryName, String snapshotName, String indexName, Settings restoredIndexSettings)
        throws Exception {
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        mountSnapshot(repositoryName, snapshotName, indexName, restoredIndexName, restoredIndexSettings);
        return restoredIndexName;
    }

    protected void mountSnapshot(
        String repositoryName,
        String snapshotName,
        String indexName,
        String restoredIndexName,
        Settings restoredIndexSettings
    ) throws Exception {
        mountSnapshot(repositoryName, snapshotName, indexName, restoredIndexName, restoredIndexSettings, Storage.FULL_COPY);
    }

    protected void mountSnapshot(
        String repositoryName,
        String snapshotName,
        String indexName,
        String restoredIndexName,
        Settings restoredIndexSettings,
        final Storage storage
    ) throws Exception {
        final MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            TEST_REQUEST_TIMEOUT,
            restoredIndexName,
            repositoryName,
            snapshotName,
            indexName,
            restoredIndexSettings,
            Strings.EMPTY_ARRAY,
            true,
            storage
        );

        final RestoreSnapshotResponse restoreResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, mountRequest).actionGet();
        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(getNumShards(restoredIndexName).numPrimaries));
        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));
    }

    protected void createAndPopulateIndex(String indexName, Settings.Builder settings) throws InterruptedException {
        assertAcked(prepareCreate(indexName, settings));
        ensureGreen(indexName);
        populateIndex(indexName, 100);
    }

    protected void populateIndex(String indexName, int maxIndexRequests) throws InterruptedException {
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        // This index does not permit dynamic fields, so we can only use defined field names
        final String key = indexName.equals(SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX) ? "type" : "foo";
        for (int i = between(10, maxIndexRequests); i >= 0; i--) {
            indexRequestBuilders.add(prepareIndex(indexName).setSource(key, randomBoolean() ? "bar" : "baz"));
        }
        indexRandom(true, true, indexRequestBuilders);
        refresh(indexName);
        if (randomBoolean()) {
            assertThat(
                indicesAdmin().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
                equalTo(0)
            );
        }
    }

    protected void checkSoftDeletesNotEagerlyLoaded(String restoredIndexName) {
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                if (indexService.index().getName().equals(restoredIndexName)) {
                    for (IndexShard indexShard : indexService) {
                        try {
                            Engine engine = IndexShardTestCase.getEngine(indexShard);
                            assertThat(engine, instanceOf(ReadOnlyEngine.class));
                            EngineTestCase.checkNoSoftDeletesLoaded((ReadOnlyEngine) engine);
                        } catch (AlreadyClosedException ace) {
                            // ok to ignore these
                        }
                    }
                }
            }
        }
    }

    protected void assertShardFolders(String indexName, boolean isSearchableSnapshot) throws IOException {
        final Index restoredIndex = resolveIndex(indexName);
        final String customDataPath = resolveCustomDataPath(indexName);
        final ShardId shardId = new ShardId(restoredIndex, 0);
        boolean shardFolderFound = false;
        for (String node : internalCluster().getNodeNames()) {
            final NodeEnvironment service = internalCluster().getInstance(NodeEnvironment.class, node);
            final ShardPath shardPath = ShardPath.loadShardPath(logger, service, shardId, customDataPath);
            if (shardPath != null && Files.exists(shardPath.getDataPath())) {
                shardFolderFound = true;
                final boolean indexExists = Files.exists(shardPath.resolveIndex());
                final boolean translogExists = Files.exists(shardPath.resolveTranslog());
                logger.info(
                    "--> [{}] verifying shard data path [{}] (index exists: {}, translog exists: {})",
                    node,
                    shardPath.getDataPath(),
                    indexExists,
                    translogExists
                );
                assertThat(
                    isSearchableSnapshot ? "Index file should not exist" : "Index file should exist",
                    indexExists,
                    not(isSearchableSnapshot)
                );
                if (isSearchableSnapshot) {
                    assertThat("Translog should not exist", translogExists, equalTo(false));
                } else {
                    assertThat("Translog should exist", translogExists, equalTo(true));
                    try (Stream<Path> dir = Files.list(shardPath.resolveTranslog())) {
                        final long translogFiles = dir.filter(path -> path.getFileName().toString().contains("translog")).count();
                        assertThat(
                            "There should be 2+ translog files for a non-snapshot directory",
                            translogFiles,
                            greaterThanOrEqualTo(2L)
                        );
                    }
                }
            }
        }
        assertTrue("no shard folder found for index " + indexName, shardFolderFound);
    }

    protected void assertTotalHits(String indexName, TotalHits originalAllHits, TotalHits originalBarHits) throws Exception {
        final Thread[] threads = new Thread[between(1, 5)];
        final AtomicArray<TotalHits> allHits = new AtomicArray<>(threads.length);
        final AtomicArray<TotalHits> barHits = new AtomicArray<>(threads.length);

        final CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < threads.length; i++) {
            int t = i;
            threads[i] = new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertResponse(prepareSearch(indexName).setTrackTotalHits(true), resp -> allHits.set(t, resp.getHits().getTotalHits()));
                assertResponse(
                    prepareSearch(indexName).setTrackTotalHits(true).setQuery(matchQuery("foo", "bar")),
                    resp -> barHits.set(t, resp.getHits().getTotalHits())
                );
            });
            threads[i].start();
        }

        ensureGreen(indexName);
        latch.countDown();

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();

            final TotalHits allTotalHits = allHits.get(i);
            final TotalHits barTotalHits = barHits.get(i);

            logger.info("--> thread #{} has [{}] hits in total, of which [{}] match the query", i, allTotalHits, barTotalHits);
            assertThat(allTotalHits, equalTo(originalAllHits));
            assertThat(barTotalHits, equalTo(originalBarHits));
        }
    }

    protected void assertRecoveryStats(String indexName, boolean preWarmEnabled) throws Exception {
        int shardCount = getNumShards(indexName).totalNumShards;
        assertBusy(() -> {
            final RecoveryResponse recoveryResponse = indicesAdmin().prepareRecoveries(indexName).get();
            assertThat(recoveryResponse.toString(), recoveryResponse.shardRecoveryStates().get(indexName).size(), equalTo(shardCount));

            for (List<RecoveryState> recoveryStates : recoveryResponse.shardRecoveryStates().values()) {
                for (RecoveryState recoveryState : recoveryStates) {
                    RecoveryState.Index index = recoveryState.getIndex();
                    assertThat(
                        Strings.toString(recoveryState, true, true),
                        index.recoveredFileCount(),
                        preWarmEnabled ? equalTo(index.totalRecoverFiles()) : greaterThanOrEqualTo(0)
                    );
                    assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
                }
            }
        }, 30L, TimeUnit.SECONDS);
    }

    protected DiscoveryNodes getDiscoveryNodes() {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).clear().setNodes(true).get().getState().nodes();
    }

    protected void assertExecutorIsIdle(String executorName) throws Exception {
        assertBusy(() -> {
            for (ThreadPool threadPool : internalCluster().getInstances(ThreadPool.class)) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) threadPool.executor(executorName);
                assertThat(threadPoolExecutor.getQueue().size(), equalTo(0));
                assertThat(threadPoolExecutor.getActiveCount(), equalTo(0));
            }
        });
    }

    protected static void waitUntilRecoveryIsDone(String index) throws Exception {
        assertBusy(() -> {
            RecoveryResponse recoveryResponse = indicesAdmin().prepareRecoveries(index).get();
            assertThat(recoveryResponse.hasRecoveries(), equalTo(true));
            for (List<RecoveryState> value : recoveryResponse.shardRecoveryStates().values()) {
                for (RecoveryState recoveryState : value) {
                    assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
                }
            }
        });
    }

    public static class LicensedSnapshotBasedRecoveriesPlugin extends SnapshotBasedRecoveriesPlugin {

        public LicensedSnapshotBasedRecoveriesPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public boolean isLicenseEnabled() {
            return true;
        }
    }

    public static class ForbiddenActionsPlugin extends Plugin implements ActionPlugin {

        private ActionFilter actionFilter;

        @Override
        public Collection<?> createComponents(PluginServices services) {
            final var clusterService = services.clusterService();
            actionFilter = new ActionFilter.Simple() {
                @Override
                protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
                    if (action.equals(TransportNodesListShardStoreMetadata.ACTION_NAME)) {
                        final var shardId = asInstanceOf(TransportNodesListShardStoreMetadata.Request.class, request).shardId();
                        final var indexMetadata = clusterService.state().metadata().getProject().index(shardId.getIndex());
                        if (indexMetadata != null) {
                            assertFalse(shardId.toString(), indexMetadata.isSearchableSnapshot());
                        }
                    }
                    return true;
                }

                @Override
                public int order() {
                    return 0;
                }
            };
            return List.of();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(actionFilter);
        }
    }
}
