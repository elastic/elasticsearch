/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import org.elasticsearch.xpack.searchablesnapshots.LocalStateSearchableSnapshots;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.license.LicenseSettings.SELF_GENERATED_LICENSE_TYPE;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage.FULL_COPY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchableSnapshotDiskThresholdIntegTests extends DiskUsageIntegTestCase {

    private static final long WATERMARK_BYTES = ByteSizeValue.of(10, ByteSizeUnit.KB).getBytes();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            // we want to control the refresh of cluster info updates
            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), "60m")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(LocalStateSearchableSnapshots.class, CustomMockRepositoryPlugin.class))
            .toList();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private int createIndices() throws InterruptedException {
        final int nbIndices = randomIntBetween(1, 5);
        final CountDownLatch latch = new CountDownLatch(nbIndices);

        for (int i = 0; i < nbIndices; i++) {
            final String index = "index-" + i;
            var thread = new Thread(() -> {
                try {
                    createIndex(
                        index,
                        indexSettings(1, 0).put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                            .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
                            .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                            .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), DataTier.DATA_HOT)
                            // Disable merges. A merge can cause discrepancy between the size we detect and the size in the snapshot,
                            // which could make room for more shards.
                            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                            .build()
                    );
                    int nbDocs = 100;
                    try (BackgroundIndexer indexer = new BackgroundIndexer(index, client(), nbDocs)) {
                        while (true) {
                            waitForDocs(nbDocs, indexer);
                            indexer.assertNoFailures();
                            assertNoFailures(
                                indicesAdmin().prepareForceMerge().setFlush(true).setIndices(index).setMaxNumSegments(1).get()
                            );
                            Map<String, Long> storeSize = sizeOfShardsStores(index);
                            if (storeSize.get(index) > WATERMARK_BYTES) {
                                break;
                            }
                            int moreDocs = scaledRandomIntBetween(100, 1_000);
                            indexer.continueIndexing(moreDocs);
                            nbDocs += moreDocs;
                        }
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                } finally {
                    latch.countDown();
                }
            });
            thread.start();
        }
        latch.await();
        return nbIndices;
    }

    private void createRepository(String name, String type) {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name)
                .setType(type)
                .setSettings(Settings.builder().put("location", randomRepoPath()).build())
        );
    }

    private void createSnapshot(String repository, String snapshot, int nbIndices) {
        var snapshotInfo = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repository, snapshot)
            .setIndices("index-*")
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(nbIndices));
        assertThat(snapshotInfo.failedShards(), equalTo(0));
    }

    private void mountIndices(Collection<String> indices, String prefix, String repositoryName, String snapshotName, Storage storage)
        throws InterruptedException {
        CountDownLatch mountLatch = new CountDownLatch(indices.size());
        logger.info("--> mounting [{}] indices with [{}] prefix", indices.size(), prefix);
        for (String index : indices) {
            logger.info("Mounting index {}", index);
            client().execute(
                MountSearchableSnapshotAction.INSTANCE,
                new MountSearchableSnapshotRequest(
                    TEST_REQUEST_TIMEOUT,
                    prefix + index,
                    repositoryName,
                    snapshotName,
                    index,
                    Settings.EMPTY,
                    Strings.EMPTY_ARRAY,
                    false,
                    storage
                ),
                ActionListener.wrap(response -> mountLatch.countDown(), e -> mountLatch.countDown())
            );
        }
        mountLatch.await();
    }

    public void testHighWatermarkCanNotBeExceededOnColdNode() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(onlyRole(DATA_HOT_NODE_ROLE));

        final int nbIndices = createIndices();

        final String repositoryName = "repository";
        createRepository(repositoryName, FsRepository.TYPE);

        final String snapshot = "snapshot";
        createSnapshot(repositoryName, snapshot, nbIndices);

        final Map<String, Long> indicesStoresSizes = sizeOfShardsStores("index-*");
        assertAcked(indicesAdmin().prepareDelete("index-*"));

        // The test completes reliably successfully only when we do a full copy, we can overcommit on SHARED_CACHE
        final Storage storage = FULL_COPY;
        logger.info("--> using storage [{}]", storage);

        final Settings.Builder otherDataNodeSettings = Settings.builder();
        if (storage == FULL_COPY) {
            otherDataNodeSettings.put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_COLD_NODE_ROLE.roleName());
        } else {
            otherDataNodeSettings.put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
                .put(
                    SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                    ByteSizeValue.ofBytes(Math.min(indicesStoresSizes.values().stream().mapToLong(value -> value).sum(), 5 * 1024L * 1024L))
                );
        }
        final String otherDataNode = internalCluster().startNode(otherDataNodeSettings.build());
        ensureStableCluster(3);

        final String otherDataNodeId = internalCluster().getInstance(NodeEnvironment.class, otherDataNode).nodeId();
        logger.info("--> reducing disk size of node [{}/{}] so that all shards can fit on the node", otherDataNode, otherDataNodeId);
        final long totalSpace = indicesStoresSizes.values().stream().mapToLong(size -> size).sum() + WATERMARK_BYTES + 1024L;
        getTestFileStore(otherDataNode).setTotalSpace(totalSpace);

        logger.info("--> refreshing cluster info");
        final var masterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(masterInfoService);

        DiskUsage usage = masterInfoService.getClusterInfo().getNodeMostAvailableDiskUsages().get(otherDataNodeId);
        assertThat(usage.totalBytes(), equalTo(totalSpace));

        mountIndices(indicesStoresSizes.keySet(), "mounted-", repositoryName, snapshot, storage);

        // The cold/frozen data node has enough disk space to hold all the shards
        assertBusy(() -> {
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).setRoutingTable(true).get().getState();
            assertThat(
                state.routingTable()
                    .allShards()
                    .filter(shardRouting -> state.metadata().getProject().index(shardRouting.shardId().getIndex()).isSearchableSnapshot())
                    .allMatch(
                        shardRouting -> shardRouting.state() == ShardRoutingState.STARTED
                            && otherDataNodeId.equals(shardRouting.currentNodeId())
                    ),
                equalTo(true)
            );
        });

        mountIndices(indicesStoresSizes.keySet(), "extra-", repositoryName, snapshot, storage);

        assertBusy(() -> {
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).setRoutingTable(true).get().getState();
            assertThat(
                state.routingTable()
                    .allShards()
                    .filter(
                        shardRouting -> shardRouting.shardId().getIndexName().startsWith("extra-")
                            && state.metadata().getProject().index(shardRouting.shardId().getIndex()).isSearchableSnapshot()
                    )
                    .noneMatch(
                        shardRouting -> shardRouting.state() == ShardRoutingState.STARTED
                            && otherDataNodeId.equals(shardRouting.currentNodeId())
                    ),
                equalTo(true)
            );
        });
    }

    public void testHighWatermarkCanNotBeExceededWithInitializingSearchableSnapshots() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(onlyRole(DATA_HOT_NODE_ROLE));

        int nbIndices = createIndices();

        String repositoryName = "repository";
        createRepository(repositoryName, CustomMockRepositoryPlugin.TYPE);

        String snapshotName = "snapshot";
        createSnapshot(repositoryName, snapshotName, nbIndices);

        Map<String, Long> indicesStoresSizes = sizeOfShardsStores("index-*");
        assertAcked(indicesAdmin().prepareDelete("index-*"));

        String coldNodeName = internalCluster().startNode(
            Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_COLD_NODE_ROLE.roleName()).build()
        );
        ensureStableCluster(3);

        String coldNodeId = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes().resolveNode(coldNodeName).getId();
        logger.info("--> reducing disk size of node [{}/{}] so that all shards except one can fit on the node", coldNodeName, coldNodeId);
        String indexToSkip = randomFrom(indicesStoresSizes.keySet());
        Map<String, Long> indicesToBeMounted = indicesStoresSizes.entrySet()
            .stream()
            .filter(e -> e.getKey().equals(indexToSkip) == false)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        long totalSpace = indicesToBeMounted.values().stream().mapToLong(e -> e).sum() + WATERMARK_BYTES + 1024L;
        getTestFileStore(coldNodeName).setTotalSpace(totalSpace);

        logger.info("--> refreshing cluster info");
        InternalClusterInfoService masterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(
            ClusterInfoService.class
        );
        ClusterInfoServiceUtils.refresh(masterInfoService);
        DiskUsage usage = masterInfoService.getClusterInfo().getNodeMostAvailableDiskUsages().get(coldNodeId);
        assertThat(usage.totalBytes(), equalTo(totalSpace));

        String prefix = "mounted-";
        mountIndices(indicesToBeMounted.keySet(), prefix, repositoryName, snapshotName, FULL_COPY);

        assertBusy(() -> {
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).setRoutingTable(true).get().getState();
            assertThat(
                state.routingTable()
                    .allShards()
                    .filter(s -> indicesToBeMounted.containsKey(s.shardId().getIndexName().replace(prefix, "")))
                    .filter(s -> state.metadata().getProject().index(s.shardId().getIndex()).isSearchableSnapshot())
                    .filter(s -> coldNodeId.equals(s.currentNodeId()))
                    .filter(s -> s.state() == ShardRoutingState.INITIALIZING)
                    .count(),
                equalTo((long) indicesToBeMounted.size())
            );
        });

        logger.info("--> All shards are being initialized, attempt to mount an extra index");

        mountIndices(List.of(indexToSkip), prefix, repositoryName, snapshotName, FULL_COPY);
        assertBusy(() -> {
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).setRoutingTable(true).get().getState();
            assertThat(state.routingTable().index(prefix + indexToSkip).shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        });

        logger.info("--> Unlocking the initialized shards");
        var mockRepository = (CustomMockRepository) internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class)
            .repository(repositoryName);
        mockRepository.unlockRestore();

        assertBusy(() -> {
            var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).setRoutingTable(true).get().getState();
            assertThat(state.routingTable().index(prefix + indexToSkip).shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
            assertThat(
                state.routingTable()
                    .allShards()
                    .filter(s -> indicesToBeMounted.containsKey(s.shardId().getIndexName().replace(prefix, "")))
                    .filter(s -> state.metadata().getProject().index(s.shardId().getIndex()).isSearchableSnapshot())
                    .filter(s -> coldNodeId.equals(s.currentNodeId()))
                    .filter(s -> s.state() == ShardRoutingState.STARTED)
                    .count(),
                equalTo((long) indicesToBeMounted.size())
            );
        });
    }

    private static Map<String, Long> sizeOfShardsStores(String indexPattern) {
        return Arrays.stream(indicesAdmin().prepareStats(indexPattern).clear().setStore(true).get().getShards())
            .collect(
                Collectors.toUnmodifiableMap(s -> s.getShardRouting().getIndexName(), s -> s.getStats().getStore().sizeInBytes(), Long::sum)
            );
    }

    public static class CustomMockRepositoryPlugin extends MockRepository.Plugin {

        public static final String TYPE = "custom-mock";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Collections.singletonMap(
                TYPE,
                metadata -> new CustomMockRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
            );
        }
    }

    public static class CustomMockRepository extends MockRepository {

        private static final CountDownLatch RESTORE_SHARD_LATCH = new CountDownLatch(1);

        public CustomMockRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
        }

        private void unlockRestore() {
            RESTORE_SHARD_LATCH.countDown();
        }

        @Override
        public void restoreShard(
            Store store,
            SnapshotId snapshotId,
            IndexId indexId,
            ShardId snapshotShardId,
            RecoveryState recoveryState,
            ActionListener<Void> listener
        ) {
            try {
                assertTrue(RESTORE_SHARD_LATCH.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            super.restoreShard(store, snapshotId, indexId, snapshotShardId, recoveryState, listener);
        }
    }

}
