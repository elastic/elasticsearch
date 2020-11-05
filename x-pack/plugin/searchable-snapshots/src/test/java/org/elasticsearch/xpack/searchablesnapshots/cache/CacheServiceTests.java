/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import com.carrotsearch.hppc.IntHashSet;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.index.store.cache.TestUtils.FSyncTrackingFileSystemProvider;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotAllocator;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.index.IndexModule.INDEX_RECOVERY_TYPE_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.index.store.cache.TestUtils.randomPopulateAndReads;
import static org.elasticsearch.xpack.searchablesnapshots.cache.CacheService.SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CacheServiceTests extends AbstractSearchableSnapshotsTestCase {

    private static FSyncTrackingFileSystemProvider fileSystemProvider;

    @BeforeClass
    public static void installFileSystem() {
        fileSystemProvider = new FSyncTrackingFileSystemProvider(PathUtils.getDefaultFileSystem(), createTempDir());
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
    }

    @AfterClass
    public static void removeFileSystem() {
        PathUtilsForTesting.teardown();
    }

    public void testCacheSynchronizationTaskScheduling() {
        try (CacheService cacheService = defaultCacheService()) {
            cacheService.start();

            final CacheService.CacheSynchronizationTask cacheSyncTask = cacheService.getCacheSyncTask();
            assertThat(cacheSyncTask, notNullValue());
            assertThat(cacheSyncTask.getInterval(), equalTo(SNAPSHOT_CACHE_SYNC_INTERVAL_SETTING.getDefault(Settings.EMPTY)));
            assertThat(cacheSyncTask.isScheduled(), is(false));
            assertThat(cacheSyncTask.isClosed(), is(false));

            final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
            final int nbShards = randomIntBetween(1, 10);
            ClusterServiceUtils.setState(clusterService, createUnassignedShardsToClusterState(clusterService.state(), indexName, nbShards));
            assertThat(cacheSyncTask.isScheduled(), is(false));

            ClusterServiceUtils.setState(clusterService, assignShardsToLocalNodeInClusterState(clusterService.state()));
            assertThat(cacheSyncTask.isScheduled(), is(false));

            ClusterServiceUtils.setState(clusterService, startShardsInClusterState(clusterService.state()));
            assertThat(cacheSyncTask.isScheduled(), is(true));

            ClusterServiceUtils.setState(clusterService, removeShardsInClusterState(clusterService.state()));
            assertThat(cacheSyncTask.isScheduled(), is(false));

            cacheService.stop();
            assertThat(cacheSyncTask.isScheduled(), is(false));
            assertThat(cacheSyncTask.isClosed(), is(true));
        }
    }

    public void testHasSearchableSnapshotShards() {
        final String localNodeId = clusterService.localNode().getId();

        ClusterState clusterState = clusterService.state();
        assertThat(CacheService.hasSearchableSnapshotShards(clusterState, localNodeId), is(false));

        final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        clusterState = createUnassignedShardsToClusterState(clusterState, indexName, randomIntBetween(1, 10));
        clusterState = assignShardsToLocalNodeInClusterState(clusterState);

        assertThat(CacheService.hasSearchableSnapshotShards(clusterState, localNodeId), is(false));

        clusterState = startShardsInClusterState(clusterState);
        assertThat(CacheService.hasSearchableSnapshotShards(clusterState, localNodeId), is(true));
        assertThat(CacheService.hasSearchableSnapshotShards(clusterState, "other"), is(false));
    }

    public void testCacheSynchronization() throws Exception {
        final int numShards = randomIntBetween(1, 3);
        final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        logger.debug("--> creating index [{}] with [{}] unassigned primaries in cluster state", indexName, numShards);
        ClusterServiceUtils.setState(clusterService, createUnassignedShardsToClusterState(clusterService.state(), indexName, numShards));

        final IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        final Index index = clusterService.state().metadata().index(indexName).getIndex();

        final SnapshotId snapshotId = new SnapshotId(
            SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.get(indexMetadata.getSettings()),
            SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexMetadata.getSettings())
        );
        final IndexId indexId = new IndexId(
            SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING.get(indexMetadata.getSettings()),
            SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.get(indexMetadata.getSettings())
        );

        logger.debug("--> creating shard directories on disk");
        final Path[] shardsCacheDirs = new Path[numShards];
        for (int i = 0; i < numShards; i++) {
            final Path shardDataPath = randomFrom(nodeEnvironment.availableShardPaths(new ShardId(index, i)));
            assertFalse(Files.exists(shardDataPath));

            logger.debug("--> creating directories [{}] for shard [{}]", shardDataPath.toAbsolutePath(), i);
            shardsCacheDirs[i] = Files.createDirectories(CacheService.resolveSnapshotCache(shardDataPath).resolve(snapshotId.getUUID()));
        }

        logger.debug("--> assigning searchable snapshot shards to local node");
        ClusterServiceUtils.setState(clusterService, assignShardsToLocalNodeInClusterState(clusterService.state()));

        try (CacheService cacheService = defaultCacheService()) {
            cacheService.start();

            logger.debug("--> setting large cache sync interval (explicit cache synchronization calls in test)");
            cacheService.setCacheSyncInterval(TimeValue.timeValueMillis(Long.MAX_VALUE));

            logger.debug("--> starting searchable snapshot shards");
            ClusterServiceUtils.setState(clusterService, startShardsInClusterState(clusterService.state()));

            // Keep a count of the number of writes for every cache file existing in the cache
            final Map<CacheKey, Tuple<CacheFile, Integer>> previous = new HashMap<>();

            for (int iteration = 0; iteration < between(1, 10); iteration++) {

                final Map<CacheKey, Tuple<CacheFile, Integer>> updates = new HashMap<>();

                logger.trace("--> more random reads/writes from existing cache files");
                for (Map.Entry<CacheKey, Tuple<CacheFile, Integer>> cacheEntry : randomSubsetOf(previous.entrySet())) {
                    final CacheKey cacheKey = cacheEntry.getKey();
                    final CacheFile cacheFile = cacheEntry.getValue().v1();

                    final CacheFile.EvictionListener listener = evictedCacheFile -> {};
                    cacheFile.acquire(listener);

                    final SortedSet<Tuple<Long, Long>> newCacheRanges = randomPopulateAndReads(cacheFile);
                    if (newCacheRanges.isEmpty() == false) {
                        final int numberOfWrites = cacheEntry.getValue().v2() + 1;
                        updates.put(cacheKey, Tuple.tuple(cacheFile, numberOfWrites));
                    }
                    cacheFile.release(listener);
                }

                logger.trace("--> creating new cache files and randomly read/write them");
                for (int i = 0; i < between(0, 25); i++) {
                    final ShardId shardId = new ShardId(index, randomIntBetween(0, numShards - 1));
                    final String fileName = String.format(Locale.ROOT, "file_%d_%d", iteration, i);
                    final CacheKey cacheKey = new CacheKey(snapshotId, indexId, shardId, fileName);
                    final CacheFile cacheFile = cacheService.get(cacheKey, randomIntBetween(1, 10_000), shardsCacheDirs[shardId.id()]);

                    final CacheFile.EvictionListener listener = evictedCacheFile -> {};
                    cacheFile.acquire(listener);

                    final SortedSet<Tuple<Long, Long>> newRanges = randomPopulateAndReads(cacheFile);
                    updates.put(cacheKey, Tuple.tuple(cacheFile, newRanges.isEmpty() ? 0 : 1));
                    cacheFile.release(listener);
                }

                logger.trace("--> evicting random cache files");
                for (CacheKey evictedCacheKey : randomSubsetOf(Sets.union(previous.keySet(), updates.keySet()))) {
                    cacheService.removeFromCache(evictedCacheKey::equals);
                    previous.remove(evictedCacheKey);
                    updates.remove(evictedCacheKey);
                }

                logger.trace("--> capturing expected number of fsyncs per shard cache directory before synchronization");
                final Map<Path, Integer> shardCacheDirFSyncs = new HashMap<>();
                for (int i = 0; i < shardsCacheDirs.length; i++) {
                    final Path shardCacheDir = shardsCacheDirs[i];
                    final ShardId shardId = new ShardId(index, i);
                    final Integer numberOfFSyncs = fileSystemProvider.getNumberOfFSyncs(shardCacheDir);
                    if (updates.entrySet()
                        .stream()
                        .filter(update -> update.getValue().v2() != null)
                        .filter(update -> update.getValue().v2() > 0)
                        .anyMatch(update -> update.getKey().getShardId().equals(shardId))) {
                        shardCacheDirFSyncs.put(shardCacheDir, numberOfFSyncs == null ? 1 : numberOfFSyncs + 1);
                    } else {
                        shardCacheDirFSyncs.put(shardCacheDir, numberOfFSyncs);
                    }
                }

                logger.debug("--> synchronizing cache files [#{}]", iteration);
                cacheService.synchronizeCache();

                logger.trace("--> verifying cache synchronization correctness");
                shardCacheDirFSyncs.forEach((dir, expected) -> assertThat(fileSystemProvider.getNumberOfFSyncs(dir), equalTo(expected)));
                previous.putAll(updates);
                previous.forEach(
                    (key, tuple) -> assertThat(fileSystemProvider.getNumberOfFSyncs(tuple.v1().getFile()), equalTo(tuple.v2()))
                );
            }
        }
    }

    private static ClusterState createUnassignedShardsToClusterState(
        final ClusterState currentState,
        final String indexName,
        final int numberOfShards
    ) {
        final String repositoryName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(5), UUIDs.randomBase64UUID(random()));
        final IndexId indexId = new IndexId(indexName, UUIDs.randomBase64UUID(random()));
        final RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(random()),
            new Snapshot(repositoryName, snapshotId),
            Version.CURRENT,
            indexId
        );

        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
                    .put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
                    .put(SearchableSnapshots.SNAPSHOT_REPOSITORY_SETTING.getKey(), repositoryName)
                    .put(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.getKey(), snapshotId.getName())
                    .put(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.getKey(), snapshotId.getUUID())
                    .put(SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING.getKey(), indexId.getName())
                    .put(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.getKey(), indexId.getId())
                    .put(INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsConstants.SNAPSHOT_DIRECTORY_FACTORY_KEY)
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
                    .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), SearchableSnapshotAllocator.ALLOCATOR_NAME)
                    .put(INDEX_RECOVERY_TYPE_SETTING.getKey(), SearchableSnapshotsConstants.SNAPSHOT_RECOVERY_STATE_FACTORY_KEY)
            );

        for (int i = 0; i < numberOfShards; i++) {
            indexMetadataBuilder.putInSyncAllocationIds(i, Collections.singleton(AllocationId.newInitializing().getId()));
        }

        final Metadata.Builder metadata = Metadata.builder(currentState.metadata())
            .put(indexMetadataBuilder.build(), true)
            .generateClusterUuidIfNeeded();

        final IndexMetadata indexMetadata = metadata.get(indexName);
        final Index index = indexMetadata.getIndex();

        final RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());
        routingTable.add(IndexRoutingTable.builder(index).initializeAsNewRestore(indexMetadata, recoverySource, new IntHashSet()).build());

        final RestoreInProgress.Builder restores = new RestoreInProgress.Builder(
            currentState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)
        );
        final ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shards = ImmutableOpenMap.builder();
        for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
            shards.put(new ShardId(index, i), new RestoreInProgress.ShardRestoreStatus(currentState.nodes().getLocalNodeId()));
        }

        restores.add(
            new RestoreInProgress.Entry(
                recoverySource.restoreUUID(),
                recoverySource.snapshot(),
                RestoreInProgress.State.INIT,
                Collections.singletonList(indexName),
                shards.build()
            )
        );

        return ClusterState.builder(currentState)
            .putCustom(RestoreInProgress.TYPE, restores.build())
            .routingTable(routingTable.build())
            .metadata(metadata)
            .build();
    }

    private static ClusterState assignShardsToLocalNodeInClusterState(final ClusterState currentState) {
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        for (IndexRoutingTable indexRoutingTable : currentState.routingTable()) {
            List<ShardRouting> unassignedShards = indexRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED);
            if (unassignedShards.isEmpty() == false) {
                final IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexRoutingTable.getIndex());
                unassignedShards.forEach(
                    shardRouting -> indexRoutingTableBuilder.addShard(
                        shardRouting.initialize(
                            currentState.getNodes().getLocalNodeId(),
                            null,
                            ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                        )
                    )
                );
                routingTableBuilder.add(indexRoutingTableBuilder);
            }
        }
        return ClusterState.builder(currentState).routingTable(routingTableBuilder.build()).build();
    }

    private static ClusterState startShardsInClusterState(final ClusterState currentState) {
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        for (IndexRoutingTable indexRoutingTable : currentState.routingTable()) {
            List<ShardRouting> unassignedShards = indexRoutingTable.shardsWithState(ShardRoutingState.INITIALIZING);
            if (unassignedShards.isEmpty() == false) {
                final IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexRoutingTable.getIndex());
                unassignedShards.forEach(shardRouting -> indexRoutingTableBuilder.addShard(shardRouting.moveToStarted()));
                routingTableBuilder.add(indexRoutingTableBuilder);
            }
        }
        return ClusterState.builder(currentState).routingTable(routingTableBuilder.build()).build();
    }

    private static ClusterState removeShardsInClusterState(final ClusterState currentState) {
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        for (IndexRoutingTable indexRoutingTable : currentState.routingTable()) {
            List<ShardRouting> unassignedShards = indexRoutingTable.shardsWithState(ShardRoutingState.STARTED);
            if (unassignedShards.isEmpty() == false) {
                routingTableBuilder.remove(indexRoutingTable.getIndex().getName());
            }
        }
        return ClusterState.builder(currentState).routingTable(routingTableBuilder.build()).build();
    }
}
