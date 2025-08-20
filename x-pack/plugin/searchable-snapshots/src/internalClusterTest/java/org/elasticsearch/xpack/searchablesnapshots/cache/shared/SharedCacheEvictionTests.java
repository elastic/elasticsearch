/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.shared;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardStateMetadata;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.BaseFrozenSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.LocalStateSearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class SharedCacheEvictionTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    private static final Map<String, SharedBlobCacheService<CacheKey>> sharedBlobCacheServices = new ConcurrentHashMap<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> classes = super.nodePlugins();
        return Stream.concat(
            classes.stream().filter(plugin -> plugin != LocalStateSearchableSnapshots.class),
            Stream.of(SpyableSharedCacheSearchableSnapshots.class)
        ).toList();
    }

    @SuppressWarnings("unchecked")
    @Before
    public void clearEvictionStats() {
        sharedBlobCacheServices.values().forEach(Mockito::clearInvocations);
    }

    public void testPartialShardsAreEvictedAsynchronouslyOnDelete() throws Exception {
        final String mountedSnapshotName = "mounted_" + randomIdentifier();
        snapshotAndMount(mountedSnapshotName, MountSearchableSnapshotRequest.Storage.SHARED_CACHE);

        final Map<String, Integer> allocations = getShardCounts(mountedSnapshotName);

        assertAcked(indicesAdmin().prepareDelete(mountedSnapshotName));
        allocations.forEach((nodeId, shardCount) -> {
            SharedBlobCacheService<CacheKey> sharedBlobCacheService = sharedBlobCacheServices.get(nodeId);
            verify(sharedBlobCacheService, Mockito.atLeast(shardCount)).forceEvictAsync(ArgumentMatchers.any());
            verify(sharedBlobCacheService, never()).forceEvict(ArgumentMatchers.any());
        });
    }

    /**
     * Fully mounted snapshots don't use the shared blob cache, so we don't need to evict them from it
     */
    public void testFullFullShardsAreNotEvictedOnDelete() throws Exception {
        final String mountedSnapshotName = "mounted_" + randomIdentifier();
        snapshotAndMount(mountedSnapshotName, MountSearchableSnapshotRequest.Storage.FULL_COPY);

        final Map<String, Integer> allocations = getShardCounts(mountedSnapshotName);

        assertAcked(indicesAdmin().prepareDelete(mountedSnapshotName));
        allocations.forEach((nodeId, shardCount) -> {
            SharedBlobCacheService<CacheKey> sharedBlobCacheService = sharedBlobCacheServices.get(nodeId);
            verify(sharedBlobCacheService, never()).forceEvictAsync(ArgumentMatchers.any());
            verify(sharedBlobCacheService, never()).forceEvict(ArgumentMatchers.any());
        });
    }

    /**
     * We let relocated shards age out of the cache, rather than evicting them
     */
    public void testPartialShardsAreNotEvictedOnRelocate() throws Exception {
        final String mountedSnapshotName = "mounted_" + randomIdentifier();
        snapshotAndMount(mountedSnapshotName, MountSearchableSnapshotRequest.Storage.SHARED_CACHE);

        final Map<String, Integer> allocations = getShardCounts(mountedSnapshotName);

        // Create another node to relocate to if we need to
        if (internalCluster().numDataNodes() == 1) {
            internalCluster().startNode();
            ensureStableCluster(2);
        }

        final String nodeToVacateId = randomFrom(allocations.keySet());
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._id", nodeToVacateId));
        try {
            waitForRelocation(ClusterHealthStatus.GREEN);
            assertThat(getShardCounts(mountedSnapshotName).keySet(), not(contains(nodeToVacateId)));

            final SharedBlobCacheService<CacheKey> sharedBlobCacheService = sharedBlobCacheServices.get(nodeToVacateId);
            verify(sharedBlobCacheService, never()).forceEvictAsync(ArgumentMatchers.any());
            verify(sharedBlobCacheService, never()).forceEvict(ArgumentMatchers.any());
        } finally {
            updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.exclude._id"));
        }
    }

    public void testPartialShardsAreEvictedSynchronouslyOnFailure() throws Exception {
        final String mountedSnapshotName = "mounted_" + randomIdentifier();

        snapshotAndMount(mountedSnapshotName, MountSearchableSnapshotRequest.Storage.SHARED_CACHE);

        final IndicesStatsResponse indicesStatsResponse = indicesAdmin().prepareStats(mountedSnapshotName).get();
        final Set<ShardId> allShards = Arrays.stream(indicesStatsResponse.getIndex(mountedSnapshotName).getShards())
            .map(sh -> sh.getShardRouting().shardId())
            .collect(Collectors.toSet());
        final String indexUUID = indicesStatsResponse.getIndex(mountedSnapshotName).getUuid();

        // Add a node to the cluster, we'll force relocation to it
        final String targetNode = internalCluster().startNode();

        // Put some conflicting shard state in the new node's shard paths to trigger a failure
        final NodeEnvironment nodeEnvironment = internalCluster().getInstance(NodeEnvironment.class, targetNode);
        for (ShardId shardId : allShards) {
            for (Path p : nodeEnvironment.availableShardPaths(shardId)) {
                ShardStateMetadata.FORMAT.write(
                    new ShardStateMetadata(true, randomValueOtherThan(indexUUID, ESTestCase::randomIdentifier), null),
                    p
                );
            }
        }

        // Force relocation, it should fail
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.require._name", targetNode));

        try {
            waitForRelocation();

            final SharedBlobCacheService<CacheKey> sharedBlobCacheService = sharedBlobCacheServices.get(getNodeId(targetNode));
            verify(sharedBlobCacheService, never()).forceEvictAsync(ArgumentMatchers.any());
            verify(sharedBlobCacheService, Mockito.atLeast(allShards.size())).forceEvict(ArgumentMatchers.any());
        } finally {
            updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.require._name"));
        }
    }

    private Map<String, Integer> getShardCounts(String indexName) {
        final IndicesStatsResponse indicesStatsResponse = indicesAdmin().prepareStats(indexName).get();
        final Map<String, Integer> allocations = new HashMap<>();
        Arrays.stream(indicesStatsResponse.getShards())
            .forEach(shardStats -> allocations.compute(shardStats.getShardRouting().currentNodeId(), (s, v) -> v == null ? 1 : v + 1));
        assertThat(allocations, not(anEmptyMap()));
        return allocations;
    }

    private void snapshotAndMount(String mountedSnapshotName, MountSearchableSnapshotRequest.Storage storage) throws Exception {
        final String repositoryName = randomIdentifier();
        final String indexName = randomValueOtherThan(mountedSnapshotName, ESTestCase::randomIdentifier);
        final String snapshotName = randomIdentifier();

        createRepository(repositoryName, "fs");
        createIndexWithRandomDocs(indexName, randomIntBetween(10, 300));
        createSnapshot(repositoryName, snapshotName, List.of(indexName));
        mountSnapshot(repositoryName, snapshotName, indexName, mountedSnapshotName, Settings.EMPTY, storage);
        ensureGreen(mountedSnapshotName);
    }

    public static class SpyableSharedCacheSearchableSnapshots extends LocalStateCompositeXPackPlugin implements SystemIndexPlugin {

        private final SearchableSnapshots plugin;

        public SpyableSharedCacheSearchableSnapshots(final Settings settings, final Path configPath) {
            super(settings, configPath);
            this.plugin = new SearchableSnapshots(settings) {

                @Override
                protected XPackLicenseState getLicenseState() {
                    return SpyableSharedCacheSearchableSnapshots.this.getLicenseState();
                }

                @Override
                protected SharedBlobCacheService<CacheKey> createSharedBlobCacheService(
                    Settings settings,
                    ThreadPool threadPool,
                    NodeEnvironment nodeEnvironment,
                    BlobCacheMetrics blobCacheMetrics
                ) {
                    final SharedBlobCacheService<CacheKey> spy = Mockito.spy(
                        super.createSharedBlobCacheService(settings, threadPool, nodeEnvironment, blobCacheMetrics)
                    );
                    sharedBlobCacheServices.put(nodeEnvironment.nodeId(), spy);
                    return spy;
                }
            };
            plugins.add(plugin);
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return plugin.getSystemIndexDescriptors(settings);
        }

        @Override
        public String getFeatureName() {
            return plugin.getFeatureName();
        }

        @Override
        public String getFeatureDescription() {
            return plugin.getFeatureDescription();
        }
    }
}
