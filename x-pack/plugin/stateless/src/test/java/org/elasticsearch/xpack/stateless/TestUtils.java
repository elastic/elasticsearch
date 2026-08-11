/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.FillCacheMemoryPressure;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtils {

    private TestUtils() {}

    /**
     * A {@link FillCacheMemoryPressure} using {@code settings} (default: heap-relative) and no telemetry, for tests that do not
     * exercise the fill-memory budget.
     */
    public static FillCacheMemoryPressure unmeteredFillCacheMemoryPressure(Settings settings, ThreadPool threadPool) {
        return new FillCacheMemoryPressure(settings, MeterRegistry.NOOP, threadPool);
    }

    public static IndicesService mockIndicesService(ClusterService clusterService) {
        final IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.clusterService()).thenReturn(clusterService);
        when(indicesService.hasShardPredicate()).thenReturn(shardId -> false);
        return indicesService;
    }

    public static IndicesService mockIndicesService(ClusterService clusterService, Predicate<ShardId> hasShardPredicate) {
        final IndicesService indicesService = mockIndicesService(clusterService);
        when(indicesService.hasShardPredicate()).thenReturn(hasShardPredicate);
        return indicesService;
    }

    /// A [ClusterService] mock backed by real [ClusterSettings] over the given node settings, for components that watch cluster settings
    /// updates in their constructor and would otherwise fail on a bare mock returning a `null` [ClusterSettings]. Registers the stateless
    /// settings that such components watch, in addition to the built-in ones.
    public static ClusterService mockClusterService(Settings settings) {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Sets.addToCopy(
                    ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                    StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING,
                    StatelessSharedBlobCacheService.STATELESS_CACHE_DEMOTE_CLOSED_SHARD_REGIONS_ENABLED_SETTING,
                    StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_TIMESTAMP_BACKFILL_ENABLED_SETTING,
                    StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_DELETED_INDEX_REGIONS_ENABLED_SETTING
                )
            )
        );
        return clusterService;
    }

    public static class StatelessPluginWithTrialLicense extends StatelessPlugin {
        public StatelessPluginWithTrialLicense(Settings settings) {
            super(settings);
        }

        protected XPackLicenseState getLicenseState() {
            return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(License.OperationMode.TRIAL, true, null));
        }
    }

    public static StatelessSharedBlobCacheService newCacheService(
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ThreadPool threadPool
    ) {
        return newCacheService(nodeEnvironment, settings, threadPool, null, mockClusterService(settings));
    }

    public static StatelessSharedBlobCacheService newCacheService(
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        return newCacheService(nodeEnvironment, settings, threadPool, meterRegistry, mockClusterService(settings));
    }

    public static StatelessSharedBlobCacheService newCacheService(
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ThreadPool threadPool,
        MeterRegistry meterRegistry,
        ClusterService clusterService
    ) {
        return new StatelessSharedBlobCacheService(
            nodeEnvironment,
            settings,
            threadPool,
            meterRegistry == null ? new BlobCacheMetrics(MeterRegistry.NOOP) : new BlobCacheMetrics(meterRegistry),
            clusterService,
            mockIndicesService(clusterService),
            new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
        );
    }

    public static StatelessCompoundCommit getCommitWithInternalFilesReplicatedRanges(
        ShardId shardId,
        BlobFile blobFile,
        String nodeEphemeralId,
        int fileOffset,
        long regionSizeInBytes
    ) {
        List<InternalFilesReplicatedRanges.InternalFileReplicatedRange> replicatedRanges = new ArrayList<>();
        Map<String, BlobLocation> commitFiles = new HashMap<>();

        long files = Math.min(
            randomIntBetween(1, 10),
            Math.floorDiv(regionSizeInBytes, REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE) // ensures all ranges fit in the first region
        );
        for (int i = 0; i < files; i++) {
            var file = "_" + i + ".cfs";
            var size = randomIntBetween(256, 10240);
            if (size < REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE) {
                replicatedRanges.add(new InternalFilesReplicatedRanges.InternalFileReplicatedRange(fileOffset, (short) size));
            } else {
                replicatedRanges.add(new InternalFilesReplicatedRanges.InternalFileReplicatedRange(fileOffset, (short) 1024));
                replicatedRanges.add(new InternalFilesReplicatedRanges.InternalFileReplicatedRange(fileOffset + size - 16, (short) 16));
            }
            commitFiles.put(file, new BlobLocation(blobFile, fileOffset, size));
            fileOffset += size;
        }
        InternalFilesReplicatedRanges ranges = InternalFilesReplicatedRanges.from(replicatedRanges);
        commitFiles = Maps.transformValues(
            commitFiles,
            location -> new BlobLocation(location.blobFile(), ranges.dataSizeInBytes() + location.offset(), location.fileLength())
        );

        return new StatelessCompoundCommit(
            shardId,
            blobFile.termAndGeneration(),
            1L,
            nodeEphemeralId,
            commitFiles,
            0,
            commitFiles.keySet(),
            0L,
            ranges,
            Map.of(),
            null
        );
    }
}
