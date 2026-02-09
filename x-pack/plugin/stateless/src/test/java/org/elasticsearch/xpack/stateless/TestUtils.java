/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.RecoveryCommitRegistrationHandler;
import org.elasticsearch.xpack.stateless.reshard.SplitSourceService;
import org.elasticsearch.xpack.stateless.reshard.SplitTargetService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE;

public class TestUtils {

    private TestUtils() {}

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
        return newCacheService(nodeEnvironment, settings, threadPool, null);
    }

    public static StatelessSharedBlobCacheService newCacheService(
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ThreadPool threadPool,
        MeterRegistry meterRegistry
    ) {
        StatelessSharedBlobCacheService statelessSharedBlobCacheService = new StatelessSharedBlobCacheService(
            nodeEnvironment,
            settings,
            threadPool,
            meterRegistry == null ? new BlobCacheMetrics(MeterRegistry.NOOP) : new BlobCacheMetrics(meterRegistry)
        );
        statelessSharedBlobCacheService.assertInvariants();
        return statelessSharedBlobCacheService;
    }

    public static StatelessIndexEventListener newStatelessIndexEventListener(
        ThreadPool threadPool,
        StatelessCommitService statelessCommitService,
        ObjectStoreService objectStoreService,
        TranslogReplicator translogReplicator,
        RecoveryCommitRegistrationHandler recoveryCommitRegistrationHandler,
        SharedBlobCacheWarmingService warmingService,
        HollowShardsService hollowShardsService,
        SplitTargetService splitTargetService,
        SplitSourceService splitSourceService,
        ProjectResolver projectResolver,
        Executor bccHeaderReadExecutor,
        ClusterSettings clusterSettings,
        StatelessSharedBlobCacheService cacheService
    ) {
        return new StatelessIndexEventListener(
            threadPool,
            statelessCommitService,
            objectStoreService,
            translogReplicator,
            recoveryCommitRegistrationHandler,
            warmingService,
            hollowShardsService,
            splitTargetService,
            splitSourceService,
            projectResolver,
            bccHeaderReadExecutor,
            clusterSettings,
            cacheService
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
