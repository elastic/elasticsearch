/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Utility class to access package private methods of the StatelessCommitService in testing outside of that package.
 */
public class StatelessCommitServiceTestUtils {

    private static final Logger logger = LogManager.getLogger(StatelessCommitServiceTestUtils.class);

    private StatelessCommitServiceTestUtils() {}

    public static void updateCommitUseTrackingForInactiveShards(StatelessCommitService statelessCommitService, Supplier<Long> time) {
        statelessCommitService.updateCommitUseTrackingForInactiveShards(time);
    }

    public static Set<String> getAllSearchNodesRetainingCommitsForShard(StatelessCommitService statelessCommitService, ShardId shardId) {
        return statelessCommitService.getAllSearchNodesRetainingCommitsForShard(shardId);
    }

    public static StatelessCommitCleaner getStatelessCommitCleaner(StatelessCommitService statelessCommitService) {
        return statelessCommitService.getCommitCleaner();
    }

    public static void logBlobReferences(StatelessCommitService statelessCommitService, ShardId shardId, Level logLevel) {
        final var commitState = statelessCommitService.getSafe(shardId);
        logger.log(logLevel, "blob references of shard [{}]: {}", shardId, commitState.getPrimaryTermAndGenToBlobReferences());
    }

    /**
     * Subclass of {@link StatelessCommitService} that can be instructed to omit the {@code @timestamp} field value range
     * from compound commits, simulating legacy CCs that predate the timestamp-range field in the CC header.
     */
    public static class NullTimestampCommitService extends StatelessCommitService {

        private final AtomicBoolean hasTimestamps = new AtomicBoolean(true);

        public NullTimestampCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            super(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        protected @Nullable StatelessCompoundCommit.TimestampFieldValueRange readTimestampFieldValueRange(
            ShardCommitState commitState,
            StatelessCommitRef reference
        ) {
            return hasTimestamps.get() ? super.readTimestampFieldValueRange(commitState, reference) : null;
        }

        public void disableTimestamps() {
            if (hasTimestamps.compareAndSet(true, false) == false) {
                throw new AssertionError("Timestamps already disabled");
            }
        }

        public void enableTimestamps() {
            if (hasTimestamps.compareAndSet(false, true) == false) {
                throw new AssertionError("Timestamps already enabled");
            }
        }
    }
}
