/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexWriteLoadTests extends ESTestCase {

    public void testGetWriteLoadForShardAndGetUptimeInMillisForShard() {
        final int numberOfPopulatedShards = 10;
        final int numberOfShards = randomIntBetween(numberOfPopulatedShards, 20);
        final IndexWriteLoad.Builder indexWriteLoadBuilder = IndexWriteLoad.builder(numberOfShards);

        final double[] populatedShardWriteLoads = new double[numberOfPopulatedShards];
        final long[] populatedShardUptimes = new long[numberOfPopulatedShards];
        for (int shardId = 0; shardId < numberOfPopulatedShards; shardId++) {
            double writeLoad = randomDoubleBetween(1, 128, true);
            long uptimeInMillis = randomNonNegativeLong();
            populatedShardWriteLoads[shardId] = writeLoad;
            populatedShardUptimes[shardId] = uptimeInMillis;
            indexWriteLoadBuilder.withShardWriteLoad(shardId, writeLoad, uptimeInMillis);
        }

        final IndexWriteLoad indexWriteLoad = indexWriteLoadBuilder.build();
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            if (shardId < numberOfPopulatedShards) {
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId).isPresent(), is(equalTo(true)));
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId).getAsDouble(), is(equalTo(populatedShardWriteLoads[shardId])));
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId).isPresent(), is(equalTo(true)));
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId).getAsLong(), is(equalTo(populatedShardUptimes[shardId])));
            } else {
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId).isPresent(), is(false));
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId).isPresent(), is(false));
            }
        }
    }

    public void testFromStatsCreation() {
        final String indexName = "idx";
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .build()
            )
            .build();

        final IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        final IndexStats indexStats = mock(IndexStats.class);

        // Shard 0 has both primary/replica
        final IndexShardStats indexShard0Stats = new IndexShardStats(
            new ShardId(indexName, "__na__", 0),
            new ShardStats[] {
                createShardStats(indexName, 0, true, TimeValue.timeValueMillis(2048).nanos(), TimeValue.timeValueMillis(1024).nanos()),
                createShardStats(indexName, 0, false, TimeValue.timeValueMillis(2048).nanos(), TimeValue.timeValueMillis(512).nanos()) }
        );

        // Shard 1 only has a replica available
        final IndexShardStats indexShard1Stats = new IndexShardStats(
            new ShardId(indexName, "__na__", 1),
            new ShardStats[] {
                createShardStats(indexName, 1, false, TimeValue.timeValueMillis(4096).nanos(), TimeValue.timeValueMillis(512).nanos()) }
        );
        // Shard 2 was not available

        when(response.getIndex(indexName)).thenReturn(indexStats);
        when(indexStats.getIndexShards()).thenReturn(Map.of(0, indexShard0Stats, 1, indexShard1Stats));

        // Shard 0 uses the results from the primary
        final IndexWriteLoad indexWriteLoadFromStats = IndexWriteLoad.fromStats(indexMetadata, response);
        assertThat(indexWriteLoadFromStats.getWriteLoadForShard(0).isPresent(), is(equalTo(true)));
        assertThat(indexWriteLoadFromStats.getWriteLoadForShard(0).getAsDouble(), is(equalTo(2.0)));
        assertThat(indexWriteLoadFromStats.getUptimeInMillisForShard(0).isPresent(), is(equalTo(true)));
        assertThat(indexWriteLoadFromStats.getUptimeInMillisForShard(0).getAsLong(), is(equalTo(1024L)));

        // Shard 1 uses the only available stats from a replica
        assertThat(indexWriteLoadFromStats.getWriteLoadForShard(1).isPresent(), is(equalTo(true)));
        assertThat(indexWriteLoadFromStats.getWriteLoadForShard(1).getAsDouble(), is(equalTo(8.0)));
        assertThat(indexWriteLoadFromStats.getUptimeInMillisForShard(1).isPresent(), is(equalTo(true)));
        assertThat(indexWriteLoadFromStats.getUptimeInMillisForShard(1).getAsLong(), is(equalTo(512L)));

        assertThat(indexWriteLoadFromStats.getWriteLoadForShard(2).isPresent(), is(equalTo(false)));
        assertThat(indexWriteLoadFromStats.getUptimeInMillisForShard(2).isPresent(), is(equalTo(false)));

        assertThat(IndexWriteLoad.fromStats(indexMetadata, null), is(nullValue()));
    }

    private ShardStats createShardStats(
        String indexName,
        int shard,
        boolean primary,
        long totalIndexingTimeSinceShardStartedInNanos,
        long totalActiveTimeInNanos
    ) {
        RecoverySource recoverySource = primary
            ? RecoverySource.EmptyStoreRecoverySource.INSTANCE
            : RecoverySource.PeerRecoverySource.INSTANCE;
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            new ShardId(indexName, "__na__", shard),
            primary,
            recoverySource,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foo")
        );
        shardRouting = ShardRoutingHelper.initialize(shardRouting, UUIDs.randomBase64UUID());
        shardRouting = ShardRoutingHelper.moveToStarted(shardRouting);

        final CommonStats commonStats = new CommonStats(CommonStatsFlags.ALL);
        commonStats.getIndexing()
            .getTotal()
            .add(
                new IndexingStats.Stats(0, 0, 0, 0, 0, 0, 0, 0, false, 0, totalIndexingTimeSinceShardStartedInNanos, totalActiveTimeInNanos)
            );
        return new ShardStats(shardRouting, commonStats, null, null, null, null, null, false);
    }
}
