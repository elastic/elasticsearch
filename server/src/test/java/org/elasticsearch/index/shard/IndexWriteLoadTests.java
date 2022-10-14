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
import org.elasticsearch.test.ESTestCase;

import java.util.List;
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
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId), is(equalTo(populatedShardWriteLoads[shardId])));
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId), is(populatedShardUptimes[shardId]));
            } else {
                double fallbackWriteLoadValue = randomDoubleBetween(1, 128, true);
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId), is(nullValue()));
                assertThat(indexWriteLoad.getWriteLoadForShard(shardId, fallbackWriteLoadValue), is(equalTo(fallbackWriteLoadValue)));

                long fallbackUptimeValue = randomNonNegativeLong();
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId), is(nullValue()));
                assertThat(indexWriteLoad.getUptimeInMillisForShard(shardId, fallbackUptimeValue), is(equalTo(fallbackUptimeValue)));
            }
        }

        expectThrows(IllegalArgumentException.class, () -> indexWriteLoad.getWriteLoadForShard(-1));
        expectThrows(IllegalArgumentException.class, () -> indexWriteLoad.getWriteLoadForShard(numberOfShards + 1));
        expectThrows(IllegalArgumentException.class, () -> indexWriteLoad.getUptimeInMillisForShard(-1));
        expectThrows(IllegalArgumentException.class, () -> indexWriteLoad.getUptimeInMillisForShard(numberOfShards + 1));
    }

    public void testValidations() {
        expectThrows(IllegalArgumentException.class, () -> IndexWriteLoad.builder(randomIntBetween(-100, 0)));
        expectThrows(IllegalArgumentException.class, () -> IndexWriteLoad.create(List.of(), List.of()));
        expectThrows(IllegalArgumentException.class, () -> IndexWriteLoad.create(List.of(2.0, 3.0), List.of(1L)));
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
            new ShardStats[] { createShardStats(indexName, 0, true, 128, 1024), createShardStats(indexName, 0, false, 126, 512) }
        );

        // Shard 1 only has a replica available
        final IndexShardStats indexShard1Stats = new IndexShardStats(
            new ShardId(indexName, "__na__", 1),
            new ShardStats[] { createShardStats(indexName, 1, false, 256, 512) }
        );
        // Shard 2 was not available

        when(response.getIndex(indexName)).thenReturn(indexStats);
        when(indexStats.getIndexShards()).thenReturn(Map.of(0, indexShard0Stats, 1, indexShard1Stats));

        // Shard 0 uses the results from the primary
        final IndexWriteLoad indexWriteLoadFromStats = IndexWriteLoad.fromStats(indexMetadata, response);
        assertThat(indexWriteLoadFromStats.getWriteLoadForShard(0), is(equalTo(128.0)));
        assertThat(indexWriteLoadFromStats.getUptimeInMillisForShard(0), is(equalTo(1024L)));

        // Shard 1 uses the only available stats from a replica
        assertThat(indexWriteLoadFromStats.getWriteLoadForShard(1), is(equalTo(256.0)));
        assertThat(indexWriteLoadFromStats.getUptimeInMillisForShard(1), is(equalTo(512L)));

        assertThat(indexWriteLoadFromStats.getWriteLoadForShard(2), is(nullValue()));
        assertThat(indexWriteLoadFromStats.getUptimeInMillisForShard(2), is(nullValue()));

        assertThat(IndexWriteLoad.fromStats(indexMetadata, null), is(nullValue()));
    }

    private ShardStats createShardStats(String indexName, int shard, boolean primary, double writeLoad, long totalActiveTimeInMillis) {
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
            .add(new IndexingStats.Stats(0, 0, 0, 0, 0, 0, 0, 0, false, 0, writeLoad, totalActiveTimeInMillis));
        return new ShardStats(shardRouting, commonStats, null, null, null, null, null, false);
    }
}
