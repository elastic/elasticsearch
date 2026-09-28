/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;

import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchableSnapshotMissingSequenceNumbersIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    public void testMountPreservesSequenceNumberGapThroughRelocateAndReplica() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String firstDataNode = internalCluster().startDataOnlyNode();
        final String secondDataNode = internalCluster().startDataOnlyNode();

        final String indexName = "test-idx";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";

        createIndex(indexName, indexSettingsNoReplicas(1).put("index.routing.allocation.include._name", firstDataNode).build());
        ensureGreen(indexName);

        for (int i = 0; i < 5; i++) {
            indexDoc(indexName, Integer.toString(i), "foo", "bar" + i);
        }

        final Index index = resolveIndex(indexName);
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, firstDataNode).getShardOrNull(new ShardId(index, 0));
        // Advance max_seq_no without marking the seq no as processed → LCP lags behind max.
        EngineTestCase.generateNewSeqNo(primary.getEngineOrNull());

        flush(indexName);
        assertGappedSeqNoStats(indexName, 4L, 5L);

        createRepository(repoName, "fs");
        createSnapshot(repoName, snapshotName, List.of(indexName));
        assertAcked(indicesAdmin().prepareDelete(indexName));

        final String mountedIndex = mountSnapshot(
            repoName,
            snapshotName,
            indexName,
            Settings.builder().put("index.routing.allocation.include._name", firstDataNode).build()
        );
        ensureGreen(mountedIndex);
        // Mount uses ReadOnlyEngine without fillSeqNoGaps — the gap must remain.
        assertGappedSeqNoStats(mountedIndex, 4L, 5L);

        logger.info("--> relocate mounted primary from [{}] to [{}]", firstDataNode, secondDataNode);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.include._name", secondDataNode), mountedIndex);
        ensureGreen(mountedIndex);
        assertThat(getNodeId(secondDataNode), equalTo(getNodeIdForPrimaryShard(mountedIndex)));
        assertGappedSeqNoStats(mountedIndex, 4L, 5L);

        logger.info("--> add a replica so both copies carry the gapped commit");
        updateIndexSettings(
            Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 1).putNull("index.routing.allocation.include._name"),
            mountedIndex
        );
        ensureGreen(mountedIndex);
        assertGappedSeqNoStats(mountedIndex, 4L, 5L);
    }

    private void assertGappedSeqNoStats(String indexName, long expectedLocalCheckpoint, long expectedMaxSeqNo) {
        assertThat(expectedLocalCheckpoint, lessThan(expectedMaxSeqNo));
        final IndicesStatsResponse stats = indicesAdmin().prepareStats(indexName).clear().get();
        assertThat(stats.getShards().length, equalTo(getNumShards(indexName).totalNumShards));
        for (ShardStats shardStats : stats.getShards()) {
            assertThat(shardStats.getSeqNoStats().getLocalCheckpoint(), equalTo(expectedLocalCheckpoint));
            assertThat(shardStats.getSeqNoStats().getMaxSeqNo(), equalTo(expectedMaxSeqNo));
            assertThat(shardStats.getSeqNoStats().getGlobalCheckpoint(), equalTo(expectedLocalCheckpoint));
        }
    }

    private static String getNodeIdForPrimaryShard(String mountedIndex) {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable()
            .index(mountedIndex)
            .shard(0)
            .primaryShard()
            .currentNodeId();
    }
}
