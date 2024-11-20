/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransformsCheckpointServiceTests extends ESTestCase {

    public void testExtractIndexCheckpoints() {
        Map<String, long[]> expectedCheckpoints = new HashMap<>();
        Set<String> indices = randomUserIndices();

        ShardStats[] shardStatsArray = createRandomShardStats(expectedCheckpoints, indices, false, false, false);

        Map<String, long[]> checkpoints = DefaultCheckpointProvider.extractIndexCheckPoints(shardStatsArray, indices, "");

        assertEquals(expectedCheckpoints.size(), checkpoints.size());
        assertEquals(expectedCheckpoints.keySet(), checkpoints.keySet());

        // low-level compare
        for (Entry<String, long[]> entry : expectedCheckpoints.entrySet()) {
            assertArrayEquals(entry.getValue(), checkpoints.get(entry.getKey()));
        }
    }

    public void testExtractIndexCheckpointsMissingSeqNoStats() {
        Map<String, long[]> expectedCheckpoints = new HashMap<>();
        Set<String> indices = randomUserIndices();

        ShardStats[] shardStatsArray = createRandomShardStats(expectedCheckpoints, indices, false, false, true);

        Map<String, long[]> checkpoints = DefaultCheckpointProvider.extractIndexCheckPoints(shardStatsArray, indices, "");

        assertEquals(expectedCheckpoints.size(), checkpoints.size());
        assertEquals(expectedCheckpoints.keySet(), checkpoints.keySet());

        // low-level compare
        for (Entry<String, long[]> entry : expectedCheckpoints.entrySet()) {
            assertArrayEquals(entry.getValue(), checkpoints.get(entry.getKey()));
        }
    }

    public void testExtractIndexCheckpointsLostPrimaries() {
        Map<String, long[]> expectedCheckpoints = new HashMap<>();
        Set<String> indices = randomUserIndices();

        ShardStats[] shardStatsArray = createRandomShardStats(expectedCheckpoints, indices, true, false, false);

        Map<String, long[]> checkpoints = DefaultCheckpointProvider.extractIndexCheckPoints(shardStatsArray, indices, "");

        assertEquals(expectedCheckpoints.size(), checkpoints.size());
        assertEquals(expectedCheckpoints.keySet(), checkpoints.keySet());

        // low-level compare
        for (Entry<String, long[]> entry : expectedCheckpoints.entrySet()) {
            assertArrayEquals(entry.getValue(), checkpoints.get(entry.getKey()));
        }
    }

    public void testExtractIndexCheckpointsInconsistentGlobalCheckpoints() {
        Map<String, long[]> expectedCheckpoints = new HashMap<>();
        Set<String> indices = randomUserIndices();

        ShardStats[] shardStatsArray = createRandomShardStats(expectedCheckpoints, indices, randomBoolean(), true, false);

        Map<String, long[]> checkpoints = DefaultCheckpointProvider.extractIndexCheckPoints(shardStatsArray, indices, "");

        assertEquals(expectedCheckpoints.size(), checkpoints.size());
        assertEquals(expectedCheckpoints.keySet(), checkpoints.keySet());

        // global checkpoints should be max() of all global checkpoints
        for (Entry<String, long[]> entry : expectedCheckpoints.entrySet()) {
            assertArrayEquals(entry.getValue(), checkpoints.get(entry.getKey()));
        }
    }

    public void testTransformCheckpointingInfoWithZeroLastCheckpoint() {
        var transformState = mock(TransformState.class);
        when(transformState.getCheckpoint()).thenReturn(0L);
        var position = mock(TransformIndexerPosition.class);
        when(transformState.getPosition()).thenReturn(position);
        var progress = mock(TransformProgress.class);
        when(transformState.getProgress()).thenReturn(progress);

        var checkpointingInfo = TransformCheckpointService.deriveBasicCheckpointingInfo(transformState);

        assertEquals(checkpointingInfo.getLast().getCheckpoint(), 0L);
        assertEquals(checkpointingInfo.getNext().getCheckpoint(), 0L);
        assertSame(checkpointingInfo.getNext().getPosition(), position);
        assertSame(checkpointingInfo.getNext().getCheckpointProgress(), progress);
    }

    public void testTransformCheckpointingInfoWithNonZeroLastCheckpoint() {
        var transformState = mock(TransformState.class);
        when(transformState.getCheckpoint()).thenReturn(1L);
        var position = mock(TransformIndexerPosition.class);
        when(transformState.getPosition()).thenReturn(position);
        var progress = mock(TransformProgress.class);
        when(transformState.getProgress()).thenReturn(progress);

        var checkpointingInfo = TransformCheckpointService.deriveBasicCheckpointingInfo(transformState);

        assertEquals(checkpointingInfo.getLast().getCheckpoint(), 1L);
        assertEquals(checkpointingInfo.getNext().getCheckpoint(), 2L);
        assertSame(checkpointingInfo.getNext().getPosition(), position);
        assertSame(checkpointingInfo.getNext().getCheckpointProgress(), progress);
    }

    /**
     * Create a random set of 3 index names
     * @return set of indices a simulated user has access to
     */
    private static Set<String> randomUserIndices() {
        Set<String> indices = new HashSet<>();

        // never create an empty set
        if (randomBoolean()) {
            indices.add("index-1");
        } else {
            indices.add("index-2");
        }
        if (randomBoolean()) {
            indices.add("index-3");
        }
        return indices;
    }

    /**
     * create a ShardStats for testing with random fuzzing
     *
     * @param expectedCheckpoints output parameter to return the checkpoints to expect
     * @param userIndices set of indices that are visible
     * @param skipPrimaries whether some shards do not have a primary shard at random
     * @param inconsistentGlobalCheckpoints whether to introduce inconsistent global checkpoints
     * @param missingSeqNoStats whether some indices miss SeqNoStats
     * @return array of ShardStats
     */
    private static ShardStats[] createRandomShardStats(
        Map<String, long[]> expectedCheckpoints,
        Set<String> userIndices,
        boolean skipPrimaries,
        boolean inconsistentGlobalCheckpoints,
        boolean missingSeqNoStats
    ) {

        // always create the full list
        List<Index> indices = new ArrayList<>();
        indices.add(new Index("index-1", UUIDs.randomBase64UUID(random())));
        indices.add(new Index("index-2", UUIDs.randomBase64UUID(random())));
        indices.add(new Index("index-3", UUIDs.randomBase64UUID(random())));

        String missingSeqNoStatsIndex = randomFrom(userIndices);

        List<ShardStats> shardStats = new ArrayList<>();
        for (final Index index : indices) {
            int numShards = randomIntBetween(1, 5);

            List<Long> checkpoints = new ArrayList<>();
            for (int shardIndex = 0; shardIndex < numShards; shardIndex++) {
                // we need at least one replica for testing
                int numShardCopies = randomIntBetween(2, 4);

                int primaryShard = 0;
                if (skipPrimaries) {
                    primaryShard = randomInt(numShardCopies - 1);
                }
                int inconsistentReplica = -1;
                if (inconsistentGlobalCheckpoints) {
                    List<Integer> replicas = new ArrayList<>(numShardCopies - 1);
                    for (int i = 0; i < numShardCopies; i++) {
                        if (primaryShard != i) {
                            replicas.add(i);
                        }
                    }
                    inconsistentReplica = randomFrom(replicas);
                }

                // SeqNoStats asserts that checkpoints are logical
                long localCheckpoint = randomLongBetween(100L, 100000000L);
                long globalCheckpoint = randomBoolean() ? localCheckpoint : randomLongBetween(100L, 100000000L);
                long maxSeqNo = Math.max(localCheckpoint, globalCheckpoint);

                SeqNoStats validSeqNoStats = null;

                // add broken seqNoStats if requested
                if (missingSeqNoStats && index.getName().equals(missingSeqNoStatsIndex)) {
                    checkpoints.add(-1L);
                } else {
                    validSeqNoStats = new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpoint);
                    checkpoints.add(globalCheckpoint);
                }

                for (int replica = 0; replica < numShardCopies; replica++) {
                    ShardId shardId = new ShardId(index, shardIndex);
                    boolean primary = (replica == primaryShard);

                    Path path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(shardIndex));
                    ShardRouting shardRouting = ShardRouting.newUnassigned(
                        shardId,
                        primary,
                        primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : PeerRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
                        ShardRouting.Role.DEFAULT
                    );
                    shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                    shardRouting = shardRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

                    CommonStats stats = new CommonStats();
                    stats.fieldData = new FieldDataStats();
                    stats.queryCache = new QueryCacheStats();
                    stats.docs = new DocsStats();
                    stats.store = new StoreStats();
                    stats.indexing = new IndexingStats();
                    stats.search = new SearchStats();
                    stats.segments = new SegmentsStats();
                    stats.merge = new MergeStats();
                    stats.refresh = new RefreshStats();
                    stats.completion = new CompletionStats();
                    stats.requestCache = new RequestCacheStats();
                    stats.get = new GetStats();
                    stats.flush = new FlushStats();
                    stats.warmer = new WarmerStats();

                    if (inconsistentReplica == replica) {
                        // overwrite
                        SeqNoStats invalidSeqNoStats = new SeqNoStats(
                            maxSeqNo,
                            localCheckpoint,
                            globalCheckpoint - randomLongBetween(10L, 100L)
                        );
                        shardStats.add(
                            new ShardStats(
                                shardRouting,
                                new ShardPath(false, path, path, shardId),
                                stats,
                                null,
                                invalidSeqNoStats,
                                null,
                                false,
                                0
                            )
                        );
                    } else {
                        shardStats.add(
                            new ShardStats(
                                shardRouting,
                                new ShardPath(false, path, path, shardId),
                                stats,
                                null,
                                validSeqNoStats,
                                null,
                                false,
                                0
                            )
                        );
                    }
                }
            }

            if (userIndices.contains(index.getName())) {
                expectedCheckpoints.put(index.getName(), checkpoints.stream().mapToLong(l -> l).toArray());
            }
        }
        // shuffle the shard stats
        Collections.shuffle(shardStats, random());
        ShardStats[] shardStatsArray = shardStats.toArray(new ShardStats[0]);
        return shardStatsArray;
    }

}
