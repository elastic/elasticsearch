/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.watch.Watch;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ShardAllocationConfigurationTests extends ESTestCase {

    public void testHostsWatchIndex() {
        final var shard0 = new ShardAllocationConfiguration(0, 3, List.of("a", "b", "c"));
        final var shard1 = new ShardAllocationConfiguration(1, 3, List.of("a", "b", "c"));
        final var shard2 = new ShardAllocationConfiguration(2, 3, List.of("a", "b", "c"));

        for (int i = 0; i < 200; i++) {
            final var id = "watch_" + i;
            final int trueCount = (shard0.hostsWatch(id) ? 1 : 0) + (shard1.hostsWatch(id) ? 1 : 0) + (shard2.hostsWatch(id) ? 1 : 0);
            assertThat("Watch [" + id + "] must be triggered exactly once across shards", trueCount, is(1));
        }
    }

    /**
     * No matter how many shards exist, every watch id must be claimed by exactly one shard. The hash distribution
     * is expected to be reasonably uniform, but the contract under test here is correctness, not balance.
     */
    public void testHostsWatchExactlyOnceAcrossAllShards() {
        final var numberOfShards = randomIntBetween(1, 20);
        final var numberOfDocuments = randomIntBetween(1, 10_000);
        final var triggered = new BitSet(numberOfDocuments);

        for (int currentShardId = 0; currentShardId < numberOfShards; currentShardId++) {
            final var shardAllocationConfig = new ShardAllocationConfiguration(currentShardId, numberOfShards, List.of());
            for (int i = 0; i < numberOfDocuments; i++) {
                if (shardAllocationConfig.hostsWatch("watch_" + i)) {
                    assertThat("Watch [" + i + "] has already been triggered", triggered.get(i), is(false));
                    triggered.set(i);
                }
            }
        }
        assertThat(triggered.cardinality(), is(numberOfDocuments));
    }

    public void testIsAllocatedToCurrentShardIsDeterministic() {
        final var shardAllocationConfiguration = new ShardAllocationConfiguration(2, 5, List.of());
        for (int i = 0; i < 50; i++) {
            final String id = randomAlphaOfLengthBetween(5, 20);
            assertThat(shardAllocationConfiguration.hostsWatch(id), is(shardAllocationConfiguration.hostsWatch(id)));
        }
    }

    public void testForLocalShardsWithSinglePrimaryStarted() {
        final var index = new Index(Watch.INDEX, "uuid");
        final var shardId = new ShardId(index, 0);
        final ShardRoutingState state = randomFrom(STARTED, RELOCATING);
        final ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            "local-node",
            state == RELOCATING ? "target-node" : null,
            true,
            state
        );
        final IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).build();

        final Map<ShardId, ShardAllocationConfiguration> result = ShardAllocationConfiguration.forLocalShards(
            List.of(shardRouting),
            indexRoutingTable
        );

        assertThat(result, aMapWithSize(1));
        final ShardAllocationConfiguration config = result.get(shardId);
        assertThat(config.index(), is(0));
        assertThat(config.shardCount(), is(1));
        assertThat(config.allocationIds(), contains(shardRouting.allocationId().getId()));
    }

    public void testForLocalShardsReturnsEmptyMapWhenNoLocalShards() {
        final var index = new Index(Watch.INDEX, "uuid");
        final var shardId = new ShardId(index, 0);
        // shard exists in the cluster but is hosted elsewhere
        final ShardRouting otherNodesShard = TestShardRouting.newShardRouting(shardId, "other-node", true, STARTED);
        final IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(otherNodesShard).build();

        final Map<ShardId, ShardAllocationConfiguration> result = ShardAllocationConfiguration.forLocalShards(List.of(), indexRoutingTable);

        assertThat(result, is(anEmptyMap()));
    }

    public void testForLocalShardsWithMultipleLocalPrimaries() {
        final var index = new Index(Watch.INDEX, "uuid");
        final var shard0 = new ShardId(index, 0);
        final var shard1 = new ShardId(index, 1);

        final List<ShardRouting> localShards = List.of(
            TestShardRouting.newShardRouting(shard0, "node1", true, STARTED),
            TestShardRouting.newShardRouting(shard1, "node1", true, STARTED)
        );

        final IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(localShards.get(0))
            .addShard(localShards.get(1))
            .addShard(TestShardRouting.newShardRouting(shard0, "node2", false, STARTED))
            .addShard(TestShardRouting.newShardRouting(shard1, "node2", false, STARTED))
            .build();

        final Map<ShardId, ShardAllocationConfiguration> result = ShardAllocationConfiguration.forLocalShards(
            localShards,
            indexRoutingTable
        );

        assertThat(result, aMapWithSize(2));
        // each shard sees two active copies (primary + replica)
        assertThat(result.get(shard0).shardCount(), is(2));
        assertThat(result.get(shard1).shardCount(), is(2));
        assertThat(result.get(shard0).allocationIds(), contains(sortedAllocationIds(indexRoutingTable, 0).toArray(String[]::new)));
    }

    /**
     * The {@code index} field is the position of the local allocation id within the sorted list of active allocation
     * ids for that shard. Sorting keeps the partitioning stable across nodes so the same watch id maps to the same
     * single owner regardless of which node observes the routing table.
     */
    public void testForLocalShardsAssignsIndexFromSortedAllocationIds() {
        final var index = new Index(Watch.INDEX, "uuid");
        final var shardId = new ShardId(index, 0);

        final ShardRouting primary = TestShardRouting.newShardRouting(shardId, "local-node", true, STARTED);
        final ShardRouting replica1 = TestShardRouting.newShardRouting(shardId, "other-1", false, STARTED);
        final ShardRouting replica2 = TestShardRouting.newShardRouting(shardId, "other-2", false, STARTED);

        final IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(primary)
            .addShard(replica1)
            .addShard(replica2)
            .build();

        final Map<ShardId, ShardAllocationConfiguration> result = ShardAllocationConfiguration.forLocalShards(
            List.of(primary),
            indexRoutingTable
        );

        final ShardAllocationConfiguration config = result.get(shardId);
        assertThat(config, is(not(equalTo(null))));
        assertThat(config.shardCount(), is(3));
        assertThat(config.allocationIds(), contains(sortedAllocationIds(indexRoutingTable, 0).toArray(String[]::new)));
        // the local allocation id's position within the sorted list determines this shard copy's bucket
        final int expectedIndex = config.allocationIds().indexOf(primary.allocationId().getId());
        assertThat(config.index(), is(expectedIndex));
    }

    /**
     * Two nodes observing the same routing table must end up with two configurations that together cover every
     * watch id exactly once — otherwise the same watch would fire twice or not at all.
     */
    public void testForLocalShardsTwoCopiesPartitionWatchesExactlyOnce() {
        final var index = new Index(Watch.INDEX, "uuid");
        final var shardId = new ShardId(index, 0);

        final ShardRouting primary = TestShardRouting.newShardRouting(shardId, "node-a", true, STARTED);
        final ShardRouting replica = TestShardRouting.newShardRouting(shardId, "node-b", false, STARTED);
        final IndexRoutingTable routingTable = IndexRoutingTable.builder(index).addShard(primary).addShard(replica).build();

        final ShardAllocationConfiguration cfgA = ShardAllocationConfiguration.forLocalShards(List.of(primary), routingTable).get(shardId);
        final ShardAllocationConfiguration cfgB = ShardAllocationConfiguration.forLocalShards(List.of(replica), routingTable).get(shardId);

        assertThat(cfgA.shardCount(), is(2));
        assertThat(cfgB.shardCount(), is(2));
        assertThat("local indexes for the two copies must differ", cfgA.index(), is(not(cfgB.index())));

        for (int i = 0; i < 500; i++) {
            final var id = "watch_" + i;
            assertThat("Watch [" + id + "] must be triggered exactly once", cfgA.hostsWatch(id) ^ cfgB.hostsWatch(id), is(true));
        }
    }

    public void testForLocalShardsIgnoresInactiveCopies() {
        final var index = new Index(Watch.INDEX, "uuid");
        final var shardId = new ShardId(index, 0);

        final ShardRouting primary = TestShardRouting.newShardRouting(shardId, "local-node", true, STARTED);
        final ShardRouting unassignedReplica = ShardRouting.newUnassigned(
            shardId,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
            ShardRouting.Role.DEFAULT
        );

        final IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(primary).addShard(unassignedReplica).build();

        final Map<ShardId, ShardAllocationConfiguration> result = ShardAllocationConfiguration.forLocalShards(
            List.of(primary),
            indexRoutingTable
        );

        final ShardAllocationConfiguration config = result.get(shardId);
        // unassigned replica must not show up in either the count or the allocation ids list
        assertThat(config.shardCount(), is(1));
        assertThat(config.allocationIds(), contains(primary.allocationId().getId()));
    }

    public void testForLocalShardsReturnsConfigsForEveryLocalShard() {
        final var totalShards = randomIntBetween(2, 8);
        final var localShardCount = randomIntBetween(1, totalShards);

        final var index = new Index(Watch.INDEX, "uuid");
        final IndexRoutingTable.Builder routingBuilder = IndexRoutingTable.builder(index);
        final List<ShardRouting> localShards = new ArrayList<>(localShardCount);
        for (int i = 0; i < totalShards; i++) {
            final var shardId = new ShardId(index, i);
            final var node = i < localShardCount ? "local-node" : "remote-node";
            final ShardRouting routing = TestShardRouting.newShardRouting(shardId, node, true, STARTED);
            routingBuilder.addShard(routing);
            if (i < localShardCount) {
                localShards.add(routing);
            }
        }

        final Map<ShardId, ShardAllocationConfiguration> result = ShardAllocationConfiguration.forLocalShards(
            localShards,
            routingBuilder.build()
        );

        assertThat(result, aMapWithSize(localShardCount));
        for (final ShardRouting local : localShards) {
            final ShardAllocationConfiguration config = result.get(local.shardId());
            assertThat(config.shardCount(), is(1));
            assertThat(config.index(), is(0));
            assertThat(config.allocationIds(), contains(local.allocationId().getId()));
        }
    }

    public void testFindShardConfigReturnsSingleShardForAnyId() {
        // with numShards=1 every id must route to shard 0
        final var index = new Index(Watch.INDEX, "uuid");
        final var shard0 = new ShardId(index, 0);
        final ShardAllocationConfiguration config = new ShardAllocationConfiguration(0, 1, List.of("a"));
        final Map<ShardId, ShardAllocationConfiguration> shardConfigs = Map.of(shard0, config);

        for (int i = 0; i < 20; i++) {
            final var id = randomAlphaOfLengthBetween(1, 20);
            assertThat(ShardAllocationConfiguration.findShardConfig(shardConfigs, id, 1), is(config));
        }
    }

    public void testFindShardConfigReturnsNullWhenShardIsNotLocal() {
        // two-shard index; only shard 0 is local — ids that hash to shard 1 must return null
        final var index = new Index(Watch.INDEX, "uuid");
        final var shard0 = new ShardId(index, 0);
        final ShardAllocationConfiguration config0 = new ShardAllocationConfiguration(0, 2, List.of("a"));
        final Map<ShardId, ShardAllocationConfiguration> shardConfigs = Map.of(shard0, config0);

        // find an id that routes to shard 1 (not local)
        final String foreignId = idHashingToShard(1, 2);
        assertThat(ShardAllocationConfiguration.findShardConfig(shardConfigs, foreignId, 2), is(nullValue()));
    }

    public void testFindShardConfigPicksCorrectShardAmongMultiple() {
        // three-shard index; all three shards are local with distinct configs
        final var index = new Index(Watch.INDEX, "uuid");
        final var config0 = new ShardAllocationConfiguration(0, 3, List.of("a"));
        final var config1 = new ShardAllocationConfiguration(1, 3, List.of("a", "b"));
        final var config2 = new ShardAllocationConfiguration(2, 3, List.of("a", "b", "c"));
        final Map<ShardId, ShardAllocationConfiguration> shardConfigs = Map.of(
            new ShardId(index, 0),
            config0,
            new ShardId(index, 1),
            config1,
            new ShardId(index, 2),
            config2
        );

        assertThat(ShardAllocationConfiguration.findShardConfig(shardConfigs, idHashingToShard(0, 3), 3), is(config0));
        assertThat(ShardAllocationConfiguration.findShardConfig(shardConfigs, idHashingToShard(1, 3), 3), is(config1));
        assertThat(ShardAllocationConfiguration.findShardConfig(shardConfigs, idHashingToShard(2, 3), 3), is(config2));
    }

    public void testFindShardConfigReturnsNullForEmptyConfigs() {
        assertThat(ShardAllocationConfiguration.findShardConfig(Map.of(), "any-id", 1), is(nullValue()));
    }

    private static List<String> sortedAllocationIds(IndexRoutingTable indexRoutingTable, int shardId) {
        return indexRoutingTable.shard(shardId)
            .activeShards()
            .stream()
            .map(ShardRouting::allocationId)
            .map(AllocationId::getId)
            .sorted()
            .toList();
    }

    /** Brute-forces an id whose Murmur3 hash maps to {@code targetShard} out of {@code numShards}. */
    private static String idHashingToShard(int targetShard, int numShards) {
        for (int i = 0;; i++) {
            String id = "watch-" + i;
            if (Math.floorMod(Murmur3HashFunction.hash(id), numShards) == targetShard) {
                return id;
            }
        }
    }
}
