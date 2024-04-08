/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RoutingNodeTests extends ESTestCase {
    private ShardRouting unassignedShard0 = TestShardRouting.newShardRouting("test", 0, "node-1", false, ShardRoutingState.STARTED);
    private ShardRouting initializingShard0 = TestShardRouting.newShardRouting("test", 1, "node-1", false, ShardRoutingState.INITIALIZING);
    private ShardRouting relocatingShard0 = TestShardRouting.newShardRouting(
        "test",
        2,
        "node-1",
        "node-2",
        false,
        ShardRoutingState.RELOCATING
    );
    private RoutingNode routingNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        InetAddress inetAddress = InetAddress.getByAddress("name1", new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("name1", "node-1", transportAddress, emptyMap(), emptySet());
        routingNode = RoutingNodesHelper.routingNode("node1", discoveryNode, unassignedShard0, initializingShard0, relocatingShard0);
    }

    public void testAdd() {
        ShardRouting initializingShard1 = TestShardRouting.newShardRouting("test", 3, "node-1", false, ShardRoutingState.INITIALIZING);
        ShardRouting relocatingShard0 = TestShardRouting.newShardRouting(
            "test",
            4,
            "node-1",
            "node-2",
            false,
            ShardRoutingState.RELOCATING
        );
        routingNode.add(initializingShard1);
        routingNode.add(relocatingShard0);
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 3)), equalTo(initializingShard1));
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 4)), equalTo(relocatingShard0));
    }

    public void testUpdate() {
        ShardRouting startedShard0 = TestShardRouting.newShardRouting("test", 0, "node-1", false, ShardRoutingState.STARTED);
        ShardRouting startedShard1 = TestShardRouting.newShardRouting("test", 1, "node-1", "node-2", false, ShardRoutingState.RELOCATING);
        ShardRouting startedShard2 = TestShardRouting.newShardRouting("test", 2, "node-1", false, ShardRoutingState.INITIALIZING);
        routingNode.update(unassignedShard0, startedShard0);
        routingNode.update(initializingShard0, startedShard1);
        routingNode.update(relocatingShard0, startedShard2);
        assertThat(
            routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 0)).state(),
            equalTo(ShardRoutingState.STARTED)
        );
        assertThat(
            routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 1)).state(),
            equalTo(ShardRoutingState.RELOCATING)
        );
        assertThat(
            routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 2)).state(),
            equalTo(ShardRoutingState.INITIALIZING)
        );
    }

    public void testRemove() {
        routingNode.remove(unassignedShard0);
        routingNode.remove(initializingShard0);
        routingNode.remove(relocatingShard0);
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 0)), is(nullValue()));
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 1)), is(nullValue()));
        assertThat(routingNode.getByShardId(new ShardId("test", IndexMetadata.INDEX_UUID_NA_VALUE, 2)), is(nullValue()));
    }

    public void testNumberOfShardsWithState() {
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(1));
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.RELOCATING), equalTo(1));
        assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));
    }

    public void testShardsWithState() {
        assertThat(routingNode.shardsWithState(ShardRoutingState.STARTED).count(), equalTo(1L));
        assertThat(routingNode.shardsWithState(ShardRoutingState.RELOCATING).count(), equalTo(1L));
        assertThat(routingNode.shardsWithState(ShardRoutingState.INITIALIZING).count(), equalTo(1L));
    }

    public void testShardsWithStateInIndex() {
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED).count(), equalTo(2L));
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.STARTED).count(), equalTo(1L));
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.RELOCATING).count(), equalTo(1L));
        assertThat(routingNode.shardsWithState("test", ShardRoutingState.INITIALIZING).count(), equalTo(1L));
    }

    public void testNumberOfOwningShards() {
        assertThat(routingNode.numberOfOwningShards(), equalTo(2));
    }

    public void testNumberOfOwningShardsForIndex() {
        final ShardRouting test1Shard0 = TestShardRouting.newShardRouting("test1", 0, "node-1", false, ShardRoutingState.STARTED);
        final ShardRouting test2Shard0 = TestShardRouting.newShardRouting(
            "test2",
            0,
            "node-1",
            "node-2",
            false,
            ShardRoutingState.RELOCATING
        );
        routingNode.add(test1Shard0);
        routingNode.add(test2Shard0);
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index("test", IndexMetadata.INDEX_UUID_NA_VALUE)), equalTo(2));
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index("test1", IndexMetadata.INDEX_UUID_NA_VALUE)), equalTo(1));
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index("test2", IndexMetadata.INDEX_UUID_NA_VALUE)), equalTo(0));
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index("test3", IndexMetadata.INDEX_UUID_NA_VALUE)), equalTo(0));
    }

    public void testReturnStartedShards() {
        assertThat(startedShardsSet(routingNode), equalTo(Set.of(ShardId.fromString("[test][0]"))));

        ShardRouting startedShard = TestShardRouting.newShardRouting("test1", 1, "node-1", false, ShardRoutingState.STARTED);

        routingNode.add(startedShard);
        assertThat(startedShardsSet(routingNode), equalTo(Set.of(ShardId.fromString("[test][0]"), ShardId.fromString("[test1][1]"))));

        ShardRouting relocatingShard = TestShardRouting.newShardRouting(
            "test2",
            2,
            "node-1",
            "node-2",
            false,
            ShardRoutingState.RELOCATING
        );
        routingNode.add(relocatingShard);
        assertThat(startedShardsSet(routingNode), equalTo(Set.of(ShardId.fromString("[test][0]"), ShardId.fromString("[test1][1]"))));

        routingNode.remove(startedShard);
        assertThat(startedShardsSet(routingNode), equalTo(Set.of(ShardId.fromString("[test][0]"))));
    }

    private static Set<ShardId> startedShardsSet(RoutingNode routingNode) {
        return Arrays.stream(routingNode.started()).map(ShardRouting::shardId).collect(Collectors.toSet());
    }

}
