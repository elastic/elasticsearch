/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.routing.RoutingNodeTests.ParsedShardIdMatcher.equalsToParsedShardId;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RoutingNodeTests extends ESTestCase {
    private final Index index = new Index("test", randomUUID());
    private final ShardRouting unassignedShard0 = TestShardRouting.newShardRouting(index, 0, "node-1", false, ShardRoutingState.STARTED);
    private final ShardRouting initializingShard0 = TestShardRouting.newShardRouting(
        index,
        1,
        "node-1",
        false,
        ShardRoutingState.INITIALIZING
    );
    private final ShardRouting relocatingShard0 = TestShardRouting.newShardRouting(
        index,
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
        ShardId shard3Id = new ShardId(index, 3);
        ShardId shard4Id = new ShardId(index, 4);
        ShardRouting initializingShard1 = TestShardRouting.newShardRouting(shard3Id, "node-1", false, ShardRoutingState.INITIALIZING);
        ShardRouting relocatingShard0 = TestShardRouting.newShardRouting(shard4Id, "node-1", "node-2", false, ShardRoutingState.RELOCATING);
        routingNode.add(initializingShard1);
        routingNode.add(relocatingShard0);
        assertThat(routingNode.getByShardId(shard3Id), equalTo(initializingShard1));
        assertThat(routingNode.getByShardId(shard4Id), equalTo(relocatingShard0));
    }

    public void testUpdate() {
        ShardId shard0Id = new ShardId(index, 0);
        ShardId shard1Id = new ShardId(index, 1);
        ShardId shard2Id = new ShardId(index, 2);
        ShardRouting startedShard0 = TestShardRouting.newShardRouting(shard0Id, "node-1", false, ShardRoutingState.STARTED);
        ShardRouting startedShard1 = TestShardRouting.newShardRouting(shard1Id, "node-1", "node-2", false, ShardRoutingState.RELOCATING);
        ShardRouting startedShard2 = TestShardRouting.newShardRouting(shard2Id, "node-1", false, ShardRoutingState.INITIALIZING);
        routingNode.update(unassignedShard0, startedShard0);
        routingNode.update(initializingShard0, startedShard1);
        routingNode.update(relocatingShard0, startedShard2);
        assertThat(routingNode.getByShardId(shard0Id).state(), equalTo(ShardRoutingState.STARTED));
        assertThat(routingNode.getByShardId(shard1Id).state(), equalTo(ShardRoutingState.RELOCATING));
        assertThat(routingNode.getByShardId(shard2Id).state(), equalTo(ShardRoutingState.INITIALIZING));
    }

    public void testRemove() {
        routingNode.remove(unassignedShard0);
        routingNode.remove(initializingShard0);
        routingNode.remove(relocatingShard0);
        assertThat(routingNode.getByShardId(new ShardId(index, 0)), is(nullValue()));
        assertThat(routingNode.getByShardId(new ShardId(index, 1)), is(nullValue()));
        assertThat(routingNode.getByShardId(new ShardId(index, 2)), is(nullValue()));
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
        assertThat(
            routingNode.shardsWithState(index.getName(), ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED).count(),
            equalTo(2L)
        );
        assertThat(routingNode.shardsWithState(index.getName(), ShardRoutingState.STARTED).count(), equalTo(1L));
        assertThat(routingNode.shardsWithState(index.getName(), ShardRoutingState.RELOCATING).count(), equalTo(1L));
        assertThat(routingNode.shardsWithState(index.getName(), ShardRoutingState.INITIALIZING).count(), equalTo(1L));
    }

    public void testNumberOfOwningShards() {
        assertThat(routingNode.numberOfOwningShards(), equalTo(2));
    }

    public void testNumberOfOwningShardsForIndex() {
        Index index1 = new Index("test1", randomUUID());
        Index index2 = new Index("test2", randomUUID());
        final ShardRouting test1Shard0 = TestShardRouting.newShardRouting(index1, 0, "node-1", false, ShardRoutingState.STARTED);
        final ShardRouting test2Shard0 = TestShardRouting.newShardRouting(
            index2,
            0,
            "node-1",
            "node-2",
            false,
            ShardRoutingState.RELOCATING
        );
        routingNode.add(test1Shard0);
        routingNode.add(test2Shard0);
        assertThat(routingNode.numberOfOwningShardsForIndex(index), equalTo(2));
        assertThat(routingNode.numberOfOwningShardsForIndex(index1), equalTo(1));
        assertThat(routingNode.numberOfOwningShardsForIndex(index2), equalTo(0));
        assertThat(routingNode.numberOfOwningShardsForIndex(new Index("test3", IndexMetadata.INDEX_UUID_NA_VALUE)), equalTo(0));
    }

    public void testReturnStartedShards() {
        Matcher<ShardId> shard0Matcher = equalsToParsedShardId("[test][0]");
        assertThat(startedShardsSet(routingNode), contains(shard0Matcher));

        ShardRouting anotherStartedShard = TestShardRouting.newShardRouting(
            "test1",
            randomUUID(),
            1,
            "node-1",
            false,
            ShardRoutingState.STARTED
        );

        routingNode.add(anotherStartedShard);
        Matcher<ShardId> shard1Matcher = equalsToParsedShardId("[test1][1]");
        assertThat(startedShardsSet(routingNode), containsInAnyOrder(shard0Matcher, shard1Matcher));

        ShardRouting relocatingShard = TestShardRouting.newShardRouting(
            "test2",
            IndexMetadata.INDEX_UUID_NA_VALUE,
            2,
            "node-1",
            "node-2",
            false,
            ShardRoutingState.RELOCATING
        );
        routingNode.add(relocatingShard);
        assertThat(startedShardsSet(routingNode), containsInAnyOrder(shard0Matcher, shard1Matcher));

        routingNode.remove(anotherStartedShard);
        assertThat(startedShardsSet(routingNode), containsInAnyOrder(shard0Matcher));
    }

    private static Set<ShardId> startedShardsSet(RoutingNode routingNode) {
        return Arrays.stream(routingNode.started()).map(ShardRouting::shardId).collect(Collectors.toSet());
    }

    /**
     * When parsing from string, we lose uuid information, this is why we do not check shard id equality.
     */
    static class ParsedShardIdMatcher extends TypeSafeMatcher<ShardId> {

        private final ShardId expectedShardId;

        private ParsedShardIdMatcher(ShardId expectedShardId) {
            this.expectedShardId = expectedShardId;
        }

        @Override
        protected boolean matchesSafely(ShardId shardId) {
            return expectedShardId.getIndexName().equals(shardId.getIndexName()) && expectedShardId.getId() == shardId.getId();
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("equals to " + expectedShardId.toString());
        }

        static Matcher<ShardId> equalsToParsedShardId(String shardId) {
            return new ParsedShardIdMatcher(ShardId.fromString(shardId));
        }
    }

}
