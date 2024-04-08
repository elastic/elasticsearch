/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class OrderedShardsIteratorTests extends ESAllocationTestCase {

    public void testOrdersShardsAccordingToAllocationRecency() {

        var nodes = DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).add(newNode("node-3")).build();

        var routing = RoutingTable.builder()
            .add(index("index-1a", "node-1"))
            .add(index("index-1b", "node-1"))
            .add(index("index-2a", "node-2"))
            .add(index("index-2b", "node-2"))
            .add(index("index-3", "node-3"))
            .build();

        var ordering = new NodeAllocationOrdering();
        ordering.recordAllocation("node-1");

        var iterator = createOrderedShardsIterator(nodes, routing, ordering);

        // order within same priority is not defined
        // no recorded allocations first
        assertThat(
            next(3, iterator),
            containsInAnyOrder(
                isIndexShardAt("index-2a", "node-2"),
                isIndexShardAt("index-2b", "node-2"),
                isIndexShardAt("index-3", "node-3")
            )
        );
        // recently recorded allocations last
        assertThat(next(2, iterator), containsInAnyOrder(isIndexShardAt("index-1a", "node-1"), isIndexShardAt("index-1b", "node-1")));
        assertThat(iterator.hasNext(), equalTo(false));
    }

    public void testReOrdersShardDuringIteration() {

        var nodes = DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).add(newNode("node-3")).build();

        var routing = RoutingTable.builder()
            .add(index("index-1a", "node-1"))
            .add(index("index-1b", "node-1"))
            .add(index("index-2", "node-2"))
            .add(index("index-3", "node-3"))
            .build();

        var ordering = new NodeAllocationOrdering();
        ordering.recordAllocation("node-3");
        ordering.recordAllocation("node-2");

        var iterator = createOrderedShardsIterator(nodes, routing, ordering);

        var first = iterator.next();
        assertThat(first, anyOf(isIndexShardAt("index-1a", "node-1"), isIndexShardAt("index-1b", "node-1")));
        iterator.dePrioritizeNode(first.currentNodeId());// this should move remaining shard from node-1 to the last place
        assertThat(iterator.next(), isIndexShardAt("index-3", "node-3"));// oldest allocation
        assertThat(iterator.next(), isIndexShardAt("index-2", "node-2"));// newer allocation
        var last = iterator.next();
        assertThat(last, allOf(anyOf(isIndexShardAt("index-1a", "node-1"), isIndexShardAt("index-1b", "node-1")), not(equalTo(first))));
        assertThat(iterator.hasNext(), equalTo(false));
    }

    private OrderedShardsIterator createOrderedShardsIterator(DiscoveryNodes nodes, RoutingTable routing, NodeAllocationOrdering ordering) {
        var routingNodes = randomBoolean() ? RoutingNodes.mutable(routing, nodes) : RoutingNodes.immutable(routing, nodes);
        return OrderedShardsIterator.create(routingNodes, ordering);
    }

    private static IndexRoutingTable index(String indexName, String nodeId) {
        var index = new Index(indexName, "_na_");
        return IndexRoutingTable.builder(index).addShard(newShardRouting(new ShardId(index, 0), nodeId, true, STARTED)).build();
    }

    private static <T> List<T> next(int size, Iterator<T> iterator) {
        var list = new ArrayList<T>(size);
        for (int i = 0; i < size; i++) {
            assertThat(iterator.hasNext(), equalTo(true));
            list.add(iterator.next());
        }
        return list;
    }

    private static TypeSafeMatcher<ShardRouting> isIndexShardAt(String indexName, String nodeId) {
        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(ShardRouting item) {
                return Objects.equals(item.shardId().getIndexName(), indexName) && Objects.equals(item.currentNodeId(), nodeId);
            }

            @Override
            public void describeTo(Description description) {}
        };
    }
}
