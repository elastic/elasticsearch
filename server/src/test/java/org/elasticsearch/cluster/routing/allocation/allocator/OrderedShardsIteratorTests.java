/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
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
import static org.hamcrest.Matchers.contains;
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

        var iterator = OrderedShardsIterator.createForNecessaryMoves(
            createRoutingAllocation(nodes, createMetadata(routing), routing),
            ordering
        );

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

        var iterator = OrderedShardsIterator.createForNecessaryMoves(
            createRoutingAllocation(nodes, createMetadata(routing), routing),
            ordering
        );

        var first = iterator.next();
        assertThat(first, anyOf(isIndexShardAt("index-1a", "node-1"), isIndexShardAt("index-1b", "node-1")));
        iterator.dePrioritizeNode(first.currentNodeId());// this should move remaining shard from node-1 to the last place
        assertThat(iterator.next(), isIndexShardAt("index-3", "node-3"));// oldest allocation
        assertThat(iterator.next(), isIndexShardAt("index-2", "node-2"));// newer allocation
        var last = iterator.next();
        assertThat(last, allOf(anyOf(isIndexShardAt("index-1a", "node-1"), isIndexShardAt("index-1b", "node-1")), not(equalTo(first))));
        assertThat(iterator.hasNext(), equalTo(false));
    }

    public void testShouldOrderShardByPriority() {

        var nodes = DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).build();

        IndexMetadata lookup = IndexMetadata.builder("lookup").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        IndexMetadata ds1 = IndexMetadata.builder(".ds-data-stream-2024.04.18-000001")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .build();
        IndexMetadata ds2 = IndexMetadata.builder(".ds-data-stream-2024.04.18-000002")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .build();

        var metadata = Metadata.builder()
            .put(lookup, false)
            .put(ds1, false)
            .put(ds2, false)
            .put(DataStream.builder("data-stream", List.of(ds1.getIndex(), ds2.getIndex())).build())
            .build();

        var routing = RoutingTable.builder()
            .add(index(lookup.getIndex(), "node-1"))
            .add(index(ds1.getIndex(), "node-1"))
            .add(index(ds2.getIndex(), "node-1"))
            .build();

        // when performing necessary moves (such as preparation for the node shutdown) write shards should be moved first
        assertThat(
            next(
                3,
                OrderedShardsIterator.createForNecessaryMoves(
                    createRoutingAllocation(nodes, metadata, routing),
                    new NodeAllocationOrdering()
                )
            ),
            contains(
                isIndexShardAt(".ds-data-stream-2024.04.18-000002", "node-1"),
                isIndexShardAt("lookup", "node-1"),
                isIndexShardAt(".ds-data-stream-2024.04.18-000001", "node-1")
            )
        );

        // when performing rebalancing write shards should be moved last
        assertThat(
            next(
                3,
                OrderedShardsIterator.createForBalancing(createRoutingAllocation(nodes, metadata, routing), new NodeAllocationOrdering())
            ),
            contains(
                isIndexShardAt(".ds-data-stream-2024.04.18-000001", "node-1"),
                isIndexShardAt("lookup", "node-1"),
                isIndexShardAt(".ds-data-stream-2024.04.18-000002", "node-1")
            )
        );
    }

    public void testShouldOrderShardByPriorityAcrossMultipleProjects() {

        var nodes = DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).build();

        IndexMetadata lookup = IndexMetadata.builder("lookup")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
            .build();
        IndexMetadata ds1 = IndexMetadata.builder(".ds-data-stream-2024.04.18-000001")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
            .build();
        IndexMetadata ds2 = IndexMetadata.builder(".ds-data-stream-2024.04.18-000002")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, randomUUID()))
            .build();

        // "lookup" index in project 1, data streams in project 2
        var metadata = Metadata.builder()
            .put(ProjectMetadata.builder(randomUniqueProjectId()).put(lookup, false))
            .put(
                ProjectMetadata.builder(randomUniqueProjectId())
                    .put(ds1, false)
                    .put(ds2, false)
                    .put(DataStream.builder("data-stream", List.of(ds1.getIndex(), ds2.getIndex())).build())
            )
            .build();

        var routing = GlobalRoutingTableTestHelper.buildRoutingTable(
            metadata,
            (builder, index) -> builder.add(index(index.getIndex(), "node-1"))
        );

        // when performing necessary moves (such as preparation for the node shutdown) write shards should be moved first
        assertThat(
            next(
                3,
                OrderedShardsIterator.createForNecessaryMoves(
                    createRoutingAllocation(nodes, metadata, routing),
                    new NodeAllocationOrdering()
                )
            ),
            contains(
                isIndexShardAt(".ds-data-stream-2024.04.18-000002", "node-1"),
                isIndexShardAt("lookup", "node-1"),
                isIndexShardAt(".ds-data-stream-2024.04.18-000001", "node-1")
            )
        );

        // when performing rebalancing write shards should be moved last
        assertThat(
            next(
                3,
                OrderedShardsIterator.createForBalancing(createRoutingAllocation(nodes, metadata, routing), new NodeAllocationOrdering())
            ),
            contains(
                isIndexShardAt(".ds-data-stream-2024.04.18-000001", "node-1"),
                isIndexShardAt("lookup", "node-1"),
                isIndexShardAt(".ds-data-stream-2024.04.18-000002", "node-1")
            )
        );
    }

    private Metadata createMetadata(RoutingTable routingTable) {
        final ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        for (var idxRoutingTable : routingTable) {
            final String indexName = idxRoutingTable.getIndex().getName();
            final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(indexSettings(IndexVersion.current(), 1, 1))
                .build();
            project.put(indexMetadata, false);
        }
        return Metadata.builder().put(project).build();
    }

    private static RoutingAllocation createRoutingAllocation(DiscoveryNodes nodes, Metadata metadata, RoutingTable routing) {
        return new RoutingAllocation(
            new AllocationDeciders(List.of()),
            ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).routingTable(routing).build(),
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            0
        );
    }

    private static RoutingAllocation createRoutingAllocation(DiscoveryNodes nodes, Metadata metadata, GlobalRoutingTable routing) {
        return new RoutingAllocation(
            new AllocationDeciders(List.of()),
            ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).routingTable(routing).build(),
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            0
        );
    }

    private static IndexRoutingTable index(String indexName, String nodeId) {
        return index(new Index(indexName, "_na_"), nodeId);
    }

    private static IndexRoutingTable index(Index index, String nodeId) {
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
            public void describeTo(Description description) {
                description.appendText("[" + indexName + "][0], node[" + nodeId + "]");
            }
        };
    }
}
