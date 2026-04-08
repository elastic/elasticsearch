/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.allocator.NodeAllocationOrdering;
import org.elasticsearch.cluster.routing.allocation.allocator.OrderedShardsIterator;
import org.elasticsearch.cluster.routing.allocation.allocator.OrderedShardsIteratorTests;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardRelocationOrder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.elasticsearch.xpack.stateless.allocation.StatelessShardRelocationOrder.PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class StatelessShardRelocationOrderTests extends OrderedShardsIteratorTests {

    @Override
    protected ShardRelocationOrder createShardRelocationOrder() {
        // defaults to the same behaviour as the default shard relocation order
        return new StatelessShardRelocationOrder(getClusterSettings(true));
    }

    public void testOrderBalancingMoves() {
        ShardRelocationOrder relocationOrder = new StatelessShardRelocationOrder(getClusterSettings(false));
        var nodes = DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).build();

        IndexMetadata lookup = IndexMetadata.builder("lookup").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        IndexMetadata ds1 = IndexMetadata.builder(".ds-data-stream-2024.04.18-000001")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .build();
        IndexMetadata ds2 = IndexMetadata.builder(".ds-data-stream-2024.04.18-000002")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .build();

        var metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID)
                    .put(lookup, false)
                    .put(ds1, false)
                    .put(ds2, false)
                    .put(DataStream.builder("data-stream", List.of(ds1.getIndex(), ds2.getIndex())).build())
            )
            .build();

        var routing = GlobalRoutingTableTestHelper.buildRoutingTable(
            metadata,
            (builder, index) -> builder.add(index(index.getIndex(), "node-1"))
        );

        // when performing rebalancing write shards should be moved last
        assertThat(
            next(
                3,
                OrderedShardsIterator.createForBalancing(
                    TestRoutingAllocationFactory.forClusterState(
                        ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).routingTable(routing).build()
                    ).build(),
                    new NodeAllocationOrdering(),
                    relocationOrder
                )
            ),
            contains(
                isIndexShardAt(".ds-data-stream-2024.04.18-000001", "node-1"),
                isIndexShardAt("lookup", "node-1"),
                isIndexShardAt(".ds-data-stream-2024.04.18-000002", "node-1")
            )
        );
    }

    public void testOrderNecessaryMovesWithInterleaving() {
        var nodes = DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")).build();

        int numDataStreams = randomIntBetween(0, 20);
        int numNormalIndices = randomIntBetween(0, 20);

        var projectBuilder = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        var routingTableBuilder = RoutingTable.builder();
        var writeLoads = new HashMap<ShardId, Double>();
        var dsReadShardIds = new HashSet<ShardId>();
        var shardsToSpread = new HashSet<ShardId>();

        // Create data streams, each with 1 write shard (last index) and 0-20 read shards
        for (int i = 0; i < numDataStreams; i++) {
            String dsName = "data-stream-" + randomIdentifier() + "-" + i;
            int numReadShards = randomIntBetween(0, 20);
            List<Index> dsIndices = new ArrayList<>();

            // Create read shard backing indices
            for (int j = 0; j < numReadShards; j++) {
                String indexName = Strings.format(".ds-%s-2024.04.18-%06d", dsName, j + 1);
                IndexMetadata im = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
                projectBuilder.put(im, false);
                dsIndices.add(im.getIndex());
                routingTableBuilder.add(index(im.getIndex(), "node-1"));

                ShardId shardId = new ShardId(im.getIndex(), 0);
                writeLoads.put(shardId, randomWriteLoad());
                dsReadShardIds.add(shardId);
            }

            // Create write shard backing index (last in the list = write index)
            String writeIndexName = Strings.format(".ds-%s-2024.04.18-%06d", dsName, numReadShards + 1);
            IndexMetadata writeIm = IndexMetadata.builder(writeIndexName).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
            projectBuilder.put(writeIm, false);
            dsIndices.add(writeIm.getIndex());
            routingTableBuilder.add(index(writeIm.getIndex(), "node-1"));

            ShardId writeShardId = new ShardId(writeIm.getIndex(), 0);
            writeLoads.put(writeShardId, randomWriteLoad());
            shardsToSpread.add(writeShardId);

            projectBuilder.put(DataStream.builder(dsName, dsIndices).build());
        }

        // Create normal indices, each with 1-5 shards
        int normalShardsWithNoWriteLoad = 0;
        for (int i = 0; i < numNormalIndices; i++) {
            int numShards = randomIntBetween(1, 5);
            String indexName = randomIndexName() + "-" + i;
            IndexMetadata im = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), numShards, 0)).build();
            projectBuilder.put(im, false);

            var irtBuilder = IndexRoutingTable.builder(im.getIndex());
            for (int s = 0; s < numShards; s++) {
                ShardId shardId = new ShardId(im.getIndex(), s);
                irtBuilder.addShard(newShardRouting(shardId, "node-1", true, STARTED));
                double writeLoad = randomWriteLoad();
                writeLoads.put(shardId, writeLoad);
                if (writeLoad > 0.0) {
                    shardsToSpread.add(shardId);
                } else {
                    normalShardsWithNoWriteLoad++;
                }
            }
            routingTableBuilder.add(irtBuilder.build());
        }

        var metadata = Metadata.builder().put(projectBuilder).build();
        var routing = GlobalRoutingTableTestHelper.routingTable(Metadata.DEFAULT_PROJECT_ID, routingTableBuilder);
        var clusterInfo = ClusterInfo.builder().shardWriteLoads(writeLoads).build();

        var allocation = TestRoutingAllocationFactory.forClusterState(
            ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).routingTable(routing).build()
        ).clusterInfo(clusterInfo).build();

        int totalShards = writeLoads.size();
        ShardRelocationOrder relocationOrder = new StatelessShardRelocationOrder(getClusterSettings(false));
        var iterator = OrderedShardsIterator.createForNecessaryMoves(allocation, new NodeAllocationOrdering(), relocationOrder);
        var orderedShards = next(totalShards, iterator);

        // 1. No duplicates
        assertThat("no duplicate shards in ordering", orderedShards.size(), equalTo(new HashSet<>(orderedShards).size()));

        // 2. All created shards are present (order doesn't matter)
        assertThat(orderedShards.size(), equalTo(writeLoads.size()));
        assertThat(orderedShards, containsInAnyOrder(allocation.routingNodes().node("node-1").copyShards()));

        // 3. Tail of the ordered list should consist entirely of DS read shards (the "second half")
        int expectedSecondHalfSize = Math.ceilDiv(dsReadShardIds.size(), 2); // ceil(N/2) — addToSecondHalf starts true
        for (int i = orderedShards.size() - 1; i >= orderedShards.size() - expectedSecondHalfSize; i--) {
            assertTrue("tail should contain half of the read shards", dsReadShardIds.contains(orderedShards.get(i).shardId()));
        }

        // 4. Make sure the shards with potential write are spread out in the firstHalf (not all at the top).
        List<ShardRouting> firstHalf = orderedShards.subList(0, orderedShards.size() - expectedSecondHalfSize);
        int toSpread = shardsToSpread.size();
        // Only check when there are enough spread shards to observe ordering (>1) and
        // the firstHalf contains enough other shards too, otherwise there is nothing to interleave with.
        if (firstHalf.size() > toSpread + 1 && toSpread > 1) { // at least 2 non-spread and 2 to spread
            long spreadInTopN = firstHalf.subList(0, toSpread).stream().filter(s -> shardsToSpread.contains(s.shardId())).count();
            assertThat("write shards should not all be clustered at the start", spreadInTopN, lessThan((long) toSpread));
        }

        // 5. Check the max number of non writes that can be at the front
        if (shardsToSpread.isEmpty() == false) {
            final int nonWriteShardsInFirstHalf = Math.ceilDiv(dsReadShardIds.size(), 2) + normalShardsWithNoWriteLoad;
            final int maxNonWrites = Math.ceilDiv(nonWriteShardsInFirstHalf, shardsToSpread.size()); // same as how many we interleave
            assertTrue(
                "there should be at least one write shard in the first " + (maxNonWrites + 1) + " elements",
                orderedShards.subList(0, maxNonWrites + 1).stream().anyMatch(s -> shardsToSpread.contains(s.shardId()))
            );
            assertTrue(shardsToSpread.contains(orderedShards.getFirst().shardId()));
        }
    }

    private double randomWriteLoad() {
        return randomBoolean() ? randomDoubleBetween(0.01, 10.0, true) : 0.0;
    }

    private ClusterSettings getClusterSettings(boolean prioritizeWriteShardRelocation) {
        return new ClusterSettings(
            Settings.builder().put(PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING.getKey(), prioritizeWriteShardRelocation).build(),
            Sets.union(BUILT_IN_CLUSTER_SETTINGS, Set.of(PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING))
        );
    }
}
