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

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.allocator.NodeAllocationOrdering;
import org.elasticsearch.cluster.routing.allocation.allocator.OrderedShardsIterator;
import org.elasticsearch.cluster.routing.allocation.allocator.OrderedShardsIteratorTests;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardRelocationOrder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexVersion;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.elasticsearch.xpack.stateless.allocation.StatelessShardRelocationOrder.PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING;
import static org.hamcrest.Matchers.contains;

public class StatelessShardRelocationOrderTests extends OrderedShardsIteratorTests {

    @Override
    protected ShardRelocationOrder createShardRelocationOrder() {
        // defaults to the same behaviour as the default shard relocation order
        return new StatelessShardRelocationOrder(getClusterSettings(true));
    }

    public void testNoWriteShardRelocationPriority() {
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

        // when performing necessary moves (such as preparation for the node shutdown) no prioritization should be applied
        var routingAllocation = createRoutingAllocation(nodes, metadata, routing);
        var shards = routingAllocation.routingNodes().node("node-1").copyShards();
        assertThat(
            next(3, OrderedShardsIterator.createForNecessaryMoves(routingAllocation, new NodeAllocationOrdering(), relocationOrder)),
            contains(
                isIndexShardAt(shards[0].getIndexName(), "node-1"),
                isIndexShardAt(shards[1].getIndexName(), "node-1"),
                isIndexShardAt(shards[2].getIndexName(), "node-1")
            )
        );

        // when performing rebalancing write shards should be moved last
        assertThat(
            next(
                3,
                OrderedShardsIterator.createForBalancing(
                    createRoutingAllocation(nodes, metadata, routing),
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

    private ClusterSettings getClusterSettings(boolean prioritizeWriteShardRelocation) {
        return new ClusterSettings(
            Settings.builder().put(PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING.getKey(), prioritizeWriteShardRelocation).build(),
            Sets.union(BUILT_IN_CLUSTER_SETTINGS, Set.of(PRIORITIZE_WRITE_SHARD_RELOCATIONS_SETTING))
        );
    }
}
