/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.TransportSearchableSnapshotCacheStoresAction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class SearchableSnapshotAllocatorTests extends ESAllocationTestCase {

    public void testAllocateToNodeWithLargestCache() {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final DiscoveryNode node1 = newNode("node1");
        final DiscoveryNode node2 = newNode("node2");
        final DiscoveryNode node3 = newNode("node3");
        final DiscoveryNode localNode = randomFrom(randomFrom(node1, node2, node3));
        final Settings localNodeSettings = Settings.builder().put(NODE_NAME_SETTING.getKey(), localNode.getName()).build();

        final ClusterName clusterName = org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(localNodeSettings, random());

        final Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(
                        settings(Version.CURRENT).put(
                            ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(),
                            SearchableSnapshotAllocator.ALLOCATOR_NAME
                        )
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putInSyncAllocationIds(shardId.id(), Collections.emptySet())
            )
            .build();
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsRestore(metadata.index(shardId.getIndex()), randomSnapshotSource(shardId));
        final ClusterState state = ClusterState.builder(clusterName)
            .metadata(metadata)
            .routingTable(routingTableBuilder.build())
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        final long shardSize = randomNonNegativeLong();
        final RoutingAllocation allocation = new RoutingAllocation(
            yesAllocationDeciders(),
            new RoutingNodes(state, false),
            state,
            null,
            new SnapshotShardSizeInfo(ImmutableOpenMap.of()) {
                @Override
                public Long getShardSize(ShardRouting shardRouting) {
                    return shardSize;
                }
            },
            TimeUnit.MILLISECONDS.toNanos(deterministicTaskQueue.getCurrentTimeMillis())
        );

        final AtomicInteger reroutesTriggered = new AtomicInteger(0);
        final Map<DiscoveryNode, Long> existingCacheSizes = Map.of(
            node1,
            randomLongBetween(0, shardSize),
            node2,
            randomLongBetween(0, shardSize),
            node3,
            randomLongBetween(0, shardSize)
        );

        final Client client = new NoOpNodeClient(deterministicTaskQueue.getThreadPool()) {

            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action == ClusterRerouteAction.INSTANCE) {
                    reroutesTriggered.incrementAndGet();
                    listener.onResponse((Response) new ClusterRerouteResponse(true, state, new RoutingExplanations()));
                } else if (action == TransportSearchableSnapshotCacheStoresAction.TYPE) {
                    listener.onResponse(
                        (Response) new TransportSearchableSnapshotCacheStoresAction.NodesCacheFilesMetadata(
                            clusterName,
                            existingCacheSizes.entrySet()
                                .stream()
                                .map(
                                    entry -> new TransportSearchableSnapshotCacheStoresAction.NodeCacheFilesMetadata(
                                        entry.getKey(),
                                        entry.getValue()
                                    )
                                )
                                .collect(Collectors.toList()),
                            List.of()
                        )
                    );
                } else {
                    throw new AssertionError("Unexpected action [" + action + "]");
                }
            }
        };

        final SearchableSnapshotAllocator allocator = new SearchableSnapshotAllocator(client);
        allocateAllUnassigned(allocation, allocator);

        assertEquals(1, reroutesTriggered.get());
        assertThat(allocation.routingNodesChanged(), equalTo(true));
    }

    private static void allocateAllUnassigned(RoutingAllocation allocation, ExistingShardsAllocator allocator) {
        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            allocator.allocateUnassigned(iterator.next(), allocation, iterator);
        }
    }

    private static RecoverySource.SnapshotRecoverySource randomSnapshotSource(ShardId shardId) {
        return new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(random()),
                new Snapshot("test-repo", new SnapshotId("test-snap", UUIDs.randomBase64UUID(random()))),
                Version.CURRENT,
                new IndexId(shardId.getIndexName(), UUIDs.randomBase64UUID(random()))
        );
    }
}
