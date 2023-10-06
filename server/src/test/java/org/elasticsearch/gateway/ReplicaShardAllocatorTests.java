/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ReplicaShardAllocatorTests extends ESAllocationTestCase {
    private static final String MIN_SUPPORTED_LUCENE_VERSION = IndexVersion.MINIMUM_COMPATIBLE.luceneVersion().toString();

    private final ShardId shardId = new ShardId("test", "_na_", 0);
    private final DiscoveryNode node1 = newNode("node1");
    private final DiscoveryNode node2 = newNode("node2");
    private final DiscoveryNode node3 = newNode("node3");

    private TestAllocator testAllocator;

    @Before
    public void buildTestAllocator() {
        this.testAllocator = new TestAllocator();
    }

    private void allocateAllUnassigned(final RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        while (iterator.hasNext()) {
            testAllocator.allocateUnassigned(iterator.next(), allocation, iterator);
        }
    }

    /**
     * Verifies that when we are still fetching data in an async manner, the replica shard moves to ignore unassigned.
     */
    public void testNoAsyncFetchData() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.clean();
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that on index creation, we don't go and fetch data, but keep the replica shard unassigned to let
     * the shard allocator to allocate it. There isn't a copy around to find anyhow.
     */
    public void testNoAsyncFetchOnIndexCreation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(
            yesAllocationDeciders(),
            Settings.EMPTY,
            UnassignedInfo.Reason.INDEX_CREATED
        );
        testAllocator.clean();
        allocateAllUnassigned(allocation);
        assertThat(testAllocator.getFetchDataCalledAndClean(), equalTo(false));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that for anything but index creation, fetch data ends up being called, since we need to go and try
     * and find a better copy for the shard.
     */
    public void testAsyncFetchOnAnythingButIndexCreation() {
        UnassignedInfo.Reason reason = RandomPicks.randomFrom(
            random(),
            EnumSet.complementOf(EnumSet.of(UnassignedInfo.Reason.INDEX_CREATED))
        );
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders(), Settings.EMPTY, reason);
        testAllocator.clean();
        allocateAllUnassigned(allocation);
        assertThat("failed with reason " + reason, testAllocator.getFetchDataCalledAndClean(), equalTo(true));
    }

    /**
     * Verifies that when there is a full match (syncId and files) we allocate it to matching node.
     */
    public void testSimpleFullMatchAllocation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(nodeToMatch, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(nodeToMatch.getId())
        );
    }

    /**
     * Verifies that when there is a sync id match but no files match, we allocate it to matching node.
     */
    public void testSyncIdMatch() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(nodeToMatch, "MATCH", new StoreFileMetadata("file1", 10, "NO_MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(nodeToMatch.getId())
        );
    }

    /**
     * Verifies that when there is no sync id match but files match, we allocate it to matching node.
     */
    public void testFileChecksumMatch() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        DiscoveryNode nodeToMatch = randomBoolean() ? node2 : node3;
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(nodeToMatch, "NO_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(nodeToMatch.getId())
        );
    }

    public void testPreferCopyWithHighestMatchingOperations() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        long retainingSeqNoOnPrimary = randomLongBetween(1, Integer.MAX_VALUE);
        long retainingSeqNoForNode2 = randomLongBetween(0, retainingSeqNoOnPrimary - 1);
        // Rarely use a seqNo above retainingSeqNoOnPrimary, which could in theory happen when primary fails and comes back quickly.
        long retainingSeqNoForNode3 = randomLongBetween(retainingSeqNoForNode2 + 1, retainingSeqNoOnPrimary + 100);
        List<RetentionLease> retentionLeases = Arrays.asList(
            newRetentionLease(node1, retainingSeqNoOnPrimary),
            newRetentionLease(node2, retainingSeqNoForNode2),
            newRetentionLease(node3, retainingSeqNoForNode3)
        );
        testAllocator.addData(
            node1,
            retentionLeases,
            "MATCH",
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testAllocator.addData(node2, "NOT_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node3.getId())
        );
    }

    public void testCancelRecoveryIfFoundCopyWithNoopRetentionLease() {
        final UnassignedInfo unassignedInfo;
        final Set<String> failedNodeIds;
        if (randomBoolean()) {
            failedNodeIds = Collections.emptySet();
            unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null);
        } else {
            failedNodeIds = new HashSet<>(randomSubsetOf(Set.of("node-4", "node-5", "node-6", "node-7")));
            unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.ALLOCATION_FAILED,
                null,
                null,
                randomIntBetween(1, 10),
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                failedNodeIds,
                null
            );
        }
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), unassignedInfo);
        long retainingSeqNo = randomLongBetween(1, Long.MAX_VALUE);
        testAllocator.addData(
            node1,
            Arrays.asList(newRetentionLease(node1, retainingSeqNo), newRetentionLease(node3, retainingSeqNo)),
            "MATCH",
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testAllocator.addData(node2, "NO_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        List<ShardRouting> unassignedShards = shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED);
        assertThat(unassignedShards, hasSize(1));
        assertThat(unassignedShards.get(0).shardId(), equalTo(shardId));
        assertThat(unassignedShards.get(0).unassignedInfo().getNumFailedAllocations(), equalTo(0));
        assertThat(unassignedShards.get(0).unassignedInfo().getFailedNodeIds(), equalTo(failedNodeIds));
    }

    public void testNotCancellingRecoveryIfCurrentRecoveryHasRetentionLease() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        List<RetentionLease> peerRecoveryRetentionLeasesOnPrimary = new ArrayList<>();
        long retainingSeqNo = randomLongBetween(1, Long.MAX_VALUE);
        peerRecoveryRetentionLeasesOnPrimary.add(newRetentionLease(node1, retainingSeqNo));
        peerRecoveryRetentionLeasesOnPrimary.add(newRetentionLease(node2, randomLongBetween(1, retainingSeqNo)));
        if (randomBoolean()) {
            peerRecoveryRetentionLeasesOnPrimary.add(newRetentionLease(node3, randomLongBetween(0, retainingSeqNo)));
        }
        testAllocator.addData(
            node1,
            peerRecoveryRetentionLeasesOnPrimary,
            "MATCH",
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testAllocator.addData(node2, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(0));
    }

    public void testNotCancelIfPrimaryDoesNotHaveValidRetentionLease() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        testAllocator.addData(
            node1,
            Collections.singletonList(newRetentionLease(node3, randomNonNegativeLong())),
            "MATCH",
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testAllocator.addData(node2, "NOT_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, "NOT_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(0));
    }

    public void testIgnoreRetentionLeaseIfCopyIsEmpty() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        long retainingSeqNo = randomLongBetween(1, Long.MAX_VALUE);
        List<RetentionLease> retentionLeases = new ArrayList<>();
        retentionLeases.add(newRetentionLease(node1, retainingSeqNo));
        retentionLeases.add(newRetentionLease(node2, randomLongBetween(0, retainingSeqNo)));
        if (randomBoolean()) {
            retentionLeases.add(newRetentionLease(node3, randomLongBetween(0, retainingSeqNo)));
        }
        testAllocator.addData(
            node1,
            retentionLeases,
            randomSyncId(),
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testAllocator.addData(node2, null); // has retention lease but store is empty
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node3.getId())
        );
    }

    /**
     * When we can't find primary data, but still find replica data, we go ahead and keep it unassigned
     * to be allocated. This is today behavior, which relies on a primary corruption identified with
     * adding a replica and having that replica actually recover and cause the corruption to be identified
     * See CorruptFileTest#
     */
    public void testNoPrimaryData() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that when there is primary data, but no data at all on other nodes, the shard keeps
     * unassigned to be allocated later on.
     */
    public void testNoDataForReplicaOnAnyNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * Verifies that when there is primary data, but no matching data at all on other nodes, the shard keeps
     * unassigned to be allocated later on.
     */
    public void testNoMatchingFilesForReplicaOnAnyNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(yesAllocationDeciders());
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "NO_MATCH", new StoreFileMetadata("file1", 10, "NO_MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    /**
     * When there is no decision or throttle decision across all nodes for the shard, make sure the shard
     * moves to the ignore unassigned list.
     */
    public void testNoOrThrottleDecidersRemainsInUnassigned() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(
            randomBoolean() ? noAllocationDeciders() : throttleAllocationDeciders()
        );
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    /**
     * Tests when the node to allocate to due to matching is being throttled, we move the shard to ignored
     * to wait till throttling on it is done.
     */
    public void testThrottleWhenAllocatingToMatchingNode() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(
            new AllocationDeciders(
                Arrays.asList(
                    new TestAllocateDecision(Decision.YES),
                    new SameShardAllocationDecider(createBuiltInClusterSettings()),
                    new AllocationDecider() {
                        @Override
                        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                            if (node.node().equals(node2)) {
                                return Decision.THROTTLE;
                            }
                            return Decision.YES;
                        }
                    }
                )
            )
        );
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));
    }

    public void testDelayedAllocation() {
        RoutingAllocation allocation = onePrimaryOnNode1And1Replica(
            yesAllocationDeciders(),
            Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueHours(1)).build(),
            UnassignedInfo.Reason.NODE_LEFT
        );
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        if (randomBoolean()) {
            // we sometime return empty list of files, make sure we test this as well
            testAllocator.addData(node2, null);
        }
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(allocation.routingNodes().unassigned().ignored().size(), equalTo(1));
        assertThat(allocation.routingNodes().unassigned().ignored().get(0).shardId(), equalTo(shardId));

        allocation = onePrimaryOnNode1And1Replica(
            yesAllocationDeciders(),
            Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueHours(1)).build(),
            UnassignedInfo.Reason.NODE_LEFT
        );
        testAllocator.addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        allocateAllUnassigned(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo(node2.getId())
        );
    }

    public void testCancelRecoveryBetterSyncId() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "NO_MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node3, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(true));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(1));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).get(0).shardId(), equalTo(shardId));
    }

    public void testNotCancellingRecoveryIfSyncedOnExistingRecovery() {
        final UnassignedInfo unassignedInfo;
        if (randomBoolean()) {
            unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null);
        } else {
            unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.ALLOCATION_FAILED,
                null,
                null,
                randomIntBetween(1, 10),
                System.nanoTime(),
                System.currentTimeMillis(),
                false,
                UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                Set.of("node-4"),
                null
            );
        }
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), unassignedInfo);
        List<RetentionLease> retentionLeases = new ArrayList<>();
        if (randomBoolean()) {
            long retainingSeqNoOnPrimary = randomLongBetween(0, Long.MAX_VALUE);
            retentionLeases.add(newRetentionLease(node1, retainingSeqNoOnPrimary));
            if (randomBoolean()) {
                retentionLeases.add(newRetentionLease(node2, randomLongBetween(0, retainingSeqNoOnPrimary)));
            }
            if (randomBoolean()) {
                retentionLeases.add(newRetentionLease(node3, randomLongBetween(0, retainingSeqNoOnPrimary)));
            }
        }
        testAllocator.addData(
            node1,
            retentionLeases,
            "MATCH",
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        );
        testAllocator.addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(0));
    }

    public void testNotCancellingRecovery() {
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders());
        testAllocator.addData(node1, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node2, "MATCH", new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED).size(), equalTo(0));
    }

    public void testDoNotCancelForBrokenNode() {
        Set<String> failedNodes = new HashSet<>();
        failedNodes.add(node3.getId());
        if (randomBoolean()) {
            failedNodes.add("node4");
        }
        UnassignedInfo unassignedInfo = new UnassignedInfo(
            UnassignedInfo.Reason.ALLOCATION_FAILED,
            null,
            null,
            randomIntBetween(failedNodes.size(), 10),
            System.nanoTime(),
            System.currentTimeMillis(),
            false,
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            failedNodes,
            null
        );
        RoutingAllocation allocation = onePrimaryOnNode1And1ReplicaRecovering(yesAllocationDeciders(), unassignedInfo);
        long retainingSeqNoOnPrimary = randomLongBetween(0, Long.MAX_VALUE);
        List<RetentionLease> retentionLeases = Arrays.asList(
            newRetentionLease(node1, retainingSeqNoOnPrimary),
            newRetentionLease(node3, retainingSeqNoOnPrimary)
        );
        testAllocator.addData(
            node1,
            retentionLeases,
            "MATCH",
            new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION)
        )
            .addData(node2, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION))
            .addData(node3, randomSyncId(), new StoreFileMetadata("file1", 10, "MATCH_CHECKSUM", MIN_SUPPORTED_LUCENE_VERSION));
        testAllocator.processExistingRecoveries(allocation);
        assertThat(allocation.routingNodesChanged(), equalTo(false));
        assertThat(shardsWithState(allocation.routingNodes(), ShardRoutingState.UNASSIGNED), empty());
    }

    private RoutingAllocation onePrimaryOnNode1And1Replica(AllocationDeciders deciders) {
        return onePrimaryOnNode1And1Replica(deciders, Settings.EMPTY, UnassignedInfo.Reason.CLUSTER_RECOVERED);
    }

    private RoutingAllocation onePrimaryOnNode1And1Replica(AllocationDeciders deciders, Settings settings, UnassignedInfo.Reason reason) {
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, node1.getId(), true, ShardRoutingState.STARTED);
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(settings(IndexVersion.current()).put(settings))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putInSyncAllocationIds(0, Sets.newHashSet(primaryShard.allocationId().getId()));
        Metadata metadata = Metadata.builder().put(indexMetadata).build();
        // mark shard as delayed if reason is NODE_LEFT
        boolean delayed = reason == UnassignedInfo.Reason.NODE_LEFT
            && UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(settings).nanos() > 0;
        int failedAllocations = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ? 1 : 0;
        final String lastAllocatedNodeId = reason == UnassignedInfo.Reason.NODE_RESTARTING || randomBoolean()
            ? randomAlphaOfLength(10)
            : null;
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard)
                            .addShard(
                                ShardRouting.newUnassigned(
                                    shardId,
                                    false,
                                    RecoverySource.PeerRecoverySource.INSTANCE,
                                    new UnassignedInfo(
                                        reason,
                                        null,
                                        null,
                                        failedAllocations,
                                        System.nanoTime(),
                                        System.currentTimeMillis(),
                                        delayed,
                                        UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                                        Collections.emptySet(),
                                        lastAllocatedNodeId
                                    ),
                                    ShardRouting.Role.DEFAULT
                                )
                            )
                    )
            )
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(
            deciders,
            state.mutableRoutingNodes(),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private RoutingAllocation onePrimaryOnNode1And1ReplicaRecovering(AllocationDeciders deciders, UnassignedInfo unassignedInfo) {
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, node1.getId(), true, ShardRoutingState.STARTED);
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shardId.getIndexName())
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Sets.newHashSet(primaryShard.allocationId().getId()))
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard)
                            .addShard(
                                TestShardRouting.newShardRouting(
                                    shardId,
                                    node2.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.INITIALIZING,
                                    unassignedInfo
                                )
                            )
                    )
            )
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3))
            .build();
        return new RoutingAllocation(
            deciders,
            state.mutableRoutingNodes(),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
    }

    private RoutingAllocation onePrimaryOnNode1And1ReplicaRecovering(AllocationDeciders deciders) {
        return onePrimaryOnNode1And1ReplicaRecovering(deciders, new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));
    }

    static RetentionLease newRetentionLease(DiscoveryNode node, long retainingSeqNo) {
        return new RetentionLease(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(node.getId()),
            retainingSeqNo,
            randomNonNegativeLong(),
            ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
        );
    }

    static String randomSyncId() {
        return randomFrom("MATCH", "NOT_MATCH", null);
    }

    class TestAllocator extends ReplicaShardAllocator {

        private Map<DiscoveryNode, TransportNodesListShardStoreMetadata.StoreFilesMetadata> data = null;
        private final AtomicBoolean fetchDataCalled = new AtomicBoolean(false);

        public void clean() {
            data = null;
        }

        public boolean getFetchDataCalledAndClean() {
            return fetchDataCalled.getAndSet(false);
        }

        public TestAllocator addData(DiscoveryNode node, String syncId, StoreFileMetadata... files) {
            return addData(node, Collections.emptyList(), syncId, files);
        }

        TestAllocator addData(
            DiscoveryNode node,
            List<RetentionLease> peerRecoveryRetentionLeases,
            String syncId,
            StoreFileMetadata... files
        ) {
            if (data == null) {
                data = new HashMap<>();
            }
            Map<String, StoreFileMetadata> filesAsMap = new HashMap<>();
            for (StoreFileMetadata file : files) {
                filesAsMap.put(file.name(), file);
            }
            Map<String, String> commitData = new HashMap<>();
            if (syncId != null) {
                commitData.put(Engine.SYNC_COMMIT_ID, syncId);
            }
            data.put(
                node,
                new TransportNodesListShardStoreMetadata.StoreFilesMetadata(
                    new Store.MetadataSnapshot(unmodifiableMap(filesAsMap), unmodifiableMap(commitData), randomInt()),
                    peerRecoveryRetentionLeases
                )
            );
            return this;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetchData(
            ShardRouting shard,
            RoutingAllocation allocation
        ) {
            fetchDataCalled.set(true);
            Map<DiscoveryNode, TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> tData = null;
            if (data != null) {
                tData = new HashMap<>();
                for (Map.Entry<DiscoveryNode, TransportNodesListShardStoreMetadata.StoreFilesMetadata> entry : data.entrySet()) {
                    tData.put(
                        entry.getKey(),
                        new TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata(entry.getKey(), entry.getValue())
                    );
                }
            }
            return new AsyncShardFetch.FetchResult<>(shardId, tData, Collections.emptySet());
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return fetchDataCalled.get();
        }
    }
}
