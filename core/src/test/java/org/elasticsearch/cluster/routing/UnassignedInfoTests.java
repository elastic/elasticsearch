/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.test.ESAllocationTestCase;

import java.util.Collections;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class UnassignedInfoTests extends ESAllocationTestCase {
    public void testReasonOrdinalOrder() {
        UnassignedInfo.Reason[] order = new UnassignedInfo.Reason[]{
                UnassignedInfo.Reason.INDEX_CREATED,
                UnassignedInfo.Reason.CLUSTER_RECOVERED,
                UnassignedInfo.Reason.INDEX_REOPENED,
                UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED,
                UnassignedInfo.Reason.NEW_INDEX_RESTORED,
                UnassignedInfo.Reason.EXISTING_INDEX_RESTORED,
                UnassignedInfo.Reason.REPLICA_ADDED,
                UnassignedInfo.Reason.ALLOCATION_FAILED,
                UnassignedInfo.Reason.NODE_LEFT,
                UnassignedInfo.Reason.REROUTE_CANCELLED,
                UnassignedInfo.Reason.REINITIALIZED,
                UnassignedInfo.Reason.REALLOCATED_REPLICA,
                UnassignedInfo.Reason.PRIMARY_FAILED};
        for (int i = 0; i < order.length; i++) {
            assertThat(order[i].ordinal(), equalTo(i));
        }
        assertThat(UnassignedInfo.Reason.values().length, equalTo(order.length));
    }

    public void testSerialization() throws Exception {
        UnassignedInfo.Reason reason = RandomPicks.randomFrom(random(), UnassignedInfo.Reason.values());
        UnassignedInfo meta = reason == UnassignedInfo.Reason.ALLOCATION_FAILED ?
            new UnassignedInfo(reason, randomBoolean() ? randomAsciiOfLength(4) : null, null, randomIntBetween(1, 100), System.nanoTime(), System.currentTimeMillis(), false):
            new UnassignedInfo(reason, randomBoolean() ? randomAsciiOfLength(4) : null);
        BytesStreamOutput out = new BytesStreamOutput();
        meta.writeTo(out);
        out.close();

        UnassignedInfo read = new UnassignedInfo(StreamInput.wrap(out.bytes()));
        assertThat(read.getReason(), equalTo(meta.getReason()));
        assertThat(read.getUnassignedTimeInMillis(), equalTo(meta.getUnassignedTimeInMillis()));
        assertThat(read.getMessage(), equalTo(meta.getMessage()));
        assertThat(read.getDetails(), equalTo(meta.getDetails()));
        assertThat(read.getNumFailedAllocations(), equalTo(meta.getNumFailedAllocations()));
    }

    public void testIndexCreated() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNew(metaData.index("test")).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.INDEX_CREATED));
        }
    }

    public void testClusterRecovered() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsRecovery(metaData.index("test")).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.CLUSTER_RECOVERED));
        }
    }

    public void testIndexReopened() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsFromCloseToOpen(metaData.index("test")).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.INDEX_REOPENED));
        }
    }

    public void testNewIndexRestored() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNewRestore(metaData.index("test"), new RestoreSource(new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())), Version.CURRENT, "test"), new IntHashSet()).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.NEW_INDEX_RESTORED));
        }
    }

    public void testExistingIndexRestored() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsRestore(metaData.index("test"), new RestoreSource(new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())), Version.CURRENT, "test")).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED));
        }
    }

    public void testDanglingIndexImported() {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(randomIntBetween(1, 3)).numberOfReplicas(randomIntBetween(0, 3)))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsFromDangling(metaData.index("test")).build()).build();
        for (ShardRouting shard : clusterState.getRoutingNodes().shardsWithState(UNASSIGNED)) {
            assertThat(shard.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED));
        }
    }

    public void testReplicaAdded() {
        AllocationService allocation = createAllocationService();
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();
        final Index index = metaData.index("test").getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNew(metaData.index(index)).build()).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1"))).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState, "reroute")).build();
        // starting primaries
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        for (IndexShardRoutingTable indexShardRoutingTable : clusterState.routingTable().index(index)) {
            builder.addIndexShard(indexShardRoutingTable);
        }
        builder.addReplica();
        clusterState = ClusterState.builder(clusterState).routingTable(RoutingTable.builder(clusterState.routingTable()).add(builder).build()).build();
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo(), notNullValue());
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.REPLICA_ADDED));
    }

    /**
     * The unassigned meta is kept when a shard goes to INITIALIZING, but cleared when it moves to STARTED.
     */
    public void testStateTransitionMetaHandling() {
        ShardRouting shard = TestShardRouting.newShardRouting("test", 1, null, null, null, true, ShardRoutingState.UNASSIGNED, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        assertThat(shard.unassignedInfo(), notNullValue());
        shard = shard.initialize("test_node", null, -1);
        assertThat(shard.state(), equalTo(ShardRoutingState.INITIALIZING));
        assertThat(shard.unassignedInfo(), notNullValue());
        shard = shard.moveToStarted();
        assertThat(shard.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(shard.unassignedInfo(), nullValue());
    }

    /**
     * Tests that during reroute when a node is detected as leaving the cluster, the right unassigned meta is set
     */
    public void testNodeLeave() {
        AllocationService allocation = createAllocationService();
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNew(metaData.index("test")).build()).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState, "reroute")).build();
        // starting primaries
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
        // starting replicas
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState, "reroute")).build();
        // verify that NODE_LEAVE is the reason for meta
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(true));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo(), notNullValue());
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.NODE_LEFT));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getUnassignedTimeInMillis(), greaterThan(0L));
    }

    /**
     * Verifies that when a shard fails, reason is properly set and details are preserved.
     */
    public void testFailedShard() {
        AllocationService allocation = createAllocationService();
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNew(metaData.index("test")).build()).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState, "reroute")).build();
        // starting primaries
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
        // starting replicas
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // fail shard
        ShardRouting shardToFail = clusterState.getRoutingNodes().shardsWithState(STARTED).get(0);
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyFailedShards(clusterState, Collections.singletonList(new FailedRerouteAllocation.FailedShard(shardToFail, "test fail", null)))).build();
        // verify the reason and details
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(true));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo(), notNullValue());
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getMessage(), equalTo("test fail"));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getDetails(), equalTo("test fail"));
        assertThat(clusterState.getRoutingNodes().shardsWithState(UNASSIGNED).get(0).unassignedInfo().getUnassignedTimeInMillis(), greaterThan(0L));
    }

    /**
     * Verifies that delayed allocation calculation are correct.
     */
    public void testRemainingDelayCalculation() throws Exception {
        final long baseTime = System.nanoTime();
        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, "test", null, 0, baseTime, System.currentTimeMillis(), randomBoolean());
        final long totalDelayNanos = TimeValue.timeValueMillis(10).nanos();
        final Settings indexSettings = Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueNanos(totalDelayNanos)).build();
        long delay = unassignedInfo.getRemainingDelay(baseTime, indexSettings);
        assertThat(delay, equalTo(totalDelayNanos));
        long delta1 = randomIntBetween(1, (int) (totalDelayNanos - 1));
        delay = unassignedInfo.getRemainingDelay(baseTime + delta1, indexSettings);
        assertThat(delay, equalTo(totalDelayNanos - delta1));
        delay = unassignedInfo.getRemainingDelay(baseTime + totalDelayNanos, indexSettings);
        assertThat(delay, equalTo(0L));
        delay = unassignedInfo.getRemainingDelay(baseTime + totalDelayNanos + randomIntBetween(1, 20), indexSettings);
        assertThat(delay, equalTo(0L));
    }


    public void testNumberOfDelayedUnassigned() throws Exception {
        MockAllocationService allocation = createAllocationService(Settings.EMPTY, new DelayedShardsMockGatewayAllocator());
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNew(metaData.index("test1")).addAsNew(metaData.index("test2")).build()).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState, "reroute")).build();
        assertThat(UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), equalTo(0));
        // starting primaries
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
        // starting replicas
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        // make sure both replicas are marked as delayed (i.e. not reallocated)
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState, "reroute")).build();
        assertThat(clusterState.prettyPrint(), UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), equalTo(2));
    }

    public void testFindNextDelayedAllocation() {
        MockAllocationService allocation = createAllocationService(Settings.EMPTY, new DelayedShardsMockGatewayAllocator());
        final TimeValue delayTest1 = TimeValue.timeValueMillis(randomIntBetween(1, 200));
        final TimeValue delayTest2 = TimeValue.timeValueMillis(randomIntBetween(1, 200));
        final long expectMinDelaySettingsNanos = Math.min(delayTest1.nanos(), delayTest2.nanos());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayTest1)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayTest2)).numberOfShards(1).numberOfReplicas(1))
                .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metaData(metaData)
                .routingTable(RoutingTable.builder().addAsNew(metaData.index("test1")).addAsNew(metaData.index("test2")).build()).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState, "reroute")).build();
        assertThat(UnassignedInfo.getNumberOfDelayedUnassigned(clusterState), equalTo(0));
        // starting primaries
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
        // starting replicas
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING))).build();
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        // remove node2 and reroute
        final long baseTime = System.nanoTime();
        allocation.setNanoTimeOverride(baseTime);
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2")).build();
        clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState, "reroute")).build();

        final long delta = randomBoolean() ? 0 : randomInt((int) expectMinDelaySettingsNanos - 1);

        if (delta > 0) {
            allocation.setNanoTimeOverride(baseTime + delta);
            clusterState = ClusterState.builder(clusterState).routingResult(allocation.reroute(clusterState, "time moved")).build();
        }

        assertThat(UnassignedInfo.findNextDelayedAllocation(baseTime + delta, clusterState), equalTo(expectMinDelaySettingsNanos - delta));
    }
}
