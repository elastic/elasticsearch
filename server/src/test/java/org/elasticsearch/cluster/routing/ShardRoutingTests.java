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

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ShardRoutingTests extends ESTestCase {

    public void testIsSameAllocation() {
        ShardRouting unassignedShard0 = TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting unassignedShard1 = TestShardRouting.newShardRouting("test", 1, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting initializingShard0 =
            TestShardRouting.newShardRouting("test", 0, "1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting initializingShard1 =
            TestShardRouting.newShardRouting("test", 1, "1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting startedShard0 = initializingShard0.moveToStarted();
        ShardRouting startedShard1 = initializingShard1.moveToStarted();

        // test identity
        assertTrue(initializingShard0.isSameAllocation(initializingShard0));

        // test same allocation different state
        assertTrue(initializingShard0.isSameAllocation(startedShard0));

        // test unassigned is false even to itself
        assertFalse(unassignedShard0.isSameAllocation(unassignedShard0));

        // test different shards/nodes/state
        assertFalse(unassignedShard0.isSameAllocation(unassignedShard1));
        assertFalse(unassignedShard0.isSameAllocation(initializingShard0));
        assertFalse(unassignedShard0.isSameAllocation(initializingShard1));
        assertFalse(unassignedShard0.isSameAllocation(startedShard1));
    }

    private ShardRouting randomShardRouting(String index, int shard) {
        ShardRoutingState state = randomFrom(ShardRoutingState.values());
        return TestShardRouting.newShardRouting(index, shard, state == ShardRoutingState.UNASSIGNED ? null : "1",
            state == ShardRoutingState.RELOCATING ? "2" : null, state != ShardRoutingState.UNASSIGNED && randomBoolean(), state);
    }

    public void testIsSourceTargetRelocation() {
        ShardRouting unassignedShard0 =
            TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting initializingShard0 =
            TestShardRouting.newShardRouting("test", 0, "node1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting initializingShard1 =
            TestShardRouting.newShardRouting("test", 1, "node1", randomBoolean(), ShardRoutingState.INITIALIZING);
        assertFalse(initializingShard0.isRelocationTarget());
        ShardRouting startedShard0 = initializingShard0.moveToStarted();
        assertFalse(startedShard0.isRelocationTarget());
        assertFalse(initializingShard1.isRelocationTarget());
        ShardRouting startedShard1 = initializingShard1.moveToStarted();
        assertFalse(startedShard1.isRelocationTarget());
        ShardRouting sourceShard0a = startedShard0.relocate("node2", -1);
        assertFalse(sourceShard0a.isRelocationTarget());
        ShardRouting targetShard0a = sourceShard0a.getTargetRelocatingShard();
        assertTrue(targetShard0a.isRelocationTarget());
        ShardRouting sourceShard0b = startedShard0.relocate("node2", -1);
        ShardRouting sourceShard1 = startedShard1.relocate("node2", -1);

        // test true scenarios
        assertTrue(targetShard0a.isRelocationTargetOf(sourceShard0a));
        assertTrue(sourceShard0a.isRelocationSourceOf(targetShard0a));

        // test two shards are not mixed
        assertFalse(targetShard0a.isRelocationTargetOf(sourceShard1));
        assertFalse(sourceShard1.isRelocationSourceOf(targetShard0a));

        // test two allocations are not mixed
        assertFalse(targetShard0a.isRelocationTargetOf(sourceShard0b));
        assertFalse(sourceShard0b.isRelocationSourceOf(targetShard0a));

        // test different shard states
        assertFalse(targetShard0a.isRelocationTargetOf(unassignedShard0));
        assertFalse(sourceShard0a.isRelocationTargetOf(unassignedShard0));
        assertFalse(unassignedShard0.isRelocationSourceOf(targetShard0a));
        assertFalse(unassignedShard0.isRelocationSourceOf(sourceShard0a));

        assertFalse(targetShard0a.isRelocationTargetOf(initializingShard0));
        assertFalse(sourceShard0a.isRelocationTargetOf(initializingShard0));
        assertFalse(initializingShard0.isRelocationSourceOf(targetShard0a));
        assertFalse(initializingShard0.isRelocationSourceOf(sourceShard0a));

        assertFalse(targetShard0a.isRelocationTargetOf(startedShard0));
        assertFalse(sourceShard0a.isRelocationTargetOf(startedShard0));
        assertFalse(startedShard0.isRelocationSourceOf(targetShard0a));
        assertFalse(startedShard0.isRelocationSourceOf(sourceShard0a));
    }

    public void testEqualsIgnoringVersion() {
        ShardRouting routing = randomShardRouting("test", 0);

        ShardRouting otherRouting = routing;

        Integer[] changeIds = new Integer[]{0, 1, 2, 3, 4, 5, 6};
        for (int changeId : randomSubsetOf(randomIntBetween(1, changeIds.length), changeIds)) {
            boolean unchanged = false;
            switch (changeId) {
                case 0:
                    // change index
                    ShardId shardId = new ShardId(new Index("blubb", randomAlphaOfLength(10)), otherRouting.id());
                    otherRouting = new ShardRouting(shardId, otherRouting.currentNodeId(), otherRouting.relocatingNodeId(),
                            otherRouting.primary(), otherRouting.state(), otherRouting.recoverySource(), otherRouting.unassignedInfo(),
                            otherRouting.allocationId(), otherRouting.getExpectedShardSize());
                    break;
                case 1:
                    // change shard id
                    otherRouting = new ShardRouting(new ShardId(otherRouting.index(), otherRouting.id() + 1),
                        otherRouting.currentNodeId(), otherRouting.relocatingNodeId(),
                            otherRouting.primary(), otherRouting.state(), otherRouting.recoverySource(), otherRouting.unassignedInfo(),
                            otherRouting.allocationId(), otherRouting.getExpectedShardSize());
                    break;
                case 2:
                    // change current node
                    if (otherRouting.assignedToNode() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(otherRouting.shardId(), otherRouting.currentNodeId() + "_1",
                            otherRouting.relocatingNodeId(), otherRouting.primary(), otherRouting.state(), otherRouting.recoverySource(),
                            otherRouting.unassignedInfo(), otherRouting.allocationId(), otherRouting.getExpectedShardSize());
                    }
                    break;
                case 3:
                    // change relocating node
                    if (otherRouting.relocating() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(otherRouting.shardId(), otherRouting.currentNodeId(),
                            otherRouting.relocatingNodeId() + "_1", otherRouting.primary(), otherRouting.state(),
                            otherRouting.recoverySource(), otherRouting.unassignedInfo(), otherRouting.allocationId(),
                            otherRouting.getExpectedShardSize());
                    }
                    break;
                case 4:
                    // change recovery source (only works for inactive primaries)
                    if (otherRouting.active() || otherRouting.primary() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(otherRouting.shardId(), otherRouting.currentNodeId(),
                            otherRouting.relocatingNodeId(), otherRouting.primary(), otherRouting.state(),
                            new RecoverySource.SnapshotRecoverySource(UUIDs.randomBase64UUID(), new Snapshot("test",
                                new SnapshotId("s1", UUIDs.randomBase64UUID())), Version.CURRENT, "test"),
                            otherRouting.unassignedInfo(), otherRouting.allocationId(), otherRouting.getExpectedShardSize());
                    }
                    break;
                case 5:
                    // change primary flag
                    otherRouting = TestShardRouting.newShardRouting(otherRouting.getIndexName(), otherRouting.id(),
                        otherRouting.currentNodeId(), otherRouting.relocatingNodeId(), otherRouting.primary() == false,
                        otherRouting.state(), otherRouting.unassignedInfo());
                    break;
                case 6:
                    // change state
                    ShardRoutingState newState;
                    do {
                        newState = randomFrom(ShardRoutingState.values());
                    } while (newState == otherRouting.state());

                    UnassignedInfo unassignedInfo = otherRouting.unassignedInfo();
                    if (unassignedInfo == null && (newState == ShardRoutingState.UNASSIGNED ||
                        newState == ShardRoutingState.INITIALIZING)) {
                        unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test");
                    }

                    otherRouting = TestShardRouting.newShardRouting(otherRouting.getIndexName(), otherRouting.id(),
                        newState == ShardRoutingState.UNASSIGNED ? null :
                            (otherRouting.currentNodeId() == null ? "1" : otherRouting.currentNodeId()),
                        newState == ShardRoutingState.RELOCATING ? "2" : null,
                            otherRouting.primary(), newState, unassignedInfo);
                    break;
            }

            if (randomBoolean()) {
                // change unassigned info
                otherRouting = TestShardRouting.newShardRouting(otherRouting.getIndexName(), otherRouting.id(),
                    otherRouting.currentNodeId(), otherRouting.relocatingNodeId(), otherRouting.primary(), otherRouting.state(),
                        otherRouting.unassignedInfo() == null ? new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test") :
                            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, otherRouting.unassignedInfo().getMessage() + "_1"));
            }

            if (unchanged == false) {
                logger.debug("comparing\nthis  {} to\nother {}", routing, otherRouting);
                assertFalse("expected non-equality\nthis  " + routing + ",\nother " + otherRouting,
                    routing.equalsIgnoringMetaData(otherRouting));
            }
        }
    }

    public void testExpectedSize() throws IOException {
        final int iters = randomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            ShardRouting routing = randomShardRouting("test", 0);
            long byteSize = randomIntBetween(0, Integer.MAX_VALUE);
            if (routing.unassigned()) {
                routing = ShardRoutingHelper.initialize(routing, "foo", byteSize);
            } else if (routing.started()) {
                routing = ShardRoutingHelper.relocate(routing, "foo", byteSize);
            } else {
                byteSize = -1;
            }
            if (randomBoolean()) {
                BytesStreamOutput out = new BytesStreamOutput();
                routing.writeTo(out);
                routing = new ShardRouting(out.bytes().streamInput());
            }
            if (routing.initializing() || routing.relocating()) {
                assertEquals(routing.toString(), byteSize, routing.getExpectedShardSize());
                if (byteSize >= 0) {
                    assertTrue(routing.toString(), routing.toString().contains("expected_shard_size[" + byteSize + "]"));
                }
                if (routing.initializing()) {
                    routing = routing.moveToStarted();
                    assertEquals(-1, routing.getExpectedShardSize());
                    assertFalse(routing.toString(), routing.toString().contains("expected_shard_size[" + byteSize + "]"));
                }
            } else {
                assertFalse(routing.toString(), routing.toString().contains("expected_shard_size [" + byteSize + "]"));
                assertEquals(byteSize, routing.getExpectedShardSize());
            }
        }
    }
}
