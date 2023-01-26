/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardIdTests;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNullElseGet;
import static org.hamcrest.Matchers.containsString;

public class ShardRoutingTests extends AbstractWireSerializingTestCase<ShardRouting> {

    @Override
    protected Writeable.Reader<ShardRouting> instanceReader() {
        return ShardRouting::new;
    }

    @Override
    protected ShardRouting createTestInstance() {
        var state = randomFrom(ShardRoutingState.values());
        var primary = randomBoolean();
        return new ShardRouting(
            new ShardId(randomIdentifier(), UUIDs.randomBase64UUID(), randomIntBetween(0, 99)),
            state == ShardRoutingState.UNASSIGNED ? null : randomIdentifier(),
            state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.STARTED ? null : randomIdentifier(),
            primary,
            state,
            TestShardRouting.buildRecoveryTarget(primary, state),
            TestShardRouting.buildUnassignedInfo(state),
            TestShardRouting.buildRelocationFailureInfo(state),
            TestShardRouting.buildAllocationId(state),
            randomLongBetween(-1, 1024),
            randomFrom(ShardRouting.Role.DEFAULT, (primary ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY))
        );
    }

    @Override
    protected ShardRouting mutateInstance(ShardRouting instance) {
        var mutation = randomInt(4);
        if (mutation == 0) {
            return mutateShardId(instance);
        } else if (mutation == 1) {
            return mutateState(instance);
        } else if (mutation == 2 && instance.state() == ShardRoutingState.STARTED) {
            return mutateCurrentNodeId(instance);
        } else if (mutation == 3) {
            return mutateRole(instance);
        } else {
            return randomValueOtherThan(instance, this::createTestInstance);
        }
    }

    private static ShardRouting mutateShardId(ShardRouting instance) {
        return new ShardRouting(
            ShardIdTests.mutate(instance.shardId()),
            instance.currentNodeId(),
            instance.relocatingNodeId(),
            instance.primary(),
            instance.state(),
            instance.recoverySource(),
            instance.unassignedInfo(),
            instance.relocationFailureInfo(),
            instance.allocationId(),
            instance.getExpectedShardSize(),
            instance.role()
        );
    }

    private static ShardRouting mutateState(ShardRouting instance) {
        var newState = randomValueOtherThan(instance.state(), () -> randomFrom(ShardRoutingState.values()));
        return new ShardRouting(
            instance.shardId(),
            newState == ShardRoutingState.UNASSIGNED ? null : requireNonNullElseGet(instance.currentNodeId(), ESTestCase::randomIdentifier),
            newState == ShardRoutingState.UNASSIGNED || newState == ShardRoutingState.STARTED
                ? null
                : requireNonNullElseGet(instance.relocatingNodeId(), ESTestCase::randomIdentifier),
            instance.primary(),
            newState,
            newState == ShardRoutingState.STARTED || newState == ShardRoutingState.RELOCATING
                ? null
                : requireNonNullElseGet(
                    instance.recoverySource(),
                    () -> TestShardRouting.buildRecoveryTarget(instance.primary(), newState)
                ),
            newState == ShardRoutingState.STARTED || newState == ShardRoutingState.RELOCATING
                ? null
                : requireNonNullElseGet(instance.unassignedInfo(), () -> TestShardRouting.buildUnassignedInfo(newState)),
            instance.relocationFailureInfo(),
            switch (newState) {
            case UNASSIGNED -> null;
            case INITIALIZING, STARTED -> requireNonNullElseGet(instance.allocationId(), AllocationId::newInitializing);
            case RELOCATING -> AllocationId.newRelocation(requireNonNullElseGet(instance.allocationId(), AllocationId::newInitializing));
            },
            instance.getExpectedShardSize(),
            instance.role()
        );
    }

    private static ShardRouting mutateCurrentNodeId(ShardRouting instance) {
        return new ShardRouting(
            instance.shardId(),
            randomValueOtherThan(instance.currentNodeId(), ESTestCase::randomIdentifier),
            instance.relocatingNodeId(),
            instance.primary(),
            instance.state(),
            instance.recoverySource(),
            instance.unassignedInfo(),
            instance.relocationFailureInfo(),
            instance.allocationId(),
            instance.getExpectedShardSize(),
            instance.role()
        );
    }

    private static ShardRouting mutateRole(ShardRouting instance) {
        return new ShardRouting(
            instance.shardId(),
            instance.currentNodeId(),
            instance.relocatingNodeId(),
            instance.primary(),
            instance.state(),
            instance.recoverySource(),
            instance.unassignedInfo(),
            instance.relocationFailureInfo(),
            instance.allocationId(),
            instance.getExpectedShardSize(),
            randomValueOtherThan(
                instance.role(),
                () -> randomFrom(
                    ShardRouting.Role.DEFAULT,
                    (instance.primary() ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY)
                )
            )
        );
    }

    private ShardRouting randomShardRouting(String index, int shard) {
        ShardRoutingState state = randomFrom(ShardRoutingState.values());
        return TestShardRouting.newShardRouting(
            index,
            shard,
            state == ShardRoutingState.UNASSIGNED ? null : "1",
            state == ShardRoutingState.RELOCATING ? "2" : null,
            state != ShardRoutingState.UNASSIGNED && randomBoolean(),
            state
        );
    }

    public void testIsSameAllocation() {
        ShardRouting unassignedShard0 = TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting unassignedShard1 = TestShardRouting.newShardRouting("test", 1, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting initializingShard0 = TestShardRouting.newShardRouting("test", 0, "1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting initializingShard1 = TestShardRouting.newShardRouting("test", 1, "1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting startedShard0 = initializingShard0.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        ShardRouting startedShard1 = initializingShard1.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

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

    public void testIsSourceTargetRelocation() {
        ShardRouting unassignedShard0 = TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting initializingShard0 = TestShardRouting.newShardRouting(
            "test",
            0,
            "node1",
            randomBoolean(),
            ShardRoutingState.INITIALIZING
        );
        ShardRouting initializingShard1 = TestShardRouting.newShardRouting(
            "test",
            1,
            "node1",
            randomBoolean(),
            ShardRoutingState.INITIALIZING
        );
        assertFalse(initializingShard0.isRelocationTarget());
        ShardRouting startedShard0 = initializingShard0.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        assertFalse(startedShard0.isRelocationTarget());
        assertFalse(initializingShard1.isRelocationTarget());
        ShardRouting startedShard1 = initializingShard1.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
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

        Integer[] changeIds = new Integer[] { 0, 1, 2, 3, 4, 5, 6 };
        for (int changeId : randomSubsetOf(randomIntBetween(1, changeIds.length), changeIds)) {
            boolean unchanged = false;
            switch (changeId) {
                case 0:
                    // change index
                    ShardId shardId = new ShardId(new Index("blubb", randomAlphaOfLength(10)), otherRouting.id());
                    otherRouting = new ShardRouting(
                        shardId,
                        otherRouting.currentNodeId(),
                        otherRouting.relocatingNodeId(),
                        otherRouting.primary(),
                        otherRouting.state(),
                        otherRouting.recoverySource(),
                        otherRouting.unassignedInfo(),
                        otherRouting.relocationFailureInfo(),
                        otherRouting.allocationId(),
                        otherRouting.getExpectedShardSize(),
                        otherRouting.role()
                    );
                    break;
                case 1:
                    // change shard id
                    otherRouting = new ShardRouting(
                        new ShardId(otherRouting.index(), otherRouting.id() + 1),
                        otherRouting.currentNodeId(),
                        otherRouting.relocatingNodeId(),
                        otherRouting.primary(),
                        otherRouting.state(),
                        otherRouting.recoverySource(),
                        otherRouting.unassignedInfo(),
                        otherRouting.relocationFailureInfo(),
                        otherRouting.allocationId(),
                        otherRouting.getExpectedShardSize(),
                        otherRouting.role()
                    );
                    break;
                case 2:
                    // change current node
                    if (otherRouting.assignedToNode() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(
                            otherRouting.shardId(),
                            otherRouting.currentNodeId() + "_1",
                            otherRouting.relocatingNodeId(),
                            otherRouting.primary(),
                            otherRouting.state(),
                            otherRouting.recoverySource(),
                            otherRouting.unassignedInfo(),
                            otherRouting.relocationFailureInfo(),
                            otherRouting.allocationId(),
                            otherRouting.getExpectedShardSize(),
                            otherRouting.role()
                        );
                    }
                    break;
                case 3:
                    // change relocating node
                    if (otherRouting.relocating() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(
                            otherRouting.shardId(),
                            otherRouting.currentNodeId(),
                            otherRouting.relocatingNodeId() + "_1",
                            otherRouting.primary(),
                            otherRouting.state(),
                            otherRouting.recoverySource(),
                            otherRouting.unassignedInfo(),
                            otherRouting.relocationFailureInfo(),
                            otherRouting.allocationId(),
                            otherRouting.getExpectedShardSize(),
                            otherRouting.role()
                        );
                    }
                    break;
                case 4:
                    // change recovery source (only works for inactive primaries)
                    if (otherRouting.active() || otherRouting.primary() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(
                            otherRouting.shardId(),
                            otherRouting.currentNodeId(),
                            otherRouting.relocatingNodeId(),
                            otherRouting.primary(),
                            otherRouting.state(),
                            new RecoverySource.SnapshotRecoverySource(
                                UUIDs.randomBase64UUID(),
                                new Snapshot("test", new SnapshotId("s1", UUIDs.randomBase64UUID())),
                                Version.CURRENT,
                                new IndexId("test", UUIDs.randomBase64UUID(random()))
                            ),
                            otherRouting.unassignedInfo(),
                            otherRouting.relocationFailureInfo(),
                            otherRouting.allocationId(),
                            otherRouting.getExpectedShardSize(),
                            otherRouting.role()
                        );
                    }
                    break;
                case 5:
                    // change primary flag
                    otherRouting = TestShardRouting.newShardRouting(
                        otherRouting.getIndexName(),
                        otherRouting.id(),
                        otherRouting.currentNodeId(),
                        otherRouting.relocatingNodeId(),
                        otherRouting.primary() == false,
                        otherRouting.state(),
                        otherRouting.unassignedInfo()
                    );
                    break;
                case 6:
                    // change state
                    ShardRoutingState newState = randomValueOtherThan(otherRouting.state(), () -> randomFrom(ShardRoutingState.values()));
                    otherRouting = TestShardRouting.newShardRouting(
                        otherRouting.getIndexName(),
                        otherRouting.id(),
                        newState == ShardRoutingState.UNASSIGNED ? null : Objects.requireNonNullElse(otherRouting.currentNodeId(), "1"),
                        newState == ShardRoutingState.RELOCATING ? "2" : null,
                        otherRouting.primary(),
                        newState,
                        newState == ShardRoutingState.UNASSIGNED || newState == ShardRoutingState.INITIALIZING
                            ? Objects.requireNonNullElse(
                                otherRouting.unassignedInfo(),
                                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
                            )
                            : null
                    );
                    break;
            }

            if (randomBoolean() && otherRouting.state() == ShardRoutingState.UNASSIGNED) {
                // change unassigned info
                otherRouting = TestShardRouting.newShardRouting(
                    otherRouting.getIndexName(),
                    otherRouting.id(),
                    otherRouting.currentNodeId(),
                    otherRouting.relocatingNodeId(),
                    otherRouting.primary(),
                    otherRouting.state(),
                    otherRouting.unassignedInfo() == null
                        ? new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
                        : new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, otherRouting.unassignedInfo().getMessage() + "_1")
                );
            }

            if (unchanged == false) {
                logger.debug("comparing\nthis  {} to\nother {}", routing, otherRouting);
                assertFalse(
                    "expected non-equality\nthis  " + routing + ",\nother " + otherRouting,
                    routing.equalsIgnoringMetadata(otherRouting)
                );
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
                    routing = routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                    assertEquals(-1, routing.getExpectedShardSize());
                    assertFalse(routing.toString(), routing.toString().contains("expected_shard_size[" + byteSize + "]"));
                }
            } else {
                assertFalse(routing.toString(), routing.toString().contains("expected_shard_size [" + byteSize + "]"));
                assertEquals(byteSize, routing.getExpectedShardSize());
            }
        }
    }

    public void testSummaryContainsImportantFields() {
        var shard = createTestInstance();

        assertThat("index name", shard.shortSummary(), containsString('[' + shard.getIndexName() + ']'));
        assertThat("shard id", shard.shortSummary(), containsString(shard.shardId().toString()));
        assertThat("primary/replica", shard.shortSummary(), containsString(shard.primary() ? "[P]" : "[R]"));
        assertThat("current node id", shard.shortSummary(), containsString("node[" + shard.currentNodeId() + ']'));
        if (shard.relocating()) {
            assertThat("relocating node id", shard.shortSummary(), containsString("relocating [" + shard.relocatingNodeId() + ']'));
        }
        assertThat("state", shard.shortSummary(), containsString("s[" + shard.state() + "]"));
        if (shard.role() != ShardRouting.Role.DEFAULT) {
            assertThat("role", shard.shortSummary(), containsString("[" + shard.role() + "]"));
        }
    }
}
