/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.Matchers;

import static org.elasticsearch.snapshots.ShardSnapshotResultWireSerializationTests.randomShardSnapshotResult;
import static org.hamcrest.Matchers.containsString;

public class ShardSnapshotStatusWireSerializationTests extends AbstractWireSerializingTestCase<SnapshotsInProgress.ShardSnapshotStatus> {
    @Override
    protected Writeable.Reader<SnapshotsInProgress.ShardSnapshotStatus> instanceReader() {
        return SnapshotsInProgress.ShardSnapshotStatus::readFrom;
    }

    @Override
    protected SnapshotsInProgress.ShardSnapshotStatus createTestInstance() {
        final SnapshotsInProgress.ShardState shardState = randomFrom(SnapshotsInProgress.ShardState.values());
        final String nodeId = randomAlphaOfLength(5);
        if (shardState == SnapshotsInProgress.ShardState.QUEUED) {
            return SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED;
        } else if (shardState == SnapshotsInProgress.ShardState.SUCCESS) {
            return SnapshotsInProgress.ShardSnapshotStatus.success(nodeId, randomShardSnapshotResult());
        } else {
            final String reason = shardState.failed() ? randomAlphaOfLength(10) : null;
            return new SnapshotsInProgress.ShardSnapshotStatus(nodeId, shardState, ShardGeneration.newGeneration(), reason);
        }
    }

    @Override
    protected SnapshotsInProgress.ShardSnapshotStatus mutateInstance(SnapshotsInProgress.ShardSnapshotStatus instance) {
        if (instance.state() == SnapshotsInProgress.ShardState.QUEUED) {
            assert instance == SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED;
            return randomValueOtherThanMany(i -> i.state() == SnapshotsInProgress.ShardState.QUEUED, this::createTestInstance);
        }

        final SnapshotsInProgress.ShardState newState = randomFrom(SnapshotsInProgress.ShardState.values());
        if (newState == SnapshotsInProgress.ShardState.QUEUED) {
            return SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED;
        } else if (newState == SnapshotsInProgress.ShardState.SUCCESS) {
            if (instance.state() == SnapshotsInProgress.ShardState.SUCCESS) {
                assert instance.shardSnapshotResult() != null;
                if (randomBoolean()) {
                    return SnapshotsInProgress.ShardSnapshotStatus.success(
                        randomAlphaOfLength(11 - instance.nodeId().length()),
                        instance.shardSnapshotResult()
                    );
                } else {
                    return SnapshotsInProgress.ShardSnapshotStatus.success(
                        instance.nodeId(),
                        ShardSnapshotResultWireSerializationTests.mutateShardSnapshotResult(instance.shardSnapshotResult())
                    );
                }
            } else {
                return SnapshotsInProgress.ShardSnapshotStatus.success(instance.nodeId(), randomShardSnapshotResult());
            }
        } else if (newState.failed() && instance.state().failed() && randomBoolean()) {
            return new SnapshotsInProgress.ShardSnapshotStatus(
                instance.nodeId(),
                newState,
                instance.generation(),
                randomAlphaOfLength(15 - instance.reason().length())
            );
        } else {
            final String reason = newState.failed() ? randomAlphaOfLength(10) : null;
            if (newState != instance.state() && randomBoolean()) {
                return new SnapshotsInProgress.ShardSnapshotStatus(instance.nodeId(), newState, instance.generation(), reason);
            } else if (randomBoolean()) {
                return new SnapshotsInProgress.ShardSnapshotStatus(
                    randomAlphaOfLength(11 - instance.nodeId().length()),
                    newState,
                    instance.generation(),
                    reason
                );
            } else {
                return new SnapshotsInProgress.ShardSnapshotStatus(
                    instance.nodeId(),
                    newState,
                    randomValueOtherThan(instance.generation(), ShardGeneration::newGeneration),
                    reason
                );
            }
        }
    }

    @Override
    protected void assertEqualInstances(
        SnapshotsInProgress.ShardSnapshotStatus expectedInstance,
        SnapshotsInProgress.ShardSnapshotStatus newInstance
    ) {
        if (newInstance.state() == SnapshotsInProgress.ShardState.QUEUED) {
            assertSame(newInstance, expectedInstance);
        } else {
            assertNotSame(newInstance, expectedInstance);
        }
        assertThat(expectedInstance, Matchers.equalTo(newInstance));
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    public void testToString() {
        final SnapshotsInProgress.ShardSnapshotStatus testInstance = createTestInstance();
        if (testInstance.nodeId() != null) {
            assertThat(testInstance.toString(), containsString(testInstance.nodeId()));
        }
        if (testInstance.generation() != null) {
            assertThat(testInstance.toString(), containsString(testInstance.generation().toString()));
        }
        if (testInstance.state() == SnapshotsInProgress.ShardState.SUCCESS) {
            assertThat(testInstance.toString(), containsString(testInstance.shardSnapshotResult().toString()));
        }
    }

}
