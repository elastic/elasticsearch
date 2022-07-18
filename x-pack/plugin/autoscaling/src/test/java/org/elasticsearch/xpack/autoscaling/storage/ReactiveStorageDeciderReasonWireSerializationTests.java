/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.TreeSet;

public class ReactiveStorageDeciderReasonWireSerializationTests extends AbstractWireSerializingTestCase<
    ReactiveStorageDeciderService.ReactiveReason> {

    @Override
    protected Writeable.Reader<ReactiveStorageDeciderService.ReactiveReason> instanceReader() {
        return ReactiveStorageDeciderService.ReactiveReason::new;
    }

    @Override
    protected ReactiveStorageDeciderService.ReactiveReason mutateInstance(ReactiveStorageDeciderService.ReactiveReason instance) {
        switch (between(0, 5)) {
            case 0:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    randomValueOtherThan(instance.summary(), () -> randomAlphaOfLength(10)),
                    instance.unassigned(),
                    instance.unassignedShardIds(),
                    instance.assigned(),
                    instance.assignedShardIds(),
                    instance.unassignedShardAllocateDecision()
                );
            case 1:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    randomValueOtherThan(instance.unassigned(), ESTestCase::randomNonNegativeLong),
                    instance.unassignedShardIds(),
                    instance.assigned(),
                    instance.assignedShardIds(),
                    instance.unassignedShardAllocateDecision()
                );
            case 2:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.unassignedShardIds(),
                    randomValueOtherThan(instance.assigned(), ESTestCase::randomNonNegativeLong),
                    instance.assignedShardIds(),
                    instance.unassignedShardAllocateDecision()
                );
            case 3:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    instance.assigned(),
                    instance.assignedShardIds(),
                    instance.unassignedShardAllocateDecision()
                );
            case 4:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.unassignedShardIds(),
                    instance.assigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    instance.unassignedShardAllocateDecision()
                );
            case 5:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    instance.assigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    instance.unassignedShardAllocateDecision()
                );
            default:
                fail("unexpected");
        }
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ReactiveStorageDeciderService.ReactiveReason createTestInstance() {
        return new ReactiveStorageDeciderService.ReactiveReason(
            randomAlphaOfLength(10),
            randomNonNegativeLong(),
            new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
            randomNonNegativeLong(),
            new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
            AllocateUnassignedDecision.no(
                randomFrom(
                    UnassignedInfo.AllocationStatus.DECIDERS_NO,
                    UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION,
                    UnassignedInfo.AllocationStatus.NO_VALID_SHARD_COPY,
                    UnassignedInfo.AllocationStatus.FETCHING_SHARD_DATA
                ),
                List.of(),
                randomBoolean()
            )
        );
    }
}
