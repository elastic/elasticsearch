/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.TreeSet;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class ReactiveStorageDeciderReasonWireSerializationTests extends AbstractWireSerializingTestCase<
    ReactiveStorageDeciderService.ReactiveReason> {

    @Override
    protected Writeable.Reader<ReactiveStorageDeciderService.ReactiveReason> instanceReader() {
        return ReactiveStorageDeciderService.ReactiveReason::new;
    }

    @Override
    protected ReactiveStorageDeciderService.ReactiveReason mutateInstance(ReactiveStorageDeciderService.ReactiveReason instance) {
        switch (between(0, 8)) {
            case 0:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    randomValueOtherThan(instance.summary(), () -> randomAlphaOfLength(10)),
                    instance.unassigned(),
                    instance.unassignedShardIds(),
                    instance.assigned(),
                    instance.assignedShardIds(),
                    instance.unassignedAllocationResults(),
                    instance.assignedAllocationResults()
                );
            case 1:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    randomValueOtherThan(instance.unassigned(), ESTestCase::randomNonNegativeLong),
                    instance.unassignedShardIds(),
                    instance.assigned(),
                    instance.assignedShardIds(),
                    instance.unassignedAllocationResults(),
                    instance.assignedAllocationResults()
                );
            case 2:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.unassignedShardIds(),
                    randomValueOtherThan(instance.assigned(), ESTestCase::randomNonNegativeLong),
                    instance.assignedShardIds(),
                    instance.unassignedAllocationResults(),
                    instance.assignedAllocationResults()
                );
            case 3:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    instance.assigned(),
                    instance.assignedShardIds(),
                    instance.unassignedAllocationResults(),
                    instance.assignedAllocationResults()
                );
            case 4:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.unassignedShardIds(),
                    instance.assigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    instance.unassignedAllocationResults(),
                    instance.assignedAllocationResults()
                );
            case 5:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    instance.assigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    instance.unassignedAllocationResults(),
                    instance.assignedAllocationResults()
                );
            case 6:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.unassignedShardIds(),
                    instance.assigned(),
                    instance.assignedShardIds(),
                    randomList(8, this::randomNodeAllocationResult),
                    instance.assignedAllocationResults()
                );
            case 7:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.unassignedShardIds(),
                    instance.assigned(),
                    instance.assignedShardIds(),
                    instance.unassignedAllocationResults(),
                    randomList(8, this::randomNodeAllocationResult)
                );
            case 8:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.unassignedShardIds(),
                    instance.assigned(),
                    instance.assignedShardIds(),
                    randomList(8, this::randomNodeAllocationResult),
                    randomList(8, this::randomNodeAllocationResult)
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
            randomList(8, this::randomNodeAllocationResult),
            randomList(8, this::randomNodeAllocationResult)
        );
    }

    private NodeAllocationResult randomNodeAllocationResult() {
        return new NodeAllocationResult(
            new DiscoveryNode("node1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            randomFrom(Decision.NO, Decision.YES, Decision.THROTTLE),
            1
        );
    }
}
