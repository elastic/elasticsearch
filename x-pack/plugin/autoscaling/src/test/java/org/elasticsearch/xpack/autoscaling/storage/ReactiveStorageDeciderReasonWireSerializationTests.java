/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

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
                    instance.assigned(),
                    instance.unassignedShardIds(),
                    instance.assignedShardIds()
                );
            case 1:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    randomValueOtherThan(instance.unassigned(), ESTestCase::randomNonNegativeLong),
                    instance.assigned(),
                    instance.unassignedShardIds(),
                    instance.assignedShardIds()
                );
            case 2:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    randomValueOtherThan(instance.assigned(), ESTestCase::randomNonNegativeLong),
                    instance.unassignedShardIds(),
                    instance.assignedShardIds()
                );
            case 3:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.assigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId("index", UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    instance.assignedShardIds()
                );
            case 4:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.assigned(),
                    instance.unassignedShardIds(),
                    new TreeSet<>(randomUnique(() -> new ShardId("index", UUIDs.randomBase64UUID(), randomInt(5)), 8))
                );
            case 5:
                return new ReactiveStorageDeciderService.ReactiveReason(
                    instance.summary(),
                    instance.unassigned(),
                    instance.assigned(),
                    new TreeSet<>(randomUnique(() -> new ShardId("index", UUIDs.randomBase64UUID(), randomInt(5)), 8)),
                    new TreeSet<>(randomUnique(() -> new ShardId("index", UUIDs.randomBase64UUID(), randomInt(5)), 8))
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
            randomNonNegativeLong(),
            new TreeSet<>(randomUnique(() -> new ShardId("index", UUIDs.randomBase64UUID(), randomInt(5)), 8)),
            new TreeSet<>(randomUnique(() -> new ShardId("index", UUIDs.randomBase64UUID(), randomInt(5)), 8))
        );
    }
}
