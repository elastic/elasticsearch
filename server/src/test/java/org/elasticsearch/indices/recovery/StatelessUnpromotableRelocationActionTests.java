/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardIdTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class StatelessUnpromotableRelocationActionTests extends AbstractWireSerializingTestCase<
    StatelessUnpromotableRelocationAction.Request> {
    @Override
    protected Writeable.Reader<StatelessUnpromotableRelocationAction.Request> instanceReader() {
        return StatelessUnpromotableRelocationAction.Request::new;
    }

    @Override
    protected StatelessUnpromotableRelocationAction.Request createTestInstance() {
        return new StatelessUnpromotableRelocationAction.Request(
            randomNonNegativeLong(),
            new ShardId(randomIdentifier(), UUIDs.randomBase64UUID(), randomIntBetween(0, 99)),
            randomUUID(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected StatelessUnpromotableRelocationAction.Request mutateInstance(StatelessUnpromotableRelocationAction.Request instance)
        throws IOException {
        return switch (between(0, 3)) {
            case 0 -> new StatelessUnpromotableRelocationAction.Request(
                randomValueOtherThan(instance.getRecoveryId(), ESTestCase::randomNonNegativeLong),
                instance.getShardId(),
                instance.getTargetAllocationId(),
                instance.getClusterStateVersion()
            );
            case 1 -> new StatelessUnpromotableRelocationAction.Request(
                instance.getRecoveryId(),
                ShardIdTests.mutate(instance.getShardId()),
                instance.getTargetAllocationId(),
                instance.getClusterStateVersion()
            );
            case 2 -> new StatelessUnpromotableRelocationAction.Request(
                instance.getRecoveryId(),
                instance.getShardId(),
                randomValueOtherThan(instance.getTargetAllocationId(), ESTestCase::randomUUID),
                instance.getClusterStateVersion()
            );
            case 3 -> new StatelessUnpromotableRelocationAction.Request(
                instance.getRecoveryId(),
                instance.getShardId(),
                instance.getTargetAllocationId(),
                randomValueOtherThan(instance.getClusterStateVersion(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        };
    }
}
