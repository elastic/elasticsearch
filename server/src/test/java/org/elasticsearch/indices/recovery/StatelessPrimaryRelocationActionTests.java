/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardIdTests;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class StatelessPrimaryRelocationActionTests extends AbstractWireSerializingTestCase<StatelessPrimaryRelocationAction.Request> {

    @Override
    protected Writeable.Reader<StatelessPrimaryRelocationAction.Request> instanceReader() {
        return StatelessPrimaryRelocationAction.Request::new;
    }

    @Override
    protected StatelessPrimaryRelocationAction.Request createTestInstance() {
        return new StatelessPrimaryRelocationAction.Request(
            randomNonNegativeLong(),
            new ShardId(randomIdentifier(), UUIDs.randomBase64UUID(), randomIntBetween(0, 99)),
            newDiscoveryNode(),
            UUIDs.randomBase64UUID(),
            randomNonNegativeLong()
        );
    }

    private static DiscoveryNode newDiscoveryNode() {
        return DiscoveryNodeUtils.builder("test").ephemeralId(UUIDs.randomBase64UUID()).build();
    }

    @Override
    protected StatelessPrimaryRelocationAction.Request mutateInstance(StatelessPrimaryRelocationAction.Request instance)
        throws IOException {
        return switch (between(1, 5)) {
            case 1 -> new StatelessPrimaryRelocationAction.Request(
                randomValueOtherThan(instance.recoveryId(), ESTestCase::randomNonNegativeLong),
                instance.shardId(),
                instance.targetNode(),
                instance.targetAllocationId(),
                instance.clusterStateVersion()
            );
            case 2 -> new StatelessPrimaryRelocationAction.Request(
                instance.recoveryId(),
                ShardIdTests.mutate(instance.shardId()),
                instance.targetNode(),
                instance.targetAllocationId(),
                instance.clusterStateVersion()
            );
            case 3 -> new StatelessPrimaryRelocationAction.Request(
                instance.recoveryId(),
                instance.shardId(),
                randomValueOtherThan(instance.targetNode(), StatelessPrimaryRelocationActionTests::newDiscoveryNode),
                instance.targetAllocationId(),
                instance.clusterStateVersion()
            );
            case 4 -> new StatelessPrimaryRelocationAction.Request(
                instance.recoveryId(),
                instance.shardId(),
                instance.targetNode(),
                randomValueOtherThan(instance.targetAllocationId(), UUIDs::randomBase64UUID),
                instance.clusterStateVersion()
            );
            case 5 -> new StatelessPrimaryRelocationAction.Request(
                instance.recoveryId(),
                instance.shardId(),
                instance.targetNode(),
                instance.targetAllocationId(),
                randomValueOtherThan(instance.clusterStateVersion(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError("impossible");
        };
    }
}
