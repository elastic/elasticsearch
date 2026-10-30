/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportGetShardWarmAmountShareAction.NodeRequest;

public class GetShardWarmAmountShareNodeRequestSerializationTests extends AbstractWireSerializingTestCase<NodeRequest> {

    @Override
    protected Writeable.Reader<NodeRequest> instanceReader() {
        return NodeRequest::new;
    }

    @Override
    protected NodeRequest createTestInstance() {
        return new NodeRequest(randomIdentifier(), randomShardId(), randomNonNegativeLong());
    }

    @Override
    protected NodeRequest mutateInstance(NodeRequest instance) {
        return switch (between(0, 2)) {
            case 0 -> new NodeRequest(
                randomValueOtherThan(instance.targetNodeId(), ESTestCase::randomIdentifier),
                instance.shardId(),
                instance.shutdownSignalTimestamp()
            );
            case 1 -> new NodeRequest(
                instance.targetNodeId(),
                randomValueOtherThan(instance.shardId(), GetShardWarmAmountShareNodeRequestSerializationTests::randomShardId),
                instance.shutdownSignalTimestamp()
            );
            case 2 -> new NodeRequest(
                instance.targetNodeId(),
                instance.shardId(),
                randomValueOtherThan(instance.shutdownSignalTimestamp(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError("unreachable");
        };
    }

    private static ShardId randomShardId() {
        return new ShardId(randomIdentifier(), UUIDs.randomBase64UUID(random()), between(0, 10));
    }
}
