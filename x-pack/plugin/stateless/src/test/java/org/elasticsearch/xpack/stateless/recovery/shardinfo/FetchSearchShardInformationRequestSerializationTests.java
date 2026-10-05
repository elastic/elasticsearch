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
import org.elasticsearch.xpack.stateless.recovery.shardinfo.TransportFetchSearchShardInformationAction.Request;

public class FetchSearchShardInformationRequestSerializationTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomBoolean() ? null : randomIdentifier(), randomShardId());
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> new Request(
                randomValueOtherThan(instance.getNodeId(), () -> randomBoolean() ? null : randomIdentifier()),
                instance.getShardId()
            );
            case 1 -> new Request(
                instance.getNodeId(),
                randomValueOtherThan(instance.getShardId(), FetchSearchShardInformationRequestSerializationTests::randomShardId)
            );
            default -> throw new AssertionError("unreachable");
        };
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }
}
