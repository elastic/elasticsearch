/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.action.get.TransportShardMultiGetFomTranslogAction.Request;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.UUID;

public class ShardMultiGetFromTranslogRequestSerializationTests extends AbstractWireSerializingTestCase<Request> {
    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomMultiGetShardRequest(), randomShardId());
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {
        return randomBoolean()
            ? new Request(instance.getMultiGetShardRequest(), randomValueOtherThan(instance.getShardId(), this::randomShardId))
            : new Request(
                randomValueOtherThan(instance.getMultiGetShardRequest(), this::randomMultiGetShardRequest),
                instance.getShardId()
            );
    }

    private ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(10), UUID.randomUUID().toString(), randomIntBetween(0, 5));
    }

    private MultiGetShardRequest randomMultiGetShardRequest() {
        return MultiGetShardRequestTests.createTestInstance(randomBoolean());
    }
}
