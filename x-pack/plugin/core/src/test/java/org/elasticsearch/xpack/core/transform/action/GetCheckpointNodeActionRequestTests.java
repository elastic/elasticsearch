/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction.Request;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GetCheckpointNodeActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        Set<ShardId> shards = new HashSet<>();
        int numberOfRandomShardIds = randomInt(10);

        for (int i = 0; i < numberOfRandomShardIds; ++i) {
            shards.add(new ShardId(randomAlphaOfLength(4) + i, randomAlphaOfLength(4), randomInt(5)));
        }

        return new Request(shards);
    }

    @Override
    protected Request mutateInstance(Request instance) throws IOException {
        Set<ShardId> shards = new HashSet<>(instance.getShards());

        if (randomBoolean() && shards.size() > 0) {
            ShardId firstShard = shards.iterator().next();
            shards.remove(firstShard);
            if (randomBoolean()) {
                shards.add(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(4), randomInt(5)));
            }
        } else {
            shards.add(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(4), randomInt(5)));
        }

        return new Request(shards);
    }

}
