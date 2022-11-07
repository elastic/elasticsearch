/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Set;

public class CheckShardsOnDataPathRequestSerializationTests extends AbstractWireSerializingTestCase<CheckShardsOnDataPathRequest> {

    @Override
    protected Writeable.Reader<CheckShardsOnDataPathRequest> instanceReader() {
        return CheckShardsOnDataPathRequest::new;
    }

    @Override
    protected CheckShardsOnDataPathRequest createTestInstance() {
        Set<ShardId> shardIds = randomSet(0, 100, CheckShardsOnDataPathRequestSerializationTests::randomShardId);
        String[] nodeIds = randomArray(1, 5, String[]::new, () -> randomAlphaOfLength(20));
        CheckShardsOnDataPathRequest request = new CheckShardsOnDataPathRequest(shardIds, nodeIds);
        return randomBoolean() ? request : request.timeout(randomTimeValue());
    }

    @Override
    protected CheckShardsOnDataPathRequest mutateInstance(CheckShardsOnDataPathRequest request) throws IOException {
        int i = randomInt(2);
        return switch (i) {
            case 0 -> new CheckShardsOnDataPathRequest(
                randomSet(0, 50, CheckShardsOnDataPathRequestSerializationTests::randomShardId),
                request.nodesIds()
            ).timeout(request.timeout());
            case 1 -> new CheckShardsOnDataPathRequest(
                request.getShardIds(),
                randomArray(1, 10, String[]::new, () -> randomAlphaOfLength(20))
            ).timeout(request.timeout());
            case 2 -> new CheckShardsOnDataPathRequest(request.getShardIds(), request.nodesIds()).timeout(randomTimeValue());
            default -> throw new IllegalStateException("unexpected value: " + i);
        };
    }

    public static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }
}
