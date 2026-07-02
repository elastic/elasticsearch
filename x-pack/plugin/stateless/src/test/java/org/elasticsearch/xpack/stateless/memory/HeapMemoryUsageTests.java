/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

public class HeapMemoryUsageTests extends AbstractWireSerializingTestCase<HeapMemoryUsage> {
    @Override
    protected Writeable.Reader<HeapMemoryUsage> instanceReader() {
        return HeapMemoryUsage::from;
    }

    @Override
    protected HeapMemoryUsage createTestInstance() {
        return randomHeapMemoryUsage();
    }

    @Override
    protected HeapMemoryUsage mutateInstance(HeapMemoryUsage instance) throws IOException {
        return mutate(instance);
    }

    public static HeapMemoryUsage randomHeapMemoryUsage() {
        return new HeapMemoryUsage(randomNonNegativeLong(), randomShardMappingSizes(), randomNonNegativeLong());
    }

    public static HeapMemoryUsage mutate(HeapMemoryUsage in) {
        return switch (between(0, 2)) {
            case 0 -> new HeapMemoryUsage(
                randomValueOtherThan(in.publicationSeqNo(), ESTestCase::randomNonNegativeLong),
                in.shardMappingSizes(),
                in.clusterStateVersion()
            );
            case 1 -> new HeapMemoryUsage(
                in.publicationSeqNo(),
                randomValueOtherThan(in.shardMappingSizes(), HeapMemoryUsageTests::randomShardMappingSizes),
                in.clusterStateVersion()
            );
            case 2 -> new HeapMemoryUsage(
                in.publicationSeqNo(),
                in.shardMappingSizes(),
                randomValueOtherThan(in.clusterStateVersion(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError("invalid option");
        };
    }

    private static Map<ShardId, ShardMappingSize> randomShardMappingSizes() {
        int numShards = randomIntBetween(0, 5);
        Map<ShardId, ShardMappingSize> shards = Maps.newMapWithExpectedSize(numShards);
        for (int i = 0; i < numShards; i++) {
            var shardId = new ShardId(new Index(randomAlphaOfLengthBetween(1, 128), randomAlphaOfLengthBetween(1, 128)), between(0, 2));
            var metrics = new ShardMappingSize(
                randomNonNegativeLong(),
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomAlphaOfLength(64)
            );
            shards.put(shardId, metrics);
        }
        return shards;
    }
}
