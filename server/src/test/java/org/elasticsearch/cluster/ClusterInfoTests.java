/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.Map;

public class ClusterInfoTests extends AbstractWireSerializingTestCase<ClusterInfo> {

    @Override
    protected Writeable.Reader<ClusterInfo> instanceReader() {
        return ClusterInfo::new;
    }

    @Override
    protected ClusterInfo createTestInstance() {
        return new ClusterInfo(
            randomDiskUsage(),
            randomDiskUsage(),
            randomShardSizes(),
            randomDataSetSizes(),
            randomRoutingToDataPath(),
            randomReservedSpace()
        );
    }

    @Override
    protected ClusterInfo mutateInstance(ClusterInfo instance) {
        return createTestInstance();
    }

    private static Map<String, DiskUsage> randomDiskUsage() {
        int numEntries = randomIntBetween(0, 128);
        Map<String, DiskUsage> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            final int totalBytes = randomIntBetween(0, Integer.MAX_VALUE);
            DiskUsage diskUsage = new DiskUsage(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                totalBytes,
                randomIntBetween(0, totalBytes)
            );
            builder.put(key, diskUsage);
        }
        return builder;
    }

    private static Map<String, Long> randomShardSizes() {
        int numEntries = randomIntBetween(0, 128);
        Map<String, Long> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            long shardSize = randomIntBetween(0, Integer.MAX_VALUE);
            builder.put(key, shardSize);
        }
        return builder;
    }

    private static Map<ShardId, Long> randomDataSetSizes() {
        int numEntries = randomIntBetween(0, 128);
        Map<ShardId, Long> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            ShardId key = new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), between(0, Integer.MAX_VALUE));
            long shardSize = randomIntBetween(0, Integer.MAX_VALUE);
            builder.put(key, shardSize);
        }
        return builder;
    }

    private static Map<ClusterInfo.NodeAndShard, String> randomRoutingToDataPath() {
        int numEntries = randomIntBetween(0, 128);
        Map<ClusterInfo.NodeAndShard, String> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            ShardId shardId = new ShardId(randomAlphaOfLength(32), randomAlphaOfLength(32), randomIntBetween(0, Integer.MAX_VALUE));
            builder.put(new ClusterInfo.NodeAndShard(randomAlphaOfLength(10), shardId), randomAlphaOfLength(32));
        }
        return builder;
    }

    private static Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> randomReservedSpace() {
        int numEntries = randomIntBetween(0, 128);
        Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            final ClusterInfo.ReservedSpace.Builder valueBuilder = new ClusterInfo.ReservedSpace.Builder();
            for (int j = between(0, 10); j > 0; j--) {
                ShardId shardId = new ShardId(randomAlphaOfLength(32), randomAlphaOfLength(32), randomIntBetween(0, Integer.MAX_VALUE));
                valueBuilder.add(shardId, between(0, Integer.MAX_VALUE));
            }
            builder.put(new ClusterInfo.NodeAndPath(randomAlphaOfLength(10), randomAlphaOfLength(10)), valueBuilder.build());
        }
        return builder;
    }

}
