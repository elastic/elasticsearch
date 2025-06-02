/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
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
        return randomClusterInfo();
    }

    @Override
    protected ClusterInfo mutateInstance(ClusterInfo instance) {
        return randomClusterInfo();
    }

    public static ClusterInfo randomClusterInfo() {
        return new ClusterInfo(
            randomDiskUsage(),
            randomDiskUsage(),
            randomShardSizes(),
            randomDataSetSizes(),
            randomRoutingToDataPath(),
            randomReservedSpace(),
            randomNodeHeapUsage()
        );
    }

    private static Map<String, HeapUsage> randomNodeHeapUsage() {
        int numEntries = randomIntBetween(0, 128);
        Map<String, HeapUsage> nodeHeapUsage = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            final int totalBytes = randomIntBetween(0, Integer.MAX_VALUE);
            final HeapUsage diskUsage = new HeapUsage(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                totalBytes,
                randomIntBetween(0, totalBytes)
            );
            nodeHeapUsage.put(key, diskUsage);
        }
        return nodeHeapUsage;
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
        var builder = Maps.<String, Long>newMapWithExpectedSize(numEntries);
        for (int i = 0; i < numEntries; i++) {
            builder.put(ClusterInfo.shardIdentifierFromRouting(randomShardId(), randomBoolean()), randomLongBetween(0, Integer.MAX_VALUE));
        }
        return builder;
    }

    private static Map<ShardId, Long> randomDataSetSizes() {
        int numEntries = randomIntBetween(0, 128);
        var builder = Maps.<ShardId, Long>newMapWithExpectedSize(numEntries);
        for (int i = 0; i < numEntries; i++) {
            builder.put(randomShardId(), randomLongBetween(0, Integer.MAX_VALUE));
        }
        return builder;
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), between(0, Integer.MAX_VALUE));
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

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(createTestInstance(), ClusterInfoTests::getChunkCount);
    }

    // exposing this to tests in other packages
    public static int getChunkCount(ClusterInfo clusterInfo) {
        return clusterInfo.getChunkCount();
    }
}
