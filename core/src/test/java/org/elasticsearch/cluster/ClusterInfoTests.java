/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

public class ClusterInfoTests extends ESTestCase {

    public void testSerialization() throws Exception {
        ClusterInfo clusterInfo = new ClusterInfo(
                randomDiskUsage(), randomDiskUsage(), randomShardSizes(), randomRoutingToDataPath()
        );
        BytesStreamOutput output = new BytesStreamOutput();
        clusterInfo.writeTo(output);

        ClusterInfo result = new ClusterInfo(output.bytes().streamInput());
        assertEquals(clusterInfo.getNodeLeastAvailableDiskUsages(), result.getNodeLeastAvailableDiskUsages());
        assertEquals(clusterInfo.getNodeMostAvailableDiskUsages(), result.getNodeMostAvailableDiskUsages());
        assertEquals(clusterInfo.shardSizes, result.shardSizes);
        assertEquals(clusterInfo.routingToDataPath, result.routingToDataPath);
    }

    private static ImmutableOpenMap<String, DiskUsage> randomDiskUsage() {
        int numEntries = randomIntBetween(0, 128);
        ImmutableOpenMap.Builder<String, DiskUsage> builder = ImmutableOpenMap.builder(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAsciiOfLength(32);
            DiskUsage diskUsage = new DiskUsage(
                    randomAsciiOfLength(4), randomAsciiOfLength(4), randomAsciiOfLength(4),
                    randomIntBetween(0, Integer.MAX_VALUE), randomIntBetween(0, Integer.MAX_VALUE)
            );
            builder.put(key, diskUsage);
        }
        return builder.build();
    }

    private static ImmutableOpenMap<String, Long> randomShardSizes() {
        int numEntries = randomIntBetween(0, 128);
        ImmutableOpenMap.Builder<String, Long> builder = ImmutableOpenMap.builder(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAsciiOfLength(32);
            long shardSize = randomIntBetween(0, Integer.MAX_VALUE);
            builder.put(key, shardSize);
        }
        return builder.build();
    }

    private static ImmutableOpenMap<ShardRouting, String> randomRoutingToDataPath() {
        int numEntries = randomIntBetween(0, 128);
        ImmutableOpenMap.Builder<ShardRouting, String> builder = ImmutableOpenMap.builder(numEntries);
        for (int i = 0; i < numEntries; i++) {
            RestoreSource restoreSource = new RestoreSource(new Snapshot(randomAsciiOfLength(4),
                    new SnapshotId(randomAsciiOfLength(4), randomAsciiOfLength(4))), Version.CURRENT, randomAsciiOfLength(4));
            UnassignedInfo.Reason reason = randomFrom(UnassignedInfo.Reason.values());
            UnassignedInfo unassignedInfo = new UnassignedInfo(reason, randomAsciiOfLength(4));
            ShardId shardId = new ShardId(randomAsciiOfLength(32), randomAsciiOfLength(32), randomIntBetween(0, Integer.MAX_VALUE));
            ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, restoreSource, randomBoolean(), unassignedInfo);
            builder.put(shardRouting, randomAsciiOfLength(32));
        }
        return builder.build();
    }

}
