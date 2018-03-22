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

package org.elasticsearch.action.admin.indices.flush;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.test.ESTestCase;

public abstract class AbstractSyncedFlushTest extends ESTestCase {
    protected static class TestPlan {
        public SyncedFlushResponse.ShardCounts totalCounts;
        public Map<String, SyncedFlushResponse.ShardCounts> countsPerIndex = new HashMap<>();
        public ObjectIntMap<String> expectedFailuresPerIndex = new ObjectIntHashMap<>();
        public SyncedFlushResponse result;
    }

    protected TestPlan createTestPlan() {
        final TestPlan testPlan = new TestPlan();
        final Map<String, List<ShardsSyncedFlushResult>> indicesResults = new HashMap<>();
        final int indexCount = randomIntBetween(1, 10);
        int totalShards = 0;
        int totalSuccesful = 0;
        int totalFailed = 0;
        for (int i = 0; i < indexCount; i++) {
            final String index = "index_" + i;
            int shards = randomIntBetween(1, 4);
            int replicas = randomIntBetween(0, 2);
            int successful = 0;
            int failed = 0;
            int failures = 0;
            List<ShardsSyncedFlushResult> shardsResults = new ArrayList<>();
            for (int shard = 0; shard < shards; shard++) {
                final ShardId shardId = new ShardId(index, "_na_", shard);
                if (randomInt(5) < 2) {
                    // total shard failure
                    failed += replicas + 1;
                    failures++;
                    shardsResults.add(new ShardsSyncedFlushResult(shardId, replicas + 1, "simulated total failure"));
                } else {
                    Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardResponses = new HashMap<>();
                    for (int copy = 0; copy < replicas + 1; copy++) {
                        final ShardRouting shardRouting =
                            TestShardRouting.newShardRouting(
                                index, shard, "node_" + shardId + "_" + copy, null,
                                copy == 0, ShardRoutingState.STARTED
                            );
                        if (randomInt(5) < 2) {
                            // shard copy failure
                            failed++;
                            failures++;
                            shardResponses.put(shardRouting, new SyncedFlushService.ShardSyncedFlushResponse("copy failure " + shardId));
                        } else {
                            successful++;
                            shardResponses.put(shardRouting, new SyncedFlushService.ShardSyncedFlushResponse());
                        }
                    }
                    shardsResults.add(new ShardsSyncedFlushResult(shardId, "_sync_id_" + shard, replicas + 1, shardResponses));
                }
            }
            indicesResults.put(index, shardsResults);
            testPlan.countsPerIndex.put(index, new SyncedFlushResponse.ShardCounts(shards * (replicas + 1), successful, failed));
            testPlan.expectedFailuresPerIndex.put(index, failures);
            totalFailed += failed;
            totalShards += shards * (replicas + 1);
            totalSuccesful += successful;
        }
        testPlan.result = new SyncedFlushResponse(indicesResults);
        testPlan.totalCounts = new SyncedFlushResponse.ShardCounts(totalShards, totalSuccesful, totalFailed);
        return testPlan;
    }

    public void assertShardCounts(SyncedFlushResponse.ShardCounts first, SyncedFlushResponse.ShardCounts second) {
        if (first == null) {
            assertNull(second);
        } else {
            assertNotNull(second);
            assertEquals(first.successful, second.successful);
            assertEquals(first.failed, second.failed);
            assertEquals(first.total, second.total);
        }
    }
}
