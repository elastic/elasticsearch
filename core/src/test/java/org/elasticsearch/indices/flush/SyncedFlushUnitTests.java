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

package org.elasticsearch.indices.flush;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.flush.IndicesSyncedFlushResult.ShardCounts;
import org.elasticsearch.indices.flush.SyncedFlushService.SyncedFlushResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SyncedFlushUnitTests extends ESTestCase {


    private static class TestPlan {
        public ShardCounts totalCounts;
        public Map<String, ShardCounts> countsPerIndex = new HashMap<>();
        public ObjectIntMap<String> expectedFailuresPerIndex = new ObjectIntHashMap<>();

        public IndicesSyncedFlushResult result;

    }

    public void testIndicesSyncedFlushResult() throws IOException {
        final TestPlan testPlan = createTestPlan();
        assertThat(testPlan.result.totalShards(), equalTo(testPlan.totalCounts.total));
        assertThat(testPlan.result.successfulShards(), equalTo(testPlan.totalCounts.successful));
        assertThat(testPlan.result.failedShards(), equalTo(testPlan.totalCounts.failed));
        assertThat(testPlan.result.restStatus(), equalTo(testPlan.totalCounts.failed > 0 ? RestStatus.CONFLICT : RestStatus.OK));
        Map<String, Object> asMap = convertToMap(testPlan.result);
        assertShardCount("_shards header", (Map<String, Object>) asMap.get("_shards"), testPlan.totalCounts);

        assertThat("unexpected number of indices", asMap.size(), equalTo(1 + testPlan.countsPerIndex.size())); // +1 for the shards header
        for (String index : testPlan.countsPerIndex.keySet()) {
            Map<String, Object> indexMap = (Map<String, Object>) asMap.get(index);
            assertShardCount(index, indexMap, testPlan.countsPerIndex.get(index));
            List<Map<String, Object>> failureList = (List<Map<String, Object>>) indexMap.get("failures");
            final int expectedFailures = testPlan.expectedFailuresPerIndex.get(index);
            if (expectedFailures == 0) {
                assertNull(index + " has unexpected failures", failureList);
            } else {
                assertNotNull(index + " should have failures", failureList);
                assertThat(failureList, hasSize(expectedFailures));
            }
        }
    }

    private void assertShardCount(String name, Map<String, Object> header, ShardCounts expectedCounts) {
        assertThat(name + " has unexpected total count", (Integer) header.get("total"), equalTo(expectedCounts.total));
        assertThat(name + " has unexpected successful count", (Integer) header.get("successful"), equalTo(expectedCounts.successful));
        assertThat(name + " has unexpected failed count", (Integer) header.get("failed"), equalTo(expectedCounts.failed));
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
                final ShardId shardId = new ShardId(index, shard);
                if (randomInt(5) < 2) {
                    // total shard failure
                    failed += replicas + 1;
                    failures++;
                    shardsResults.add(new ShardsSyncedFlushResult(shardId, replicas + 1, "simulated total failure"));
                } else {
                    Map<ShardRouting, SyncedFlushResponse> shardResponses = new HashMap<>();
                    for (int copy = 0; copy < replicas + 1; copy++) {
                        final ShardRouting shardRouting = TestShardRouting.newShardRouting(index, shard, "node_" + shardId + "_" + copy, null,
                                copy == 0, ShardRoutingState.STARTED, 0);
                        if (randomInt(5) < 2) {
                            // shard copy failure
                            failed++;
                            failures++;
                            shardResponses.put(shardRouting, new SyncedFlushResponse("copy failure " + shardId));
                        } else {
                            successful++;
                            shardResponses.put(shardRouting, new SyncedFlushResponse());
                        }
                    }
                    shardsResults.add(new ShardsSyncedFlushResult(shardId, "_sync_id_" + shard, replicas + 1, shardResponses));
                }
            }
            indicesResults.put(index, shardsResults);
            testPlan.countsPerIndex.put(index, new ShardCounts(shards * (replicas + 1), successful, failed));
            testPlan.expectedFailuresPerIndex.put(index, failures);
            totalFailed += failed;
            totalShards += shards * (replicas + 1);
            totalSuccesful += successful;
        }
        testPlan.result = new IndicesSyncedFlushResult(indicesResults);
        testPlan.totalCounts = new ShardCounts(totalShards, totalSuccesful, totalFailed);
        return testPlan;
    }
}
