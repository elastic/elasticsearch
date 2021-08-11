/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse.ShardCounts;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;
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
        public SyncedFlushResponse.ShardCounts totalCounts;
        public Map<String, SyncedFlushResponse.ShardCounts> countsPerIndex = new HashMap<>();
        public ObjectIntMap<String> expectedFailuresPerIndex = new ObjectIntHashMap<>();
        public SyncedFlushResponse result;
    }

    @SuppressWarnings("unchecked")
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

    public void testResponseStreaming() throws IOException {
        final TestPlan testPlan = createTestPlan();
        assertThat(testPlan.result.totalShards(), equalTo(testPlan.totalCounts.total));
        assertThat(testPlan.result.successfulShards(), equalTo(testPlan.totalCounts.successful));
        assertThat(testPlan.result.failedShards(), equalTo(testPlan.totalCounts.failed));
        assertThat(testPlan.result.restStatus(), equalTo(testPlan.totalCounts.failed > 0 ? RestStatus.CONFLICT : RestStatus.OK));
        BytesStreamOutput out = new BytesStreamOutput();
        testPlan.result.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        SyncedFlushResponse readResponse = new SyncedFlushResponse(in);
        assertThat(readResponse.totalShards(), equalTo(testPlan.totalCounts.total));
        assertThat(readResponse.successfulShards(), equalTo(testPlan.totalCounts.successful));
        assertThat(readResponse.failedShards(), equalTo(testPlan.totalCounts.failed));
        assertThat(readResponse.restStatus(), equalTo(testPlan.totalCounts.failed > 0 ? RestStatus.CONFLICT : RestStatus.OK));
        assertThat(readResponse.getShardsResultPerIndex().size(), equalTo(testPlan.result.getShardsResultPerIndex().size()));
        for (Map.Entry<String, List<ShardsSyncedFlushResult>> entry : readResponse.getShardsResultPerIndex().entrySet()) {
            List<ShardsSyncedFlushResult> originalShardsResults = testPlan.result.getShardsResultPerIndex().get(entry.getKey());
            assertNotNull(originalShardsResults);
            List<ShardsSyncedFlushResult> readShardsResults = entry.getValue();
            assertThat(readShardsResults.size(), equalTo(originalShardsResults.size()));
            for (int i = 0; i < readShardsResults.size(); i++) {
                ShardsSyncedFlushResult originalShardResult = originalShardsResults.get(i);
                ShardsSyncedFlushResult readShardResult = readShardsResults.get(i);
                assertThat(originalShardResult.failureReason(), equalTo(readShardResult.failureReason()));
                assertThat(originalShardResult.failed(), equalTo(readShardResult.failed()));
                assertThat(originalShardResult.getShardId(), equalTo(readShardResult.getShardId()));
                assertThat(originalShardResult.successfulShards(), equalTo(readShardResult.successfulShards()));
                assertThat(originalShardResult.syncId(), equalTo(readShardResult.syncId()));
                assertThat(originalShardResult.totalShards(), equalTo(readShardResult.totalShards()));
                assertThat(originalShardResult.failedShards().size(), equalTo(readShardResult.failedShards().size()));
                for (Map.Entry<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardEntry
                        : originalShardResult.failedShards().entrySet()) {
                    SyncedFlushService.ShardSyncedFlushResponse readShardResponse = readShardResult.failedShards().get(shardEntry.getKey());
                    assertNotNull(readShardResponse);
                    SyncedFlushService.ShardSyncedFlushResponse originalShardResponse = shardEntry.getValue();
                    assertThat(originalShardResponse.failureReason(), equalTo(readShardResponse.failureReason()));
                    assertThat(originalShardResponse.success(), equalTo(readShardResponse.success()));
                }
                assertThat(originalShardResult.shardResponses().size(), equalTo(readShardResult.shardResponses().size()));
                for (Map.Entry<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardEntry
                        : originalShardResult.shardResponses().entrySet()) {
                    SyncedFlushService.ShardSyncedFlushResponse readShardResponse = readShardResult.shardResponses()
                        .get(shardEntry.getKey());
                    assertNotNull(readShardResponse);
                    SyncedFlushService.ShardSyncedFlushResponse originalShardResponse = shardEntry.getValue();
                    assertThat(originalShardResponse.failureReason(), equalTo(readShardResponse.failureReason()));
                    assertThat(originalShardResponse.success(), equalTo(readShardResponse.success()));
                }
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
                final ShardId shardId = new ShardId(index, "_na_", shard);
                if (randomInt(5) < 2) {
                    // total shard failure
                    failed += replicas + 1;
                    failures++;
                    shardsResults.add(new ShardsSyncedFlushResult(shardId, replicas + 1, "simulated total failure"));
                } else {
                    Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardResponses = new HashMap<>();
                    for (int copy = 0; copy < replicas + 1; copy++) {
                        final ShardRouting shardRouting = TestShardRouting.newShardRouting(index, shard, "node_" + shardId + "_" + copy,
                            null, copy == 0, ShardRoutingState.STARTED);
                        if (randomInt(5) < 2) {
                            // shard copy failure
                            failed++;
                            failures++;
                            shardResponses.put(shardRouting, new SyncedFlushService.ShardSyncedFlushResponse("copy failure " + shardId));
                        } else {
                            successful++;
                            shardResponses.put(shardRouting, new SyncedFlushService.ShardSyncedFlushResponse((String) null));
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

}
