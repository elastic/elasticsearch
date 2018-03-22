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

import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse.ShardCounts;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SyncedFlushUnitTests extends AbstractSyncedFlushTest {

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
        SyncedFlushResponse readResponse = new SyncedFlushResponse();
        readResponse.readFrom(in);
        assertThat(readResponse.totalShards(), equalTo(testPlan.totalCounts.total));
        assertThat(readResponse.successfulShards(), equalTo(testPlan.totalCounts.successful));
        assertThat(readResponse.failedShards(), equalTo(testPlan.totalCounts.failed));
        assertThat(readResponse.restStatus(), equalTo(testPlan.totalCounts.failed > 0 ? RestStatus.CONFLICT : RestStatus.OK));
        assertThat(readResponse.shardsResultPerIndex.size(), equalTo(testPlan.result.getShardsResultPerIndex().size()));
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
                for (Map.Entry<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardEntry : originalShardResult.failedShards().entrySet()) {
                    SyncedFlushService.ShardSyncedFlushResponse readShardResponse = readShardResult.failedShards().get(shardEntry.getKey());
                    assertNotNull(readShardResponse);
                    SyncedFlushService.ShardSyncedFlushResponse originalShardResponse = shardEntry.getValue();
                    assertThat(originalShardResponse.failureReason(), equalTo(readShardResponse.failureReason()));
                    assertThat(originalShardResponse.success(), equalTo(readShardResponse.success()));
                }
                assertThat(originalShardResult.shardResponses().size(), equalTo(readShardResult.shardResponses().size()));
                for (Map.Entry<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> shardEntry : originalShardResult.shardResponses().entrySet()) {
                    SyncedFlushService.ShardSyncedFlushResponse readShardResponse = readShardResult.shardResponses().get(shardEntry.getKey());
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

}
