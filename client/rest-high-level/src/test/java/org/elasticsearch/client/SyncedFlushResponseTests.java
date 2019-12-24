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
package org.elasticsearch.client;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SyncedFlushResponseTests extends ESTestCase {

    public void testXContentSerialization() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        TestPlan plan = createTestPlan();

        XContentBuilder serverResponsebuilder = XContentBuilder.builder(xContentType.xContent());
        assertNotNull(plan.result);
        serverResponsebuilder.startObject();
        plan.result.toXContent(serverResponsebuilder, ToXContent.EMPTY_PARAMS);
        serverResponsebuilder.endObject();
        XContentBuilder clientResponsebuilder = XContentBuilder.builder(xContentType.xContent());
        assertNotNull(plan.result);
        plan.clientResult.toXContent(clientResponsebuilder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> serverContentMap = convertFailureListToSet(
            serverResponsebuilder
                .generator()
                .contentType()
                .xContent()
                .createParser(
                    xContentRegistry(),
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(serverResponsebuilder).streamInput()
                ).map()
        );
        Map<String, Object> clientContentMap = convertFailureListToSet(
            clientResponsebuilder
                .generator()
                .contentType()
                .xContent()
                .createParser(
                    xContentRegistry(),
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(clientResponsebuilder).streamInput()
                )
                .map()
        );
        assertEquals(serverContentMap, clientContentMap);
    }

    public void testXContentDeserialization() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        TestPlan plan = createTestPlan();
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        builder.startObject();
        plan.result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = builder
            .generator()
            .contentType()
            .xContent()
            .createParser(
                xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                BytesReference.bytes(builder).streamInput()
            );
        SyncedFlushResponse originalResponse = plan.clientResult;
        SyncedFlushResponse parsedResponse = SyncedFlushResponse.fromXContent(parser);
        assertNotNull(parsedResponse);
        assertShardCounts(originalResponse.getShardCounts(), parsedResponse.getShardCounts());
        for (Map.Entry<String, SyncedFlushResponse.IndexResult> entry: originalResponse.getIndexResults().entrySet()) {
            String index = entry.getKey();
            SyncedFlushResponse.IndexResult responseResult = entry.getValue();
            SyncedFlushResponse.IndexResult parsedResult = parsedResponse.getIndexResults().get(index);
            assertNotNull(responseResult);
            assertNotNull(parsedResult);
            assertShardCounts(responseResult.getShardCounts(), parsedResult.getShardCounts());
            assertEquals(responseResult.failures().size(), parsedResult.failures().size());
            for (SyncedFlushResponse.ShardFailure responseShardFailure: responseResult.failures()) {
                assertTrue(containsFailure(parsedResult.failures(), responseShardFailure));
            }
        }
    }

    static class TestPlan {
        SyncedFlushResponse.ShardCounts totalCounts;
        Map<String, SyncedFlushResponse.ShardCounts> countsPerIndex = new HashMap<>();
        ObjectIntMap<String> expectedFailuresPerIndex = new ObjectIntHashMap<>();
        org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse result;
        SyncedFlushResponse clientResult;
    }

    TestPlan createTestPlan() throws IOException {
        final TestPlan testPlan = new TestPlan();
        final Map<String, List<ShardsSyncedFlushResult>> indicesResults = new HashMap<>();
        Map<String, SyncedFlushResponse.IndexResult> indexResults = new HashMap<>();
        final XContentType xContentType = randomFrom(XContentType.values());
        final int indexCount = randomIntBetween(1, 10);
        int totalShards = 0;
        int totalSuccessful = 0;
        int totalFailed = 0;
        for (int i = 0; i < indexCount; i++) {
            final String index = "index_" + i;
            int shards = randomIntBetween(1, 4);
            int replicas = randomIntBetween(0, 2);
            int successful = 0;
            int failed = 0;
            int failures = 0;
            List<ShardsSyncedFlushResult> shardsResults = new ArrayList<>();
            List<SyncedFlushResponse.ShardFailure> shardFailures = new ArrayList<>();
            for (int shard = 0; shard < shards; shard++) {
                final ShardId shardId = new ShardId(index, "_na_", shard);
                if (randomInt(5) < 2) {
                    // total shard failure
                    failed += replicas + 1;
                    failures++;
                    shardsResults.add(new ShardsSyncedFlushResult(shardId, replicas + 1, "simulated total failure"));
                    shardFailures.add(
                        new SyncedFlushResponse.ShardFailure(
                            shardId.id(),
                            "simulated total failure",
                            new HashMap<>()
                        )
                    );
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
                            // Building the shardRouting map here.
                            XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
                            Map<String, Object> routing =
                                shardRouting.toXContent(builder, ToXContent.EMPTY_PARAMS)
                                    .generator()
                                    .contentType()
                                    .xContent()
                                    .createParser(
                                        xContentRegistry(),
                                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                                        BytesReference.bytes(builder).streamInput()
                                    )
                                    .map();
                            shardFailures.add(
                                new SyncedFlushResponse.ShardFailure(
                                    shardId.id(),
                                    "copy failure " + shardId,
                                    routing
                                )
                            );
                        } else {
                            successful++;
                            shardResponses.put(shardRouting, new SyncedFlushService.ShardSyncedFlushResponse((String) null));
                        }
                    }
                    shardsResults.add(new ShardsSyncedFlushResult(shardId, "_sync_id_" + shard, replicas + 1, shardResponses));
                }
            }
            indicesResults.put(index, shardsResults);
            indexResults.put(
                index,
                new SyncedFlushResponse.IndexResult(
                    shards * (replicas + 1),
                    successful,
                    failed,
                    shardFailures
                )
            );
            testPlan.countsPerIndex.put(index, new SyncedFlushResponse.ShardCounts(shards * (replicas + 1), successful, failed));
            testPlan.expectedFailuresPerIndex.put(index, failures);
            totalFailed += failed;
            totalShards += shards * (replicas + 1);
            totalSuccessful += successful;
        }
        testPlan.result = new org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse(indicesResults);
        testPlan.totalCounts = new SyncedFlushResponse.ShardCounts(totalShards, totalSuccessful, totalFailed);
        testPlan.clientResult = new SyncedFlushResponse(
            new SyncedFlushResponse.ShardCounts(totalShards, totalSuccessful, totalFailed),
            indexResults
        );
        return testPlan;
    }

    public boolean containsFailure(List<SyncedFlushResponse.ShardFailure> failures, SyncedFlushResponse.ShardFailure origFailure) {
        for (SyncedFlushResponse.ShardFailure failure: failures) {
            if (failure.getShardId() == origFailure.getShardId() &&
                failure.getFailureReason().equals(origFailure.getFailureReason()) &&
                failure.getRouting().equals(origFailure.getRouting())) {
                return true;
            }
        }
        return false;
    }


    public void assertShardCounts(SyncedFlushResponse.ShardCounts first, SyncedFlushResponse.ShardCounts second) {
        if (first == null) {
            assertNull(second);
        } else {
            assertTrue(first.equals(second));
        }
    }

    public Map<String, Object> convertFailureListToSet(Map<String, Object> input) {
        Map<String, Object> retMap = new HashMap<>();
        for (Map.Entry<String, Object> entry: input.entrySet()) {
            if (entry.getKey().equals(SyncedFlushResponse.SHARDS_FIELD)) {
                retMap.put(entry.getKey(), entry.getValue());
            } else {
                // This was an index entry.
                @SuppressWarnings("unchecked")
                Map<String, Object> indexResult = (Map<String, Object>)entry.getValue();
                Map<String, Object> retResult = new HashMap<>();
                for (Map.Entry<String, Object> entry2: indexResult.entrySet()) {
                    if (entry2.getKey().equals(SyncedFlushResponse.IndexResult.FAILURES_FIELD)) {
                        @SuppressWarnings("unchecked")
                        List<Object> failures = (List<Object>)entry2.getValue();
                        Set<Object> retSet = new HashSet<>(failures);
                        retResult.put(entry.getKey(), retSet);
                    } else {
                        retResult.put(entry2.getKey(), entry2.getValue());
                    }
                }
                retMap.put(entry.getKey(), retResult);
            }
        }
        return retMap;
    }
}
