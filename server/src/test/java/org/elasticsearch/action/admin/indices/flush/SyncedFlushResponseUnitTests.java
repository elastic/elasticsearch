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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.indices.flush.SyncedFlushService;

public class SyncedFlushResponseUnitTests extends AbstractSyncedFlushTest {

    public void testXContentSerialization() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        TestPlan plan = createTestPlan();
        SyncedFlushResponse response = plan.result;
        assertNotNull(response);
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = builder
            .generator()
            .contentType()
            .xContent()
            .createParser(
                xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput()
            );
        SyncedFlushResponse parsedResponse = SyncedFlushResponse.fromXContent(parser);
        assertNotNull(parsedResponse);
        assertShardCounts(response.shardCounts, parsedResponse.shardCounts);
        for (Map.Entry<String, SyncedFlushResponse.ShardCounts> entry: response.getShardCountsPerIndex().entrySet()) {
            assertShardCounts(entry.getValue(), parsedResponse.shardCountsPerIndex.get(entry.getKey()));
            List<ShardsSyncedFlushResult> responseResults = response.shardsResultPerIndex.get(entry.getKey());
            List<ShardsSyncedFlushResult> parsedResults = parsedResponse.shardsResultPerIndex.get(entry.getKey());
            assertNotNull(responseResults);
            assertNotNull(parsedResults);
            if (entry.getValue().failed > 0) {
                // build a map for each shardId and compare total_copies and successful_copies and failed_copies
                Map<Integer, ShardsSyncedFlushResult> parsedResponseMap = new HashMap<>();
                for (ShardsSyncedFlushResult parsedResponseResult: parsedResults) {
                    parsedResponseMap.put(parsedResponseResult.shardId().id(), parsedResponseResult);
                }
                // We are just trying to perform a hash join here on the two lists based on shardId
                for (ShardsSyncedFlushResult responseResult: responseResults) {
                    Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> responseFailedShards =
                        responseResult.failedShards();
                    // After deserialization we lose information of successful shards
                    if (responseResult.failed() || responseFailedShards.size() > 0) {
                        ShardsSyncedFlushResult parsedResponseResult = parsedResponseMap.get(responseResult.shardId().id());
                        Map<ShardRouting, SyncedFlushService.ShardSyncedFlushResponse> parsedFailedShards =
                            parsedResponseResult.failedShards();
                        assertNotNull(parsedResponseResult);
                        assertEquals(responseResult.totalShards(), parsedResponseResult.totalShards());
                        assertEquals(responseResult.successfulShards(), parsedResponseResult.successfulShards());
                        assertEquals(responseResult.failureReason(), parsedResponseResult.failureReason());
                        assertEquals(responseFailedShards.size(), parsedFailedShards.size());
                    }
                }
            }
        }
        // We skip shard routing information here. Separate tests for shard routing verification exist
        // in ShardRoutingTests
    }

}
