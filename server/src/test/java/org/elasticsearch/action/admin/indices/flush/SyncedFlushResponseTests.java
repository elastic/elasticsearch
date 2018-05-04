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
import java.util.Map;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;

public class SyncedFlushResponseTests extends AbstractSyncedFlushTest {

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
        for (Map.Entry<String, SyncedFlushResponse.FlushSyncedResponsePerIndex> entry: response.getResponsePerIndex().entrySet()) {
            String index = entry.getKey();
            // assertShardCounts(entry.getValue().shardCounts, parsedResponse.shardCountsPerIndex.get(index));
            SyncedFlushResponse.FlushSyncedResponsePerIndex responseResult = entry.getValue();
            SyncedFlushResponse.FlushSyncedResponsePerIndex parsedResult = parsedResponse.responsePerIndex.get(index);
            assertNotNull(responseResult);
            assertNotNull(parsedResult);
            assertShardCounts(responseResult.shardCounts, parsedResult.shardCounts);
            assertEquals(responseResult.shardFailures.size(), parsedResult.shardFailures.size());
            for (Map.Entry<ShardId, SyncedFlushResponse.ShardFailure> failureEntry: responseResult.shardFailures.entrySet()) {
                ShardId id = failureEntry.getKey();
                SyncedFlushResponse.ShardFailure responseShardFailure = failureEntry.getValue();
                SyncedFlushResponse.ShardFailure parsedShardFailure = parsedResult.shardFailures.get(id);
                assertNotNull(parsedShardFailure);
                assertEquals(responseShardFailure.successfulCopies, parsedShardFailure.successfulCopies);
                assertEquals(responseShardFailure.totalCopies, parsedShardFailure.totalCopies);
                assertEquals(responseShardFailure.failedCopies, parsedShardFailure.failedCopies);
                // We skip shard routing information here.
                // Separate tests for shard routing verification exist in
                // org.elasticsearch.cluster.routing.ShardRoutingTests
            }
        }
    }
}
