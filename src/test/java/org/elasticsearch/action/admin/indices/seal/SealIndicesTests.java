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

package org.elasticsearch.action.admin.indices.seal;

import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SyncedFlushService;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.hamcrest.Matchers.equalTo;

public class SealIndicesTests extends ElasticsearchTestCase {

    public void testSealIndicesResponseStreaming() throws IOException {

        Set<SyncedFlushService.SyncedFlushResult> shardResults = new HashSet<>();
        // add one result where one shard failed and one succeeded
        SyncedFlushService.SyncedFlushResult syncedFlushResult = createSyncedFlushResult(0, "test");
        shardResults.add(syncedFlushResult);
        // add one result where all failed
        syncedFlushResult = new SyncedFlushService.SyncedFlushResult(new ShardId("test", 1), "all failed :(");
        shardResults.add(syncedFlushResult);
        SealIndicesResponse sealIndicesResponse = new SealIndicesResponse(shardResults);
        BytesStreamOutput out = new BytesStreamOutput();
        sealIndicesResponse.writeTo(out);
        out.close();
        StreamInput in = StreamInput.wrap(out.bytes());
        SealIndicesResponse readResponse = new SealIndicesResponse();
        readResponse.readFrom(in);
        Map<String, Object> asMap = convertToMap(readResponse);
        assertResponse(asMap);
    }

    public void testXContentResponse() throws IOException {

        Set<SyncedFlushService.SyncedFlushResult> shardResults = new HashSet<>();
        // add one result where one shard failed and one succeeded
        SyncedFlushService.SyncedFlushResult syncedFlushResult = createSyncedFlushResult(0, "test");
        shardResults.add(syncedFlushResult);
        // add one result where all failed
        syncedFlushResult = new SyncedFlushService.SyncedFlushResult(new ShardId("test", 1), "all failed :(");
        shardResults.add(syncedFlushResult);
        SealIndicesResponse sealIndicesResponse = new SealIndicesResponse(shardResults);
        Map<String, Object> asMap = convertToMap(sealIndicesResponse);
        assertResponse(asMap);
    }

    protected void assertResponse(Map<String, Object> asMap) {
        assertNotNull(asMap.get("test"));
        assertThat((Integer) (((HashMap) ((ArrayList) asMap.get("test")).get(0)).get("shard_id")), equalTo(0));
        assertThat((String) (((HashMap) ((ArrayList) asMap.get("test")).get(0)).get("message")), equalTo("failed on some copies"));
        HashMap<String, String> shardResponses = (HashMap<String, String>) ((HashMap) ((ArrayList) asMap.get("test")).get(0)).get("responses");
        assertThat(shardResponses.get("node_1"), equalTo("failed for some reason"));
        assertThat(shardResponses.get("node_2"), equalTo("success"));
        HashMap<String, Object> failedShard = (HashMap<String, Object>) (((ArrayList) asMap.get("test")).get(1));
        assertThat((Integer) (failedShard.get("shard_id")), equalTo(1));
        assertThat((String) (failedShard.get("message")), equalTo("all failed :("));
    }

    public void testXContentResponseSortsShards() throws IOException {
        Set<SyncedFlushService.SyncedFlushResult> shardResults = new HashSet<>();
        // add one result where one shard failed and one succeeded
        SyncedFlushService.SyncedFlushResult syncedFlushResult;
        for (int i = 100000; i >= 0; i--) {
            if (randomBoolean()) {
                syncedFlushResult = createSyncedFlushResult(i, "test");
                shardResults.add(syncedFlushResult);
            } else {
                syncedFlushResult = new SyncedFlushService.SyncedFlushResult(new ShardId("test", i), "all failed :(");
                shardResults.add(syncedFlushResult);
            }
        }
        SealIndicesResponse sealIndicesResponse = new SealIndicesResponse(shardResults);
        Map<String, Object> asMap = convertToMap(sealIndicesResponse);
        assertNotNull(asMap.get("test"));
        for (int i = 0; i < 100000; i++) {
            assertThat((Integer) (((HashMap) ((ArrayList) asMap.get("test")).get(i)).get("shard_id")), equalTo(i));
        }
    }

    protected SyncedFlushService.SyncedFlushResult createSyncedFlushResult(int shardId, String index) {
        Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> responses = new HashMap<>();
        ImmutableShardRouting shardRouting = new ImmutableShardRouting(index, shardId, "node_1", false, ShardRoutingState.RELOCATING, 2);
        SyncedFlushService.SyncedFlushResponse syncedFlushResponse = new SyncedFlushService.SyncedFlushResponse("failed for some reason");
        responses.put(shardRouting, syncedFlushResponse);
        shardRouting = new ImmutableShardRouting(index, shardId, "node_2", false, ShardRoutingState.RELOCATING, 2);
        syncedFlushResponse = new SyncedFlushService.SyncedFlushResponse();
        responses.put(shardRouting, syncedFlushResponse);
        return new SyncedFlushService.SyncedFlushResult(new ShardId(index, shardId), "some_sync_id", responses);
    }
}
