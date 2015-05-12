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

package org.elasticsearch.action.admin.indices.syncedflush;

import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SyncedFlushService;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.hamcrest.Matchers.equalTo;

public class SyncedFlushTests extends ElasticsearchTestCase {

    public void testXContentResponse() throws IOException {

        Set<SyncedFlushService.SyncedFlushResult> shardResults = new HashSet<>();
        // add one result where one shard failed and one succeeded
        Map<ShardRouting, SyncedFlushService.SyncedFlushResponse> responses = new HashMap<>();
        ImmutableShardRouting shardRouting = new ImmutableShardRouting("test", 0, "node_1", false, ShardRoutingState.RELOCATING, 2);
        SyncedFlushService.SyncedFlushResponse syncedFlushResponse = new SyncedFlushService.SyncedFlushResponse("failed for some reason");
        responses.put(shardRouting, syncedFlushResponse);
        shardRouting = new ImmutableShardRouting("test", 0, "node_2", false, ShardRoutingState.RELOCATING, 2);
        syncedFlushResponse = new SyncedFlushService.SyncedFlushResponse("SUCCESS");
        responses.put(shardRouting, syncedFlushResponse);
        SyncedFlushService.SyncedFlushResult syncedFlushResult = new SyncedFlushService.SyncedFlushResult(new ShardId("test", 0), "some_sync_id", responses);
        shardResults.add(syncedFlushResult);
        // add one result where all failed
        syncedFlushResult = new SyncedFlushService.SyncedFlushResult(new ShardId("test", 1), "all failed :(");
        shardResults.add(syncedFlushResult);
        SyncedFlushIndicesResponse syncedFlushIndicesResponse = new SyncedFlushIndicesResponse(shardResults);
        Map<String, Object> asMap = convertToMap(syncedFlushIndicesResponse);
        assertNotNull(asMap.get("[test]/0"));
        assertNotNull(((Map<String, Object>) asMap.get("[test]/0")).get("node_1"));
        assertThat((String) ((Map<String, Object>) asMap.get("[test]/0")).get("node_1"), equalTo("failed for some reason"));
        assertThat(((String) asMap.get("[test]/1")), equalTo("all failed :("));
    }
}
