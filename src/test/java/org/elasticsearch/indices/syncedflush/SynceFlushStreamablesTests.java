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
package org.elasticsearch.indices.syncedflush;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class SynceFlushStreamablesTests extends ElasticsearchTestCase {

    @Test
    public void streamWriteSyncResponse() throws InterruptedException, IOException {
        ShardId shardId = new ShardId("test", 0);
        Map<String, byte[]> commitIds = new HashMap<>();
        final String nodeId = "node_id";
        commitIds.put(nodeId, generateRandomId(randomInt(100)));
        SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(shardId, randomAsciiOfLength(5), commitIds);
        BytesStreamOutput out = new BytesStreamOutput();
        syncedFlushRequest.writeTo(out);
        out.close();
        StreamInput in = new BytesStreamInput(out.bytes());
        SyncedFlushRequest request = new SyncedFlushRequest();
        request.readFrom(in);
        assertArrayEquals(request.commitIds().get(nodeId), syncedFlushRequest.commitIds().get(nodeId));
    }

    @Test
    public void streamSyncResponse() throws InterruptedException, IOException {
        ShardRouting shardRouting = new ImmutableShardRouting("test", 0, "test_node",
                "other_test_node", randomBoolean(), ShardRoutingState.STARTED, randomInt());
        AtomicReferenceArray atomicReferenceArray = new AtomicReferenceArray(1);
        atomicReferenceArray.set(0, new PreSyncedShardFlushResponse(generateRandomId(randomInt(100)), shardRouting));
        PreSyncedFlushResponse preSyncedFlushResponse = new PreSyncedFlushResponse(randomInt(), randomInt(), randomInt(), new ArrayList<ShardOperationFailedException>(), atomicReferenceArray);
        BytesStreamOutput out = new BytesStreamOutput();
        preSyncedFlushResponse.writeTo(out);
        out.close();
        StreamInput in = new BytesStreamInput(out.bytes());
        PreSyncedFlushResponse request = new PreSyncedFlushResponse();
        request.readFrom(in);
        assertArrayEquals(request.commitIds().get(shardRouting), preSyncedFlushResponse.commitIds().get(shardRouting));
    }

    @Test
    public void streamShardSyncResponse() throws InterruptedException, IOException {
        ShardRouting shardRouting = new ImmutableShardRouting("test", 0, "test_node",
                "other_test_node", randomBoolean(), ShardRoutingState.STARTED, randomInt());
        PreSyncedShardFlushResponse preSyncedShardFlushResponse = new PreSyncedShardFlushResponse(generateRandomId(randomInt(100)), shardRouting);
        BytesStreamOutput out = new BytesStreamOutput();
        preSyncedShardFlushResponse.writeTo(out);
        out.close();
        StreamInput in = new BytesStreamInput(out.bytes());
        PreSyncedShardFlushResponse request = new PreSyncedShardFlushResponse();
        request.readFrom(in);
        assertArrayEquals(request.id(), preSyncedShardFlushResponse.id());
    }

    byte[] generateRandomId(int length) {
        byte[] id = new byte[length];
        for (int i = 0; i < length; i++) {
            id[i] = randomByte();
        }
        return id;
    }
}
