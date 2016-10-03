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

package org.elasticsearch.search.internal;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.AbstractSearchTestCase;

import java.io.IOException;

public class ShardSearchTransportRequestTests extends AbstractSearchTestCase {

    public void testSerialization() throws Exception {
        ShardSearchTransportRequest shardSearchTransportRequest = createShardSearchTransportRequest();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            shardSearchTransportRequest.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                ShardSearchTransportRequest deserializedRequest = new ShardSearchTransportRequest();
                deserializedRequest.readFrom(in);
                assertEquals(deserializedRequest.scroll(), shardSearchTransportRequest.scroll());
                assertArrayEquals(deserializedRequest.filteringAliases(), shardSearchTransportRequest.filteringAliases());
                assertArrayEquals(deserializedRequest.indices(), shardSearchTransportRequest.indices());
                assertArrayEquals(deserializedRequest.types(), shardSearchTransportRequest.types());
                assertEquals(deserializedRequest.indicesOptions(), shardSearchTransportRequest.indicesOptions());
                assertEquals(deserializedRequest.isProfile(), shardSearchTransportRequest.isProfile());
                assertEquals(deserializedRequest.nowInMillis(), shardSearchTransportRequest.nowInMillis());
                assertEquals(deserializedRequest.source(), shardSearchTransportRequest.source());
                assertEquals(deserializedRequest.searchType(), shardSearchTransportRequest.searchType());
                assertEquals(deserializedRequest.shardId(), shardSearchTransportRequest.shardId());
                assertEquals(deserializedRequest.numberOfShards(), shardSearchTransportRequest.numberOfShards());
                assertEquals(deserializedRequest.cacheKey(), shardSearchTransportRequest.cacheKey());
                assertNotSame(deserializedRequest, shardSearchTransportRequest);
            }
        }
    }

    private ShardSearchTransportRequest createShardSearchTransportRequest() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        ShardId shardId = new ShardId(randomAsciiOfLengthBetween(2, 10), randomAsciiOfLengthBetween(2, 10), randomInt());
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, null, null, randomBoolean(), ShardRoutingState.UNASSIGNED,
            new UnassignedInfo(randomFrom(UnassignedInfo.Reason.values()), "reason"));
        String[] filteringAliases;
        if (randomBoolean()) {
            filteringAliases = generateRandomStringArray(10, 10, false, false);
        } else {
            filteringAliases = Strings.EMPTY_ARRAY;
        }
        return new ShardSearchTransportRequest(searchRequest, shardRouting,
                randomIntBetween(1, 100), filteringAliases, Math.abs(randomLong()));
    }
}
