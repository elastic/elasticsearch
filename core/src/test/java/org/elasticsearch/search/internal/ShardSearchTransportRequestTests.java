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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchRequestTests;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public class ShardSearchTransportRequestTests extends ESTestCase {

    private static NamedWriteableRegistry namedWriteableRegistry;

    @BeforeClass
    public static void beforeClass() {
        IndicesModule indicesModule = new IndicesModule(emptyList()) {
            @Override
            protected void configure() {
                bindMapperExtension();
            }
        };
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList()) {
            @Override
            protected void configureSearch() {
                // Skip me
            }
        };
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    @AfterClass
    public static void afterClass() {
        namedWriteableRegistry = null;
    }

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

    private static ShardSearchTransportRequest createShardSearchTransportRequest() throws IOException {
        SearchRequest searchRequest = SearchRequestTests.createSearchRequest();
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
