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

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.count.CountResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class CountResponseTests extends AbstractXContentTestCase<CountResponse> {

    @Override
    protected CountResponse createTestInstance() {
        long count = 5;
        Boolean terminatedEarly = randomBoolean() ? null : randomBoolean();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, totalShards);
        int numFailures = randomIntBetween(1, 5);
        ShardSearchFailure[] failures = new ShardSearchFailure[numFailures];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = createShardFailureTestItem();
        }
        CountResponse.ShardStats shardStats = new CountResponse.ShardStats(successfulShards, totalShards, skippedShards,
            randomBoolean() ? ShardSearchFailure.EMPTY_ARRAY : failures);
        return new CountResponse(count, terminatedEarly, shardStats);
    }


    @SuppressWarnings("Duplicates") //suppress warning, as original code is in server:test:SearchResponseTests, would need to add a testJar
    // (similar as x-pack), or move test code from server to test framework
    public static ShardSearchFailure createShardFailureTestItem() {
        String randomMessage = randomAlphaOfLengthBetween(3, 20);
        Exception ex = new ParsingException(0, 0, randomMessage , new IllegalArgumentException("some bad argument"));
        SearchShardTarget searchShardTarget = null;
        if (randomBoolean()) {
            String nodeId = randomAlphaOfLengthBetween(5, 10);
            String indexName = randomAlphaOfLengthBetween(5, 10);
            searchShardTarget = new SearchShardTarget(nodeId,
                new ShardId(new Index(indexName, IndexMetaData.INDEX_UUID_NA_VALUE), randomInt()), null, null);
        }
        return new ShardSearchFailure(ex, searchShardTarget);
    }

    @Override
    protected CountResponse doParseInstance(XContentParser parser) throws IOException {
        return CountResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    @Override
    protected void assertEqualInstances(CountResponse expectedInstance, CountResponse newInstance) {
        assertEquals(expectedInstance.getCount(), newInstance.getCount());
        assertEquals(expectedInstance.status(), newInstance.status());
        assertEquals(expectedInstance.isTerminatedEarly(), newInstance.isTerminatedEarly());
        assertEquals(expectedInstance.getTotalShards(), newInstance.getTotalShards());
        assertEquals(expectedInstance.getFailedShards(), newInstance.getFailedShards());
        assertEquals(expectedInstance.getSkippedShards(), newInstance.getSkippedShards());
        assertEquals(expectedInstance.getSuccessfulShards(), newInstance.getSuccessfulShards());
        assertEquals(expectedInstance.getShardFailures().length, newInstance.getShardFailures().length);

        ShardSearchFailure[] expectedFailures = expectedInstance.getShardFailures();
        ShardSearchFailure[] newFailures = newInstance.getShardFailures();

        for (int i = 0; i < newFailures.length; i++) {
            ShardSearchFailure parsedFailure = newFailures[i];
            ShardSearchFailure originalFailure = expectedFailures[i];
            assertEquals(originalFailure.index(), parsedFailure.index());
            assertEquals(originalFailure.shard(), parsedFailure.shard());
            assertEquals(originalFailure.shardId(), parsedFailure.shardId());
            String originalMsg = originalFailure.getCause().getMessage();
            assertEquals(parsedFailure.getCause().getMessage(), "Elasticsearch exception [type=parsing_exception, reason=" +
                originalMsg + "]");
            String nestedMsg = originalFailure.getCause().getCause().getMessage();
            assertEquals(parsedFailure.getCause().getCause().getMessage(),
                "Elasticsearch exception [type=illegal_argument_exception, reason=" + nestedMsg + "]");
        }
    }


    public void testToXContent() {
        CountResponse response = new CountResponse(8, null, new CountResponse.ShardStats(1, 1, 0, ShardSearchFailure.EMPTY_ARRAY));
        String expectedString = "{\"count\":8,\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,\"failed\":0}}";
        assertEquals(expectedString, Strings.toString(response));
    }

    public void testToXContentWithTerminatedEarly() {
        CountResponse response = new CountResponse(8, true, new CountResponse.ShardStats(1, 1, 0, ShardSearchFailure.EMPTY_ARRAY));
        String expectedString = "{\"count\":8,\"terminated_early\":true,\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0," +
            "\"failed\":0}}";
        assertEquals(expectedString, Strings.toString(response));
    }

    public void testToXContentWithTerminatedEarlyAndShardFailures() {
        ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(0, 0, "not looking well", null),
            new SearchShardTarget("nodeId", new ShardId(new Index("indexName", "indexUuid"), 1), null, OriginalIndices.NONE));
        CountResponse response = new CountResponse(8, true, new CountResponse.ShardStats(1, 2, 0, new ShardSearchFailure[]{failure}));
        String expectedString =
            "{\"count\":8," +
                "\"terminated_early\":true," +
                "\"_shards\":" +
                "{\"total\":2," +
                "\"successful\":1," +
                "\"skipped\":0," +
                "\"failed\":1," +
                "\"failures\":" +
                "[{\"shard\":1," +
                "\"index\":\"indexName\"," +
                "\"node\":\"nodeId\"," +
                "\"reason\":{\"type\":\"parsing_exception\",\"reason\":\"not looking well\",\"line\":0,\"col\":0}}]" +
                "}" +
                "}";
        assertEquals(expectedString, Strings.toString(response));
    }
}
