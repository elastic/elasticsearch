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

package org.elasticsearch.client.core;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class CountResponseTests extends ESTestCase {

    // Not comparing XContent for equivalence as we cannot compare the ShardSearchFailure#cause, because it will be wrapped in an outer
    // ElasticSearchException. Best effort: try to check that the original message appears somewhere in the rendered xContent
    // For more see ShardSearchFailureTests.
    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            this::createTestInstance,
            this::toXContent,
            CountResponse::fromXContent)
            .supportsUnknownFields(false)
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(false)
            .test();
    }

    private CountResponse createTestInstance() {
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

    private void toXContent(CountResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(CountResponse.COUNT.getPreferredName(), response.getCount());
        if (response.isTerminatedEarly() != null) {
            builder.field(CountResponse.TERMINATED_EARLY.getPreferredName(), response.isTerminatedEarly());
        }
        toXContent(response.getShardStats(), builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
    }

    private void toXContent(CountResponse.ShardStats stats, XContentBuilder builder, ToXContent.Params params) throws IOException {
        RestActions.buildBroadcastShardsHeader(builder, params, stats.getTotalShards(), stats.getSuccessfulShards(), stats
            .getSkippedShards(), stats.getShardFailures().length, stats.getShardFailures());
    }

    @SuppressWarnings("Duplicates")
    private static ShardSearchFailure createShardFailureTestItem() {
        String randomMessage = randomAlphaOfLengthBetween(3, 20);
        Exception ex = new ParsingException(0, 0, randomMessage, new IllegalArgumentException("some bad argument"));
        SearchShardTarget searchShardTarget = null;
        if (randomBoolean()) {
            String nodeId = randomAlphaOfLengthBetween(5, 10);
            String indexName = randomAlphaOfLengthBetween(5, 10);
            searchShardTarget = new SearchShardTarget(nodeId,
                new ShardId(new Index(indexName, IndexMetaData.INDEX_UUID_NA_VALUE), randomInt()), null, null);
        }
        return new ShardSearchFailure(ex, searchShardTarget);
    }

    private void assertEqualInstances(CountResponse expectedInstance, CountResponse newInstance) {
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
}
