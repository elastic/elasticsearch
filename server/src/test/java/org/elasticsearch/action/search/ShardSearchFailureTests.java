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

package org.elasticsearch.action.search;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

public class ShardSearchFailureTests extends ESTestCase {

    public static ShardSearchFailure createTestItem(String indexUuid) {
        String randomMessage = randomAlphaOfLengthBetween(3, 20);
        Exception ex = new ParsingException(0, 0, randomMessage , new IllegalArgumentException("some bad argument"));
        SearchShardTarget searchShardTarget = null;
        if (randomBoolean()) {
            String nodeId = randomAlphaOfLengthBetween(5, 10);
            String indexName = randomAlphaOfLengthBetween(5, 10);
            String clusterAlias = randomBoolean() ? randomAlphaOfLengthBetween(5, 10) : null;
            searchShardTarget = new SearchShardTarget(nodeId,
                    new ShardId(new Index(indexName, indexUuid), randomInt()), clusterAlias, OriginalIndices.NONE);
        }
        return new ShardSearchFailure(ex, searchShardTarget);
    }

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to
     * ensure we can parse it back to be forward compatible with additions to
     * the xContent
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        ShardSearchFailure response = createTestItem(IndexMetaData.INDEX_UUID_NA_VALUE);
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(response, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xContentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        ShardSearchFailure parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = ShardSearchFailure.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertEquals(response.index(), parsed.index());
        assertEquals(response.shard(), parsed.shard());
        assertEquals(response.shardId(), parsed.shardId());

        /*
         * we cannot compare the cause, because it will be wrapped in an outer
         * ElasticSearchException best effort: try to check that the original
         * message appears somewhere in the rendered xContent
         */
        String originalMsg = response.getCause().getMessage();
        assertEquals(parsed.getCause().getMessage(), "Elasticsearch exception [type=parsing_exception, reason=" + originalMsg + "]");
        String nestedMsg = response.getCause().getCause().getMessage();
        assertEquals(parsed.getCause().getCause().getMessage(),
                "Elasticsearch exception [type=illegal_argument_exception, reason=" + nestedMsg + "]");
    }

    public void testToXContent() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(0, 0, "some message", null),
                new SearchShardTarget("nodeId", new ShardId(new Index("indexName", "indexUuid"), 123), null, OriginalIndices.NONE));
        BytesReference xContent = toXContent(failure, XContentType.JSON, randomBoolean());
        assertEquals(
                "{\"shard\":123,"
                        + "\"index\":\"indexName\","
                        + "\"node\":\"nodeId\","
                        + "\"reason\":{"
                            + "\"type\":\"parsing_exception\","
                            + "\"reason\":\"some message\","
                            + "\"line\":0,"
                            + "\"col\":0"
                        + "}"
                + "}",
                xContent.utf8ToString());
    }

    public void testToXContentWithClusterAlias() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(0, 0, "some message", null),
            new SearchShardTarget("nodeId", new ShardId(new Index("indexName", "indexUuid"), 123), "cluster1", OriginalIndices.NONE));
        BytesReference xContent = toXContent(failure, XContentType.JSON, randomBoolean());
        assertEquals(
            "{\"shard\":123,"
                + "\"index\":\"cluster1:indexName\","
                + "\"node\":\"nodeId\","
                + "\"reason\":{"
                + "\"type\":\"parsing_exception\","
                + "\"reason\":\"some message\","
                + "\"line\":0,"
                + "\"col\":0"
                + "}"
                + "}",
            xContent.utf8ToString());
    }

    public void testSerialization() throws IOException {
        ShardSearchFailure testItem = createTestItem(randomAlphaOfLength(12));
        ShardSearchFailure deserializedInstance = copyWriteable(testItem, writableRegistry(),
            ShardSearchFailure::new, VersionUtils.randomVersion(random()));
        assertEquals(testItem.index(), deserializedInstance.index());
        assertEquals(testItem.shard(), deserializedInstance.shard());
        assertEquals(testItem.shardId(), deserializedInstance.shardId());
        assertEquals(testItem.reason(), deserializedInstance.reason());
        assertEquals(testItem.status(), deserializedInstance.status());
    }
}
