/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;

public class ShardSearchFailureTests extends ESTestCase {

    private static SearchShardTarget randomShardTarget(String indexUuid) {
        String nodeId = randomAlphaOfLengthBetween(5, 10);
        String indexName = randomAlphaOfLengthBetween(5, 10);
        String clusterAlias = randomBoolean() ? randomAlphaOfLengthBetween(5, 10) : null;
        return new SearchShardTarget(nodeId, new ShardId(new Index(indexName, indexUuid), randomInt()), clusterAlias);
    }

    public static ShardSearchFailure createTestItem(String indexUuid) {
        String randomMessage = randomAlphaOfLengthBetween(3, 20);
        Exception ex = new ParsingException(0, 0, randomMessage, new IllegalArgumentException("some bad argument"));
        return new ShardSearchFailure(ex, randomBoolean() ? randomShardTarget(indexUuid) : null);
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
        ShardSearchFailure response = createTestItem(IndexMetadata.INDEX_UUID_NA_VALUE);
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
            parsed = SearchResponseUtils.parseShardSearchFailure(parser);
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
        assertEquals(
            parsed.getCause().getCause().getMessage(),
            "Elasticsearch exception [type=illegal_argument_exception, reason=" + nestedMsg + "]"
        );
    }

    public void testToXContent() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(
            new ParsingException(0, 0, "some message", null),
            new SearchShardTarget("nodeId", new ShardId(new Index("indexName", "indexUuid"), 123), null)
        );
        BytesReference xContent = toXContent(failure, XContentType.JSON, randomBoolean());
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "shard": 123,
              "index": "indexName",
              "node": "nodeId",
              "reason": {
                "type": "parsing_exception",
                "reason": "some message",
                "line": 0,
                "col": 0
              }
            }"""), xContent.utf8ToString());
    }

    public void testToXContentForNoShardAvailable() throws IOException {
        ShardId shardId = new ShardId(new Index("indexName", "indexUuid"), 123);
        ShardSearchFailure failure = new ShardSearchFailure(
            NoShardAvailableActionException.forOnShardFailureWrapper("shard unassigned"),
            new SearchShardTarget("nodeId", shardId, null)
        );
        BytesReference xContent = toXContent(failure, XContentType.JSON, randomBoolean());
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "shard": 123,
              "index": "indexName",
              "node": "nodeId",
              "reason":{"type":"no_shard_available_action_exception","reason":"shard unassigned"}
            }"""), xContent.utf8ToString());
    }

    public void testToXContentWithClusterAlias() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(
            new ParsingException(0, 0, "some message", null),
            new SearchShardTarget("nodeId", new ShardId(new Index("indexName", "indexUuid"), 123), "cluster1")
        );
        BytesReference xContent = toXContent(failure, XContentType.JSON, randomBoolean());
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "shard": 123,
              "index": "cluster1:indexName",
              "node": "nodeId",
              "reason": {
                "type": "parsing_exception",
                "reason": "some message",
                "line": 0,
                "col": 0
              }
            }"""), xContent.utf8ToString());
    }

    public void testSerialization() throws IOException {
        for (int runs = 0; runs < 25; runs++) {
            final ShardSearchFailure testItem;
            if (randomBoolean()) {
                testItem = createTestItem(randomAlphaOfLength(12));
            } else {
                SearchShardTarget target = randomShardTarget(randomAlphaOfLength(12));
                testItem = new ShardSearchFailure(NoShardAvailableActionException.forOnShardFailureWrapper("unavailable"), target);
            }
            ShardSearchFailure deserializedInstance = copyWriteable(
                testItem,
                writableRegistry(),
                ShardSearchFailure::new,
                TransportVersionUtils.randomVersion(random())
            );
            assertEquals(testItem.index(), deserializedInstance.index());
            assertEquals(testItem.shard(), deserializedInstance.shard());
            assertEquals(testItem.shardId(), deserializedInstance.shardId());
            assertEquals(testItem.reason(), deserializedInstance.reason());
            assertEquals(testItem.status(), deserializedInstance.status());
        }
    }
}
