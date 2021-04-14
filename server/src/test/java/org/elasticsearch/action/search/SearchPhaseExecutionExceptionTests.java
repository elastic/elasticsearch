/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class SearchPhaseExecutionExceptionTests extends ESTestCase {

    public void testToXContent() throws IOException {
        SearchPhaseExecutionException exception = new SearchPhaseExecutionException("test", "all shards failed",
                new ShardSearchFailure[]{
                        new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                                new SearchShardTarget("node_1", new ShardId("foo", "_na_", 0), null, OriginalIndices.NONE)),
                        new ShardSearchFailure(new IndexShardClosedException(new ShardId("foo", "_na_", 1)),
                                new SearchShardTarget("node_2", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE)),
                        new ShardSearchFailure(new ParsingException(5, 7, "foobar", null),
                                new SearchShardTarget("node_3", new ShardId("foo", "_na_", 2), null, OriginalIndices.NONE)),
                });

        // Failures are grouped (by default)
        final String expectedJson = XContentHelper.stripWhitespace(
            "{"
                + "  \"type\": \"search_phase_execution_exception\","
                + "  \"reason\": \"all shards failed\","
                + "  \"phase\": \"test\","
                + "  \"grouped\": true,"
                + "  \"failed_shards\": ["
                + "    {"
                + "      \"shard\": 0,"
                + "      \"index\": \"foo\","
                + "      \"node\": \"node_1\","
                + "      \"reason\": {"
                + "        \"type\": \"parsing_exception\","
                + "        \"reason\": \"foobar\","
                + "        \"line\": 1,"
                + "        \"col\": 2"
                + "      }"
                + "    },"
                + "    {"
                + "      \"shard\": 1,"
                + "      \"index\": \"foo\","
                + "      \"node\": \"node_2\","
                + "      \"reason\": {"
                + "        \"type\": \"index_shard_closed_exception\","
                + "        \"reason\": \"CurrentState[CLOSED] Closed\","
                + "        \"index_uuid\": \"_na_\","
                + "        \"shard\": \"1\","
                + "        \"index\": \"foo\""
                + "      }"
                + "    }"
                + "  ]"
                + "}"
        );
        assertEquals(expectedJson, Strings.toString(exception));
    }

    public void testToAndFromXContent() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();

        ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[randomIntBetween(1, 5)];
        for (int i = 0; i < shardSearchFailures.length; i++) {
            Exception cause = randomFrom(
                    new ParsingException(1, 2, "foobar", null),
                    new InvalidIndexTemplateException("foo", "bar"),
                    new TimestampParsingException("foo", null),
                    new NullPointerException()
            );
            shardSearchFailures[i] = new  ShardSearchFailure(cause, new SearchShardTarget("node_" + i,
                new ShardId("test", "_na_", i), null, OriginalIndices.NONE));
        }

        final String phase = randomFrom("query", "search", "other");
        SearchPhaseExecutionException actual = new SearchPhaseExecutionException(phase, "unexpected failures", shardSearchFailures);

        BytesReference exceptionBytes = toShuffledXContent(actual, xContent.type(), ToXContent.EMPTY_PARAMS, randomBoolean());

        ElasticsearchException parsedException;
        try (XContentParser parser = createParser(xContent, exceptionBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedException = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsedException);
        assertThat(parsedException.getHeaderKeys(), hasSize(0));
        assertThat(parsedException.getMetadataKeys(), hasSize(1));
        assertThat(parsedException.getMetadata("es.phase"), hasItem(phase));
        // SearchPhaseExecutionException has no cause field
        assertNull(parsedException.getCause());
    }

    public void testPhaseFailureWithoutSearchShardFailure() {
        final ShardSearchFailure[] searchShardFailures = new ShardSearchFailure[0];
        final String phase = randomFrom("fetch", "search", "other");
        SearchPhaseExecutionException actual = new SearchPhaseExecutionException(phase, "unexpected failures",
            new EsRejectedExecutionException("ES rejected execution of fetch phase"), searchShardFailures);

        assertEquals(actual.status(), RestStatus.TOO_MANY_REQUESTS);
    }

    public void testPhaseFailureWithoutSearchShardFailureAndCause() {
        final ShardSearchFailure[] searchShardFailures = new ShardSearchFailure[0];
        final String phase = randomFrom("fetch", "search", "other");
        SearchPhaseExecutionException actual = new SearchPhaseExecutionException(phase, "unexpected failures", null, searchShardFailures);

        assertEquals(actual.status(), RestStatus.SERVICE_UNAVAILABLE);
    }

    public void testPhaseFailureWithSearchShardFailure() {
        final ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[randomIntBetween(1, 5)];
        for (int i = 0; i < shardSearchFailures.length; i++) {
            Exception cause = randomFrom(
                new ParsingException(1, 2, "foobar", null),
                new InvalidIndexTemplateException("foo", "bar")
            );
            shardSearchFailures[i] = new ShardSearchFailure(cause, new SearchShardTarget("node_" + i,
                new ShardId("test", "_na_", i), null, OriginalIndices.NONE));
        }

        final String phase = randomFrom("fetch", "search", "other");
        SearchPhaseExecutionException actual = new SearchPhaseExecutionException(phase, "unexpected failures",
            new EsRejectedExecutionException("ES rejected execution of fetch phase"), shardSearchFailures);

        assertEquals(actual.status(), RestStatus.BAD_REQUEST);
    }
}
