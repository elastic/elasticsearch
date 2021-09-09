/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import com.fasterxml.jackson.core.io.JsonEOFException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;

public class RestActionsTests extends ESTestCase {

    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents());
    }

    @AfterClass
    public static void cleanup() {
        xContentRegistry = null;
    }

    public void testParseTopLevelBuilder() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("foo", "bar");
        String requestBody = "{ \"query\" : " + query.toString() + "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
            QueryBuilder actual = RestActions.getQueryContent(parser);
            assertEquals(query, actual);
        }
    }

    public void testParseTopLevelBuilderEmptyObject() throws IOException {
        for (String requestBody : Arrays.asList("{}", "")) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
                QueryBuilder query = RestActions.getQueryContent(parser);
                assertNull(query);
            }
        }
    }

    public void testParseTopLevelBuilderMalformedJson() throws IOException {
        for (String requestBody : Arrays.asList("\"\"", "\"someString\"", "\"{\"")) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
                ParsingException exception =
                    expectThrows(ParsingException.class, () -> RestActions.getQueryContent(parser));
                assertEquals("Expected [START_OBJECT] but found [VALUE_STRING]", exception.getMessage());
            }
        }
    }

    public void testParseTopLevelBuilderIncompleteJson() throws IOException {
        for (String requestBody : Arrays.asList("{", "{ \"query\" :")) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
                ParsingException exception =
                    expectThrows(ParsingException.class, () -> RestActions.getQueryContent(parser));
                assertEquals("Failed to parse", exception.getMessage());
                assertEquals(JsonEOFException.class, exception.getRootCause().getClass());
            }
        }
    }

    public void testParseTopLevelBuilderUnknownParameter() throws IOException {
        String requestBody = "{ \"foo\" : \"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> RestActions.getQueryContent(parser));
            assertEquals("request does not support [foo]", exception.getMessage());
        }
    }

    public void testBuildBroadcastShardsHeader() throws IOException {
        ShardOperationFailedException[] failures = new ShardOperationFailedException[]{
            createShardFailureParsingException("node0", 0, null),
            createShardFailureParsingException("node1", 1, null),
            createShardFailureParsingException("node2", 2, null),
            createShardFailureParsingException("node0", 0, "cluster1"),
            createShardFailureParsingException("node1", 1, "cluster1"),
            createShardFailureParsingException("node2", 2, "cluster1"),
            createShardFailureParsingException("node0", 0, "cluster2"),
            createShardFailureParsingException("node1", 1, "cluster2"),
            createShardFailureParsingException("node2", 2, "cluster2")
        };

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        RestActions.buildBroadcastShardsHeader(builder, ToXContent.EMPTY_PARAMS, 12, 3, 0, 9, failures);
        builder.endObject();
        assertThat(Strings.toString(builder), equalTo("{\n" +
            "  \"_shards\" : {\n" +
            "    \"total\" : 12,\n" +
            "    \"successful\" : 3,\n" +
            "    \"skipped\" : 0,\n" +
            "    \"failed\" : 9,\n" +
            "    \"failures\" : [\n" +
            "      {\n" +
            "        \"shard\" : 0,\n" +
            "        \"index\" : \"index\",\n" +
            "        \"node\" : \"node0\",\n" +
            "        \"reason\" : {\n" +
            "          \"type\" : \"parsing_exception\",\n" +
            "          \"reason\" : \"error\",\n" +
            "          \"index_uuid\" : \"_na_\",\n" +
            "          \"index\" : \"index\",\n" +
            "          \"line\" : 0,\n" +
            "          \"col\" : 0,\n" +
            "          \"caused_by\" : {\n" +
            "            \"type\" : \"illegal_argument_exception\",\n" +
            "            \"reason\" : \"some bad argument\"\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"shard\" : 0,\n" +
            "        \"index\" : \"cluster1:index\",\n" +
            "        \"node\" : \"node0\",\n" +
            "        \"reason\" : {\n" +
            "          \"type\" : \"parsing_exception\",\n" +
            "          \"reason\" : \"error\",\n" +
            "          \"index_uuid\" : \"_na_\",\n" +
            "          \"index\" : \"index\",\n" +
            "          \"line\" : 0,\n" +
            "          \"col\" : 0,\n" +
            "          \"caused_by\" : {\n" +
            "            \"type\" : \"illegal_argument_exception\",\n" +
            "            \"reason\" : \"some bad argument\"\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      {\n" +
            "        \"shard\" : 0,\n" +
            "        \"index\" : \"cluster2:index\",\n" +
            "        \"node\" : \"node0\",\n" +
            "        \"reason\" : {\n" +
            "          \"type\" : \"parsing_exception\",\n" +
            "          \"reason\" : \"error\",\n" +
            "          \"index_uuid\" : \"_na_\",\n" +
            "          \"index\" : \"index\",\n" +
            "          \"line\" : 0,\n" +
            "          \"col\" : 0,\n" +
            "          \"caused_by\" : {\n" +
            "            \"type\" : \"illegal_argument_exception\",\n" +
            "            \"reason\" : \"some bad argument\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}"));
    }

    private static ShardSearchFailure createShardFailureParsingException(String nodeId, int shardId, String clusterAlias) {
        String index = "index";
        ParsingException ex = new ParsingException(0, 0, "error", new IllegalArgumentException("some bad argument"));
        ex.setIndex(index);
        return new ShardSearchFailure(ex, createSearchShardTarget(nodeId, shardId, index, clusterAlias));
    }

    private static SearchShardTarget createSearchShardTarget(String nodeId, int shardId, String index, String clusterAlias) {
        return new SearchShardTarget(nodeId,
            new ShardId(new Index(index, IndexMetadata.INDEX_UUID_NA_VALUE), shardId), clusterAlias, OriginalIndices.NONE);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }
}
