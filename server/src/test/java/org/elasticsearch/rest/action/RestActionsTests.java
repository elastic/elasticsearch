/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentEOFException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.query.QueryStringQueryBuilder.DEFAULT_OPERATOR;
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
                ParsingException exception = expectThrows(ParsingException.class, () -> RestActions.getQueryContent(parser));
                assertEquals("Expected [START_OBJECT] but found [VALUE_STRING]", exception.getMessage());
            }
        }
    }

    public void testParseTopLevelBuilderIncompleteJson() throws IOException {
        for (String requestBody : Arrays.asList("{", "{ \"query\" :")) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
                ParsingException exception = expectThrows(ParsingException.class, () -> RestActions.getQueryContent(parser));
                assertEquals("Failed to parse", exception.getMessage());
                assertEquals(XContentEOFException.class, exception.getCause().getClass());
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
        ShardOperationFailedException[] failures = new ShardOperationFailedException[] {
            createShardFailureParsingException("node0", 0, null),
            createShardFailureParsingException("node1", 1, null),
            createShardFailureParsingException("node2", 2, null),
            createShardFailureParsingException("node0", 0, "cluster1"),
            createShardFailureParsingException("node1", 1, "cluster1"),
            createShardFailureParsingException("node2", 2, "cluster1"),
            createShardFailureParsingException("node0", 0, "cluster2"),
            createShardFailureParsingException("node1", 1, "cluster2"),
            createShardFailureParsingException("node2", 2, "cluster2") };

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        RestActions.buildBroadcastShardsHeader(builder, ToXContent.EMPTY_PARAMS, 12, 3, 0, 9, failures);
        builder.endObject();
        assertThat(Strings.toString(builder), equalTo("""
            {
              "_shards" : {
                "total" : 12,
                "successful" : 3,
                "skipped" : 0,
                "failed" : 9,
                "failures" : [
                  {
                    "shard" : 0,
                    "index" : "index",
                    "node" : "node0",
                    "reason" : {
                      "type" : "parsing_exception",
                      "reason" : "error",
                      "index_uuid" : "_na_",
                      "index" : "index",
                      "line" : 0,
                      "col" : 0,
                      "caused_by" : {
                        "type" : "illegal_argument_exception",
                        "reason" : "some bad argument"
                      }
                    }
                  },
                  {
                    "shard" : 0,
                    "index" : "cluster1:index",
                    "node" : "node0",
                    "reason" : {
                      "type" : "parsing_exception",
                      "reason" : "error",
                      "index_uuid" : "_na_",
                      "index" : "index",
                      "line" : 0,
                      "col" : 0,
                      "caused_by" : {
                        "type" : "illegal_argument_exception",
                        "reason" : "some bad argument"
                      }
                    }
                  },
                  {
                    "shard" : 0,
                    "index" : "cluster2:index",
                    "node" : "node0",
                    "reason" : {
                      "type" : "parsing_exception",
                      "reason" : "error",
                      "index_uuid" : "_na_",
                      "index" : "index",
                      "line" : 0,
                      "col" : 0,
                      "caused_by" : {
                        "type" : "illegal_argument_exception",
                        "reason" : "some bad argument"
                      }
                    }
                  }
                ]
              }
            }"""));
    }

    public void testUrlParamsToQueryBuilder() {
        // without any parameters, result should be null
        assertNull(RestActions.urlParamsToQueryBuilder(new FakeRestRequest()));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry).withParams(Map.of("q", "foo:bar")).build();
        QueryStringQueryBuilder queryBuilder = (QueryStringQueryBuilder) RestActions.urlParamsToQueryBuilder(request);
        assertNotNull(queryBuilder);
        assertNull(queryBuilder.analyzer());
        assertNull(queryBuilder.defaultField());
        assertEquals(DEFAULT_OPERATOR, queryBuilder.defaultOperator());
        assertNull(queryBuilder.lenient());
        assertFalse(queryBuilder.analyzeWildcard());

        request = new FakeRestRequest.Builder(xContentRegistry).withParams(
            Map.of(
                "q",
                "foo:bar",
                "analyzer",
                "german",
                "analyze_wildcard",
                "true",
                "df",
                "message",
                "lenient",
                "true",
                "default_operator",
                "and"
            )
        ).build();
        queryBuilder = (QueryStringQueryBuilder) RestActions.urlParamsToQueryBuilder(request);
        assertNotNull(queryBuilder);
        assertEquals("german", queryBuilder.analyzer());
        assertEquals("message", queryBuilder.defaultField());
        assertEquals(Operator.AND, queryBuilder.defaultOperator());
        assertTrue(queryBuilder.lenient());
        assertTrue(queryBuilder.analyzeWildcard());
    }

    public void testUrlParamsToQueryBuilderError() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry).withParams(
            Map.of("analyzer", "german", "analyze_wildcard", "true", "df", "message", "lenient", "true", "default_operator", "and")
        ).build();
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> RestActions.urlParamsToQueryBuilder(request));
        assertEquals(
            "request [/] contains parameters [df, analyzer, analyze_wildcard, lenient, default_operator] "
                + "but missing query string parameter 'q'.",
            iae.getMessage()
        );
    }

    private static ShardSearchFailure createShardFailureParsingException(String nodeId, int shardId, String clusterAlias) {
        String index = "index";
        ParsingException ex = new ParsingException(0, 0, "error", new IllegalArgumentException("some bad argument"));
        ex.setIndex(index);
        return new ShardSearchFailure(ex, createSearchShardTarget(nodeId, shardId, index, clusterAlias));
    }

    private static SearchShardTarget createSearchShardTarget(String nodeId, int shardId, String index, String clusterAlias) {
        return new SearchShardTarget(nodeId, new ShardId(new Index(index, IndexMetadata.INDEX_UUID_NA_VALUE), shardId), clusterAlias);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }
}
