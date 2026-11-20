/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class AbstractQueryBuilderTests extends ESTestCase {

    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents());
    }

    @AfterClass
    public static void cleanup() {
        xContentRegistry = null;
    }

    public void testParseInnerQueryBuilder() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("foo", "bar");
        String source = query.toString();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            QueryBuilder actual = parseInnerQueryBuilder(parser);
            assertEquals(query, actual);
        }
    }

    public void testParseInnerQueryBuilderExceptions() throws IOException {
        String source = "{ \"foo\": \"bar\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            parser.nextToken();
            parser.nextToken(); // don't start with START_OBJECT to provoke exception
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("[_na] query malformed, must start with start_object", exception.getMessage());
        }

        source = "{}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("query malformed, empty clause found at [1:2]", exception.getMessage());
        }

        source = "{ \"foo\" : \"bar\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("[foo] query malformed, no start_object after query name", exception.getMessage());
        }

        source = "{ \"boool\" : {} }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("unknown query [boool] did you mean [bool]?", exception.getMessage());
        }
        source = "{ \"match_\" : {} }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("unknown query [match_] did you mean any of [match, match_all, match_none]?", exception.getMessage());
        }
    }

    public void testQueryAtTheClauseLimitIsParsed() throws IOException {
        int limit = Math.max(AbstractQueryBuilder.PARSE_CLAUSE_LIMIT_FLOOR, IndexSearcher.getMaxClauseCount());
        // Split across two inner bool queries, which are not clauses themselves.
        String source = "{\"bool\":{\"should\":[" + boolQueryJson(limit / 2) + "," + boolQueryJson(limit - limit / 2) + "]}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            BoolQueryBuilder query = (BoolQueryBuilder) AbstractQueryBuilder.parseTopLevelQuery(parser);
            assertEquals(2, query.should().size());
        }
    }

    public void testQueryOverTheClauseLimitIsRejectedWhileParsed() throws IOException {
        int limit = Math.max(AbstractQueryBuilder.PARSE_CLAUSE_LIMIT_FLOOR, IndexSearcher.getMaxClauseCount());
        // A query at the limit still parses, so the rejection below is about the count.
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, boolQueryJson(limit))) {
            assertNotNull(AbstractQueryBuilder.parseTopLevelQuery(parser));
        }
        String source = "{\"query\":" + boolQueryJson(limit + 1_000) + ",\"size\":10}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals("query", parser.nextFieldName());
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AbstractQueryBuilder.parseTopLevelQuery(parser)
            );
            assertThat(causeMessages(e), containsString("The number of clauses in the query exceeds the maximum of [" + limit + "]"));
            // Rejected at the first clause over the limit, so the other clauses, several tokens each, are still unread.
            int unread = 0;
            while (parser.nextToken() != null) {
                unread++;
            }
            assertThat(unread, greaterThan(1_000));
        }
    }

    public void testHigherNodeClauseLimitIsUsedWhileParsed() throws IOException {
        int original = IndexSearcher.getMaxClauseCount();
        int nodeLimit = AbstractQueryBuilder.PARSE_CLAUSE_LIMIT_FLOOR * 2;
        IndexSearcher.setMaxClauseCount(nodeLimit);
        try {
            // Over the floor, but within the node's own limit.
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, boolQueryJson(nodeLimit))) {
                assertNotNull(AbstractQueryBuilder.parseTopLevelQuery(parser));
            }
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, boolQueryJson(nodeLimit + 1))) {
                IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> AbstractQueryBuilder.parseTopLevelQuery(parser)
                );
                assertThat(causeMessages(e), containsString("exceeds the maximum of [" + nodeLimit + "]"));
            }
        } finally {
            IndexSearcher.setMaxClauseCount(original);
        }
    }

    public void testSearchBodyWithTooManyClausesIsRejectedWhileParsed() throws IOException {
        int limit = Math.max(AbstractQueryBuilder.PARSE_CLAUSE_LIMIT_FLOOR, IndexSearcher.getMaxClauseCount());
        // A SMILE search body with a bool query of distinct term clauses over the limit.
        XContentBuilder body = XContentBuilder.builder(XContentType.SMILE.xContent());
        body.startObject().startObject("query").startObject("bool").startArray("should");
        for (int i = 0; i < limit * 5; i++) {
            body.startObject().startObject("term").field("field", "value" + i).endObject().endObject();
        }
        body.endArray().endObject().endObject().endObject();
        try (XContentParser parser = createParser(body)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new SearchSourceBuilder().parseXContent(parser, true, feature -> true)
            );
            assertThat(causeMessages(e), containsString("The number of clauses in the query exceeds the maximum of [" + limit + "]"));
            // Rejected at the first clause over the limit, so most of the body is never read.
            int unread = 0;
            while (parser.nextToken() != null) {
                unread++;
            }
            assertThat(unread, greaterThan(limit));
        }
    }

    private static String boolQueryJson(int termClauses) {
        StringBuilder json = new StringBuilder("{\"bool\":{\"should\":[");
        for (int i = 0; i < termClauses; i++) {
            json.append(i == 0 ? "" : ",").append("{\"term\":{\"field\":\"value").append(i).append("\"}}");
        }
        return json.append("]}}").toString();
    }

    private static String causeMessages(Throwable e) {
        StringBuilder messages = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause()) {
            messages.append(t.getMessage()).append('\n');
        }
        return messages.toString();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public void testMaybeConvertToBytesRefLongTerm() {
        String longTerm = "a".repeat(IndexWriter.MAX_TERM_LENGTH + 1);
        Exception e = expectThrows(IllegalArgumentException.class, () -> AbstractQueryBuilder.maybeConvertToBytesRef(longTerm));
        assertThat(e.getMessage(), containsString("term starting with [aaaaa"));
    }

    public void testMaybeConvertToBytesRefStringCorrectSize() {
        int capacity = randomIntBetween(20, 40);
        StringBuilder termBuilder = new StringBuilder(capacity);
        int correctSize = 0;
        for (int i = 0; i < capacity; i++) {
            if (i < capacity / 3) {
                termBuilder.append((char) randomIntBetween(0, 127));
                ++correctSize; // use only one byte for char < 128
            } else if (i < 2 * capacity / 3) {
                termBuilder.append((char) randomIntBetween(128, 2047));
                correctSize += 2; // use two bytes for char < 2048
            } else {
                termBuilder.append((char) randomIntBetween(2048, 4092));
                correctSize += 3; // use three bytes for char >= 2048
            }
        }
        BytesRef bytesRef = (BytesRef) AbstractQueryBuilder.maybeConvertToBytesRef(termBuilder.toString());
        assertEquals(correctSize, bytesRef.bytes.length);
        assertEquals(correctSize, bytesRef.length);
    }

}
