/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.search.SearchModule.INDICES_MAX_NESTED_DEPTH_SETTING;

public class WrapperQueryBuilderTests extends AbstractQueryTestCase<WrapperQueryBuilder> {

    @Override
    protected boolean supportsBoost() {
        return false;
    }

    @Override
    protected boolean supportsQueryName() {
        return false;
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return false;
    }

    @Override
    protected WrapperQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder wrappedQuery = RandomQueryBuilder.createQuery(random());
        BytesReference bytes;
        try {
            bytes = XContentHelper.toXContent(wrappedQuery, XContentType.JSON, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return switch (randomInt(2)) {
            case 0 -> new WrapperQueryBuilder(wrappedQuery.toString());
            case 1 -> new WrapperQueryBuilder(BytesReference.toBytes(bytes));
            case 2 -> new WrapperQueryBuilder(bytes);
            default -> throw new UnsupportedOperationException();
        };
    }

    @Override
    protected void doAssertLuceneQuery(WrapperQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        QueryBuilder innerQuery = queryBuilder.rewrite(createSearchExecutionContext());
        Query expected = rewrite(innerQuery.toQuery(context));
        assertEquals(rewrite(query), expected);
    }

    public void testIllegalArgument() {
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder((byte[]) null));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder(new byte[0]));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder((String) null));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder(""));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder((BytesReference) null));
        expectThrows(IllegalArgumentException.class, () -> new WrapperQueryBuilder(new BytesArray(new byte[0])));
    }

    /**
     * Replace the generic test from superclass, wrapper query only expects
     * to find `query` field with nested query and should throw exception for
     * anything else.
     */
    @Override
    public void testUnknownField() {
        String json = "{ \"" + WrapperQueryBuilder.NAME + "\" : {\"bogusField\" : \"someValue\"} }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertTrue(e.getMessage().contains("bogusField"));
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "wrapper" : {
                "query" : "e30="
              }
            }""";

        WrapperQueryBuilder parsed = (WrapperQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "{}", new String(parsed.source(), StandardCharsets.UTF_8));
    }

    @Override
    public void testMustRewrite() throws IOException {
        TermQueryBuilder tqb = new TermQueryBuilder(TEXT_FIELD_NAME, "bar");
        WrapperQueryBuilder qb = new WrapperQueryBuilder(tqb.toString());
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> qb.toQuery(createSearchExecutionContext())
        );
        assertEquals("this query must be rewritten first", e.getMessage());
        QueryBuilder rewrite = qb.rewrite(createSearchExecutionContext());
        assertEquals(tqb, rewrite);
    }

    public void testRewriteWithInnerName() throws IOException {
        QueryBuilder builder = new WrapperQueryBuilder("""
            { "match_all" : {"_name" : "foobar"}}""");
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        assertEquals(new MatchAllQueryBuilder().queryName("foobar"), builder.rewrite(searchExecutionContext));
        builder = new WrapperQueryBuilder("""
            { "match_all" : {"_name" : "foobar"}}""").queryName("outer");
        assertEquals(
            new BoolQueryBuilder().must(new MatchAllQueryBuilder().queryName("foobar")).queryName("outer"),
            builder.rewrite(searchExecutionContext)
        );
    }

    public void testRewriteWithInnerBoost() throws IOException {
        final TermQueryBuilder query = new TermQueryBuilder(TEXT_FIELD_NAME, "bar").boost(2);
        QueryBuilder builder = new WrapperQueryBuilder(query.toString());
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        assertEquals(query, builder.rewrite(searchExecutionContext));
        builder = new WrapperQueryBuilder(query.toString()).boost(3);
        assertEquals(new BoolQueryBuilder().must(query).boost(3), builder.rewrite(searchExecutionContext));
    }

    public void testRewriteInnerQueryToo() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        QueryBuilder qb = new WrapperQueryBuilder(
            new WrapperQueryBuilder(new TermQueryBuilder(TEXT_FIELD_NAME, "bar").toString()).toString()
        );
        assertEquals(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")), qb.rewrite(searchExecutionContext).toQuery(searchExecutionContext));
        qb = new WrapperQueryBuilder(
            new WrapperQueryBuilder(new WrapperQueryBuilder(new TermQueryBuilder(TEXT_FIELD_NAME, "bar").toString()).toString()).toString()
        );
        assertEquals(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")), qb.rewrite(searchExecutionContext).toQuery(searchExecutionContext));

        qb = new WrapperQueryBuilder(new BoolQueryBuilder().toString());
        assertEquals(new MatchAllDocsQuery(), qb.rewrite(searchExecutionContext).toQuery(searchExecutionContext));
    }

    @Override
    protected Query rewrite(Query query) throws IOException {
        // WrapperQueryBuilder adds some optimization if the wrapper and query builder have boosts / query names that wraps
        // the actual QueryBuilder that comes from the binary blob into a BooleanQueryBuilder to give it an outer boost / name
        // this causes some queries to be not exactly equal but equivalent such that we need to rewrite them before comparing.
        if (query != null) {
            MemoryIndex idx = new MemoryIndex();
            return idx.createSearcher().rewrite(query);
        }
        return new MatchAllDocsQuery(); // null == *:*
    }

    @Override
    protected WrapperQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        return new WrapperQueryBuilder(Strings.toString(queryBuilder));
    }

    public void testMaxNestedDepth() throws IOException {
        BoolQueryBuilderTests boolQueryBuilderTests = new BoolQueryBuilderTests();
        BoolQueryBuilder boolQuery = boolQueryBuilderTests.createQueryWithInnerQuery(new MatchAllQueryBuilder());
        int maxDepth = randomIntBetween(3, 5);
        AbstractQueryBuilder.setMaxNestedDepth(maxDepth);
        for (int i = 1; i < maxDepth - 1; i++) {
            boolQuery = boolQueryBuilderTests.createQueryWithInnerQuery(boolQuery);
        }
        WrapperQueryBuilder query = new WrapperQueryBuilder(Strings.toString(boolQuery));
        AbstractQueryBuilder.setMaxNestedDepth(maxDepth);
        try {
            // no errors, we reached the limit but we did not go beyond it
            query.rewrite(createSearchExecutionContext());
            // one more level causes an exception
            WrapperQueryBuilder q = new WrapperQueryBuilder(Strings.toString(boolQueryBuilderTests.createQueryWithInnerQuery(boolQuery)));
            IllegalArgumentException e = expectThrows(XContentParseException.class, () -> q.rewrite(createSearchExecutionContext()));
            // there may be nested XContentParseExceptions coming from ObjectParser, we just extract the root cause
            while (e.getCause() != null) {
                assertThat(e.getCause(), Matchers.instanceOf(IllegalArgumentException.class));
                e = (IllegalArgumentException) e.getCause();
            }

            assertEquals(
                "The nested depth of the query exceeds the maximum nested depth for queries set in ["
                    + INDICES_MAX_NESTED_DEPTH_SETTING.getKey()
                    + "]",
                e.getMessage()
            );
        } finally {
            AbstractQueryBuilder.setMaxNestedDepth(INDICES_MAX_NESTED_DEPTH_SETTING.getDefault(Settings.EMPTY));
        }
    }
}
