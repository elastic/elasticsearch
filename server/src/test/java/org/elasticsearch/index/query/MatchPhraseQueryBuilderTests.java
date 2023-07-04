/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MatchPhraseQueryBuilderTests extends AbstractQueryTestCase<MatchPhraseQueryBuilder> {
    @Override
    protected MatchPhraseQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(
            TEXT_FIELD_NAME,
            TEXT_ALIAS_FIELD_NAME,
            BOOLEAN_FIELD_NAME,
            INT_FIELD_NAME,
            DOUBLE_FIELD_NAME,
            DATE_FIELD_NAME
        );
        Object value;
        if (isTextField(fieldName)) {
            int terms = randomIntBetween(0, 3);
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < terms; i++) {
                builder.append(randomAlphaOfLengthBetween(1, 10)).append(" ");
            }
            value = builder.toString().trim();
        } else {
            value = getRandomValueForFieldName(fieldName);
        }

        MatchPhraseQueryBuilder matchQuery = new MatchPhraseQueryBuilder(fieldName, value);

        if (randomBoolean() && isTextField(fieldName)) {
            matchQuery.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }

        if (randomBoolean()) {
            matchQuery.slop(randomIntBetween(0, 10));
        }

        if (randomBoolean()) {
            matchQuery.zeroTermsQuery(randomFrom(ZeroTermsQueryOption.ALL, ZeroTermsQueryOption.NONE));
        }

        return matchQuery;
    }

    @Override
    protected Map<String, MatchPhraseQueryBuilder> getAlternateVersions() {
        Map<String, MatchPhraseQueryBuilder> alternateVersions = new HashMap<>();
        MatchPhraseQueryBuilder matchPhraseQuery = new MatchPhraseQueryBuilder(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        String contentString = Strings.format("""
            {
                "match_phrase" : {
                    "%s" : "%s"
                }
            }""", matchPhraseQuery.fieldName(), matchPhraseQuery.value());
        alternateVersions.put(contentString, matchPhraseQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(MatchPhraseQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        assertThat(query, notNullValue());

        if (query instanceof MatchAllDocsQuery) {
            assertThat(queryBuilder.zeroTermsQuery(), equalTo(ZeroTermsQueryOption.ALL));
            return;
        }

        assertThat(
            query,
            either(instanceOf(BooleanQuery.class)).or(instanceOf(PhraseQuery.class))
                .or(instanceOf(TermQuery.class))
                .or(instanceOf(PointRangeQuery.class))
                .or(instanceOf(IndexOrDocValuesQuery.class))
                .or(instanceOf(MatchNoDocsQuery.class))
        );
    }

    public void testIllegalValues() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchPhraseQueryBuilder(null, "value"));
        assertEquals("[match_phrase] requires fieldName", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new MatchPhraseQueryBuilder("fieldName", null));
        assertEquals("[match_phrase] requires query value", e.getMessage());
    }

    public void testBadAnalyzer() throws IOException {
        MatchPhraseQueryBuilder matchQuery = new MatchPhraseQueryBuilder("fieldName", "text");
        matchQuery.analyzer("bogusAnalyzer");
        QueryShardException e = expectThrows(QueryShardException.class, () -> matchQuery.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
    }

    public void testFromSimpleJson() throws IOException {
        String json1 = """
            {
                "match_phrase" : {
                    "message" : "this is a test"
                }
            }""";

        String expected = """
            {
              "match_phrase" : {
                "message" : {
                  "query" : "this is a test"
                }
              }
            }""";
        MatchPhraseQueryBuilder qb = (MatchPhraseQueryBuilder) parseQuery(json1);
        checkGeneratedJson(expected, qb);
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "match_phrase" : {
                "message" : {
                  "query" : "this is a test",
                  "slop" : 2,
                  "zero_terms_query" : "ALL",
                  "boost" : 2.0
                }
              }
            }""";

        MatchPhraseQueryBuilder parsed = (MatchPhraseQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "this is a test", parsed.value());
        assertEquals(json, 2, parsed.slop());
        assertEquals(json, ZeroTermsQueryOption.ALL, parsed.zeroTermsQuery());
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = """
            {
              "match_phrase" : {
                "message1" : {
                  "query" : "this is a test"
                },
                "message2" : {
                  "query" : "this is a test"
                }
              }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[match_phrase] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = """
            {
              "match_phrase" : {
                "message1" : "this is a test",
                "message2" : "this is a test"
              }
            }""";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[match_phrase] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testRewriteToTermQueries() throws IOException {
        QueryBuilder queryBuilder = new MatchPhraseQueryBuilder(KEYWORD_FIELD_NAME, "value");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = queryBuilder.rewrite(context);
            assertThat(rewritten, instanceOf(TermQueryBuilder.class));
            TermQueryBuilder tqb = (TermQueryBuilder) rewritten;
            assertEquals(KEYWORD_FIELD_NAME, tqb.fieldName);
            assertEquals(new BytesRef("value"), tqb.value);
        }
    }

    public void testRewriteToTermQueryWithAnalyzer() throws IOException {
        MatchPhraseQueryBuilder queryBuilder = new MatchPhraseQueryBuilder(TEXT_FIELD_NAME, "value");
        queryBuilder.analyzer("keyword");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = queryBuilder.rewrite(context);
            assertThat(rewritten, instanceOf(TermQueryBuilder.class));
            TermQueryBuilder tqb = (TermQueryBuilder) rewritten;
            assertEquals(TEXT_FIELD_NAME, tqb.fieldName);
            assertEquals(new BytesRef("value"), tqb.value);
        }
    }

    public void testRewriteIndexQueryToMatchNone() throws IOException {
        QueryBuilder query = new MatchPhraseQueryBuilder("_index", "does_not_exist");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
        }
    }

    public void testRewriteIndexQueryToNotMatchNone() throws IOException {
        QueryBuilder query = new MatchPhraseQueryBuilder("_index", getIndex().getName());
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
        }
    }
}
