/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PrefixQueryBuilderTests extends AbstractQueryTestCase<PrefixQueryBuilder> {

    @Override
    protected PrefixQueryBuilder doCreateTestQueryBuilder() {
        PrefixQueryBuilder query = randomPrefixQuery();
        if (randomBoolean()) {
            query.rewrite(getRandomRewriteMethod());
        }
        return query;
    }

    @Override
    protected Map<String, PrefixQueryBuilder> getAlternateVersions() {
        Map<String, PrefixQueryBuilder> alternateVersions = new HashMap<>();
        PrefixQueryBuilder prefixQuery = randomPrefixQuery();
        String contentString = Strings.format("""
            {
                "prefix" : {
                    "%s" : "%s"
                }
            }""", prefixQuery.fieldName(), prefixQuery.value());
        alternateVersions.put(contentString, prefixQuery);
        return alternateVersions;
    }

    private static PrefixQueryBuilder randomPrefixQuery() {
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, randomAlphaOfLengthBetween(1, 10));
        String value = randomAlphaOfLengthBetween(1, 10);
        return new PrefixQueryBuilder(fieldName, value);
    }

    @Override
    protected void doAssertLuceneQuery(PrefixQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(
            query,
            Matchers.anyOf(instanceOf(PrefixQuery.class), instanceOf(MatchNoDocsQuery.class), instanceOf(AutomatonQuery.class))
        );
        if (context.getFieldType(queryBuilder.fieldName()) != null && queryBuilder.caseInsensitive() == false) { // The field is mapped and
                                                                                                                 // case sensitive
            PrefixQuery prefixQuery = (PrefixQuery) query;

            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            assertThat(prefixQuery.getPrefix().field(), equalTo(expectedFieldName));
            assertThat(prefixQuery.getPrefix().text(), equalTo(queryBuilder.value()));
        }
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new PrefixQueryBuilder(null, "text"));
        assertEquals("field name is null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new PrefixQueryBuilder("", "text"));
        assertEquals("field name is null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new PrefixQueryBuilder("field", null));
        assertEquals("value cannot be null", e.getMessage());
    }

    public void testBlendedRewriteMethod() throws IOException {
        String rewrite = "top_terms_blended_freqs_10";
        Query parsedQuery = parseQuery(prefixQuery(TEXT_FIELD_NAME, "val").rewrite(rewrite)).toQuery(createSearchExecutionContext());
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term(TEXT_FIELD_NAME, "val")));
        assertThat(prefixQuery.getRewriteMethod(), instanceOf(MultiTermQuery.TopTermsBlendedFreqScoringRewrite.class));
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "prefix": {
                "user": {
                  "value": "ki",
                  "case_insensitive": true,
                  "boost": 2.0
                }
              }
            }
            """;

        PrefixQueryBuilder parsed = (PrefixQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "ki", parsed.value());
        assertEquals(json, 2.0, parsed.boost(), 0.00001);
        assertEquals(json, "user", parsed.fieldName());
    }

    public void testNumeric() throws Exception {
        PrefixQueryBuilder query = prefixQuery(INT_FIELD_NAME, "12*");
        SearchExecutionContext context = createSearchExecutionContext();
        QueryShardException e = expectThrows(QueryShardException.class, () -> query.toQuery(context));
        assertEquals(
            "Can only use prefix queries on keyword, text and wildcard fields - not on [mapped_int] which is of type [integer]",
            e.getMessage()
        );
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = """
            {
                "prefix": {
                  "user1": {
                    "value": "ki"
                  },
                  "user2": {
                    "value": "ki"
                  }
                }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[prefix] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());

        String shortJson = """
            {
                "prefix": {
                  "user1": "ki",
                  "user2": "ki"
                }
            }""";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[prefix] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());
    }

    public void testRewriteIndexQueryToMatchNone() throws Exception {
        PrefixQueryBuilder query = prefixQuery("_index", "does_not_exist");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
        }
    }

    public void testRewriteIndexQueryToNotMatchNone() throws Exception {
        PrefixQueryBuilder query = prefixQuery("_index", getIndex().getName());
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
        }
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        PrefixQueryBuilder queryBuilder = new PrefixQueryBuilder("unmapped_field", "foo");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }
}
