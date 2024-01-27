/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class WildcardQueryBuilderTests extends AbstractQueryTestCase<WildcardQueryBuilder> {

    @Override
    protected WildcardQueryBuilder doCreateTestQueryBuilder() {
        WildcardQueryBuilder query = randomWildcardQuery();
        if (randomBoolean()) {
            query.rewrite(randomFrom(getRandomRewriteMethod()));
        }
        return query;
    }

    @Override
    protected Map<String, WildcardQueryBuilder> getAlternateVersions() {
        Map<String, WildcardQueryBuilder> alternateVersions = new HashMap<>();
        WildcardQueryBuilder wildcardQuery = randomWildcardQuery();
        String contentString = Strings.format("""
            {
                "wildcard" : {
                    "%s" : "%s"
                }
            }""", wildcardQuery.fieldName(), wildcardQuery.value());
        alternateVersions.put(contentString, wildcardQuery);
        return alternateVersions;
    }

    private static WildcardQueryBuilder randomWildcardQuery() {
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, randomAlphaOfLengthBetween(1, 10));
        String text = randomAlphaOfLengthBetween(1, 10);

        return new WildcardQueryBuilder(fieldName, text);
    }

    @Override
    protected void doAssertLuceneQuery(WildcardQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        String expectedFieldName = expectedFieldName(queryBuilder.fieldName());

        if (expectedFieldName.equals(TEXT_FIELD_NAME)) {
            if (queryBuilder.caseInsensitive()) {
                assertThat(query, instanceOf(AutomatonQuery.class));
            } else {
                assertThat(query, instanceOf(WildcardQuery.class));
                WildcardQuery wildcardQuery = (WildcardQuery) query;

                assertThat(wildcardQuery.getField(), equalTo(expectedFieldName));
                assertThat(wildcardQuery.getTerm().field(), equalTo(expectedFieldName));
                // wildcard queries get normalized
                String text = wildcardQuery.getTerm().text().toLowerCase(Locale.ROOT);
                assertThat(text, equalTo(text));
            }
        } else {
            Query expected = new MatchNoDocsQuery("unknown field [" + expectedFieldName + "]");
            assertEquals(expected, query);
        }
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new WildcardQueryBuilder(null, "text"));
        assertEquals("field name is null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new WildcardQueryBuilder("", "text"));
        assertEquals("field name is null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new WildcardQueryBuilder("field", null));
        assertEquals("value cannot be null", e.getMessage());
    }

    public void testEmptyValue() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder(TEXT_FIELD_NAME, "");
        assertEquals(wildcardQueryBuilder.toQuery(context).getClass(), WildcardQuery.class);
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "wildcard": {
                "user": {
                  "wildcard": "ki*y",
                  "case_insensitive": true,
                  "boost": 2.0
                }
              }
            }""";
        WildcardQueryBuilder parsed = (WildcardQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "ki*y", parsed.value());
        assertEquals(json, 2.0, parsed.boost(), 0.0001);
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = """
            {
                "wildcard": {
                  "user1": {
                    "wildcard": "ki*y"
                  },
                  "user2": {
                    "wildcard": "ki*y"
                  }
                }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[wildcard] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());

        String shortJson = """
            {
                "wildcard": {
                  "user1": "ki*y",
                  "user2": "ki*y"
                }
            }""";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[wildcard] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());
    }

    public void testRewriteIndexQueryToMatchNone() throws IOException {
        WildcardQueryBuilder query = new WildcardQueryBuilder("_index", "does_not_exist");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
        }
    }

    public void testRewriteIndexQueryNotMatchNone() throws IOException {
        String fullIndexName = getIndex().getName();
        String firstHalfOfIndexName = fullIndexName.substring(0, fullIndexName.length() / 2);
        WildcardQueryBuilder query = new WildcardQueryBuilder("_index", firstHalfOfIndexName + "*");
        for (QueryRewriteContext context : new QueryRewriteContext[] { createSearchExecutionContext(), createQueryRewriteContext() }) {
            QueryBuilder rewritten = query.rewrite(context);
            assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
        }
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        WildcardQueryBuilder queryBuilder = new WildcardQueryBuilder("unmapped_field", "foo");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }
}
