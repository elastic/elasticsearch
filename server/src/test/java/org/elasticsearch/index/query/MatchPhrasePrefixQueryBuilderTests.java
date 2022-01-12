/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MatchPhrasePrefixQueryBuilderTests extends AbstractQueryTestCase<MatchPhrasePrefixQueryBuilder> {
    @Override
    protected MatchPhrasePrefixQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME);
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

        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder(fieldName, value);

        if (randomBoolean() && isTextField(fieldName)) {
            matchQuery.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }

        if (randomBoolean()) {
            matchQuery.slop(randomIntBetween(0, 10));
        }

        if (randomBoolean()) {
            matchQuery.maxExpansions(randomIntBetween(1, 10000));
        }
        if (randomBoolean()) {
            matchQuery.zeroTermsQuery(randomFrom(ZeroTermsQueryOption.ALL, ZeroTermsQueryOption.NONE));
        }
        return matchQuery;
    }

    @Override
    protected Map<String, MatchPhrasePrefixQueryBuilder> getAlternateVersions() {
        Map<String, MatchPhrasePrefixQueryBuilder> alternateVersions = new HashMap<>();
        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQuery = new MatchPhrasePrefixQueryBuilder(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        String contentString = """
            {
                "match_phrase_prefix" : {
                    "%s" : "%s"
                }
            }""".formatted(matchPhrasePrefixQuery.fieldName(), matchPhrasePrefixQuery.value());
        alternateVersions.put(contentString, matchPhrasePrefixQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(MatchPhrasePrefixQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        assertThat(query, notNullValue());

        if (query instanceof MatchAllDocsQuery) {
            assertThat(queryBuilder.zeroTermsQuery(), equalTo(ZeroTermsQueryOption.ALL));
            return;
        }

        assertThat(
            query,
            either(instanceOf(MultiPhrasePrefixQuery.class)).or(instanceOf(SynonymQuery.class)).or(instanceOf(MatchNoDocsQuery.class))
        );
    }

    public void testIllegalValues() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchPhrasePrefixQueryBuilder(null, "value"));
        assertEquals("[match_phrase_prefix] requires fieldName", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new MatchPhrasePrefixQueryBuilder("fieldName", null));
        assertEquals("[match_phrase_prefix] requires query value", e.getMessage());

        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder("fieldName", "text");
        e = expectThrows(IllegalArgumentException.class, () -> matchQuery.maxExpansions(-1));
    }

    public void testBadAnalyzer() throws IOException {
        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder("fieldName", "text");
        matchQuery.analyzer("bogusAnalyzer");

        QueryShardException e = expectThrows(QueryShardException.class, () -> matchQuery.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
    }

    public void testPhraseOnFieldWithNoTerms() {
        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder(DATE_FIELD_NAME, "three term phrase");
        matchQuery.analyzer("whitespace");
        expectThrows(IllegalArgumentException.class, () -> matchQuery.doToQuery(createSearchExecutionContext()));
    }

    public void testPhrasePrefixZeroTermsQuery() throws IOException {
        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder(TEXT_FIELD_NAME, "");
        matchQuery.zeroTermsQuery(ZeroTermsQueryOption.NONE);
        assertEquals(new MatchNoDocsQuery(), matchQuery.doToQuery(createSearchExecutionContext()));

        matchQuery = new MatchPhrasePrefixQueryBuilder(TEXT_FIELD_NAME, "");
        matchQuery.zeroTermsQuery(ZeroTermsQueryOption.ALL);
        assertEquals(new MatchAllDocsQuery(), matchQuery.doToQuery(createSearchExecutionContext()));
    }

    public void testPhrasePrefixMatchQuery() throws IOException {
        String json1 = """
            {
                "match_phrase_prefix" : {
                    "message" : "this is a test"
                }
            }""";

        String expected = """
            {
              "match_phrase_prefix" : {
                "message" : {
                  "query" : "this is a test",
                  "slop" : 0,
                  "max_expansions" : 50,
                  "zero_terms_query" : "NONE",
                  "boost" : 1.0
                }
              }
            }""";
        MatchPhrasePrefixQueryBuilder qb = (MatchPhrasePrefixQueryBuilder) parseQuery(json1);
        checkGeneratedJson(expected, qb);

        String json3 = """
            {
                "match_phrase_prefix" : {
                    "message" : {
                        "query" : "this is a test",
                        "max_expansions" : 10
                    }
                }
            }""";
        expected = """
            {
              "match_phrase_prefix" : {
                "message" : {
                  "query" : "this is a test",
                  "slop" : 0,
                  "max_expansions" : 10,
                  "zero_terms_query" : "NONE",
                  "boost" : 1.0
                }
              }
            }""";
        qb = (MatchPhrasePrefixQueryBuilder) parseQuery(json3);
        checkGeneratedJson(expected, qb);
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = """
            {
              "match_phrase_prefix" : {
                "message1" : {
                  "query" : "this is a test"
                },
                "message2" : {
                  "query" : "this is a test"
                }
              }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[match_phrase_prefix] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = """
            {
              "match_phrase_prefix" : {
                "message1" : "this is a test",
                "message2" : "this is a test"
              }
            }""";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[match_phrase_prefix] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }
}
