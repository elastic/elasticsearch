/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class KqlParserFieldlessQueryTests extends AbstractKqlParserTestCase {

    public void testParseUnquotedLiteralQuery() {
        // Single word
        assertMultiMatchQuery(parseKqlQuery("foo"), "foo", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        // Multiple words
        assertMultiMatchQuery(parseKqlQuery("foo bar baz"), "foo bar baz", MultiMatchQueryBuilder.Type.BEST_FIELDS);

        // Escaped keywords
        assertMultiMatchQuery(parseKqlQuery("foo \\and bar"), "foo and bar", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo \\or bar"), "foo or bar", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("\\not foo bar"), "not foo bar", MultiMatchQueryBuilder.Type.BEST_FIELDS);

        // With an escaped characters
        assertMultiMatchQuery(parseKqlQuery("foo \\* bar"), "foo * bar", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo\\(bar\\)"), "foo(bar)", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo\\{bar\\}"), "foo{bar}", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo\\:bar"), "foo:bar", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo\\<bar"), "foo<bar", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo\\>bar"), "foo>bar", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo \\\\ bar"), "foo \\ bar", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo\\\"bar\\\""), "foo\"bar\"", MultiMatchQueryBuilder.Type.BEST_FIELDS);

        // Wrapping terms into parentheses
        assertMultiMatchQuery(parseKqlQuery("(foo baz)"), "foo baz", MultiMatchQueryBuilder.Type.BEST_FIELDS);

        // Trailing operators (AND, NOT, OR) are terms of the match query
        assertMultiMatchQuery(parseKqlQuery("foo AND"), "foo AND", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo OR"), "foo OR", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo NOT"), "foo NOT", MultiMatchQueryBuilder.Type.BEST_FIELDS);

        // Leading operators (AND, OR) are terms of the match query
        assertMultiMatchQuery(parseKqlQuery("AND foo"), "AND foo", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("OR foo"), "OR foo", MultiMatchQueryBuilder.Type.BEST_FIELDS);

        // Lonely operators (AND, NOT, OR)
        assertMultiMatchQuery(parseKqlQuery("AND"), "AND", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("AND AND"), "AND AND", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("AND OR"), "AND OR", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("AND NOT"), "AND NOT", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("OR"), "OR", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("OR AND"), "OR AND", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("OR OR"), "OR OR", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("OR NOT"), "OR NOT", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("NOT"), "NOT", MultiMatchQueryBuilder.Type.BEST_FIELDS);
    }

    public void testParseWildcardQuery() {
        // Single word
        assertQueryStringBuilder(parseKqlQuery("foo*"), "foo*");
        assertQueryStringBuilder(parseKqlQuery("*foo"), "*foo");
        assertQueryStringBuilder(parseKqlQuery("fo*o"), "fo*o");

        // Multiple words
        assertQueryStringBuilder(parseKqlQuery("fo* bar"), "fo* bar");
        assertQueryStringBuilder(parseKqlQuery("foo * bar"), "foo * bar");
        assertQueryStringBuilder(parseKqlQuery("* foo bar"), "* foo bar");
        assertQueryStringBuilder(parseKqlQuery("foo bar *"), "foo bar *");

        // Check Lucene query string special chars are escaped
        assertQueryStringBuilder(parseKqlQuery("foo*[bar]"), "foo*\\[bar\\]");
        assertQueryStringBuilder(parseKqlQuery("+foo* -bar"), "\\+foo* \\-bar");

        // Trailing operators AND, NOT, OR are terms of the match query
        assertQueryStringBuilder(parseKqlQuery("foo* AND"), "foo* AND");
        assertQueryStringBuilder(parseKqlQuery("foo* OR"), "foo* OR");
        assertQueryStringBuilder(parseKqlQuery("foo* NOT"), "foo* NOT");

        // Leading operators AND, NOT, OR are terms of the match query
        assertQueryStringBuilder(parseKqlQuery("AND foo*"), "AND foo*");
        assertQueryStringBuilder(parseKqlQuery("OR foo*"), "OR foo*");
    }

    public void testParseQuotedStringQuery() {
        // Single word
        assertMultiMatchQuery(parseKqlQuery("\"foo\""), "foo", MultiMatchQueryBuilder.Type.PHRASE);
        // Multiple words
        assertMultiMatchQuery(parseKqlQuery("\"foo bar\""), "foo bar", MultiMatchQueryBuilder.Type.PHRASE);
        // Containing unescaped KQL reserved keyword
        assertMultiMatchQuery(parseKqlQuery("\"not foo and bar or baz\""), "not foo and bar or baz", MultiMatchQueryBuilder.Type.PHRASE);
        // Containing unescaped KQL reserved characters
        assertMultiMatchQuery(parseKqlQuery("\"foo*: {(<bar>})\""), "foo*: {(<bar>})", MultiMatchQueryBuilder.Type.PHRASE);
    }

    public void testParseCaseInsensitiveFieldlessQueries() {
        // Test case-insensitive unquoted literal queries
        QueryBuilder query = parseKqlQueryCaseInsensitive("Foo");
        assertThat(query, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        assertThat(boolQuery.should(), hasSize(searchableFields().size() - excludedFieldCount()));

        // Verify that each should clause targets a searchable non-date field
        List<QueryBuilder> shouldClauses = boolQuery.should();
        for (QueryBuilder clause : shouldClauses) {
            if (clause instanceof MatchQueryBuilder matchQuery) {
                assertThat(matchQuery.value(), equalTo("Foo"));
                assertThat(matchQuery.lenient(), equalTo(true));
            } else if (clause instanceof TermQueryBuilder termQuery) {
                assertThat(termQuery.value(), equalTo("Foo"));
                assertThat(termQuery.caseInsensitive(), equalTo(true));
            }
        }
    }

    public void testParseCaseInsensitiveWildcardQueries() {
        // Test case-insensitive wildcard queries
        QueryBuilder query = parseKqlQueryCaseInsensitive("Fo*");
        assertThat(query, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        assertThat(boolQuery.should(), hasSize(searchableFields().size() - excludedFieldCount()));

        // Verify that each should clause handles wildcards appropriately
        List<QueryBuilder> shouldClauses = boolQuery.should();
        for (QueryBuilder clause : shouldClauses) {
            if (clause instanceof WildcardQueryBuilder wildcardQuery) {
                assertThat(wildcardQuery.value(), equalTo("Fo*"));
                assertThat(wildcardQuery.caseInsensitive(), equalTo(true));
            } else if (clause instanceof QueryStringQueryBuilder queryStringQuery) {
                assertThat(queryStringQuery.queryString(), equalTo("Fo*"));
            }
        }
    }

    public void testParseCaseInsensitiveQuotedStringQueries() {
        // Test case-insensitive phrase queries
        // Note: With the current implementation, phrase matches use standard MultiMatchQuery
        // even in case-insensitive mode (they bypass the special case-insensitive handling)
        QueryBuilder query = parseKqlQueryCaseInsensitive("\"Foo Bar\"");
        assertThat(query, instanceOf(MultiMatchQueryBuilder.class));

        MultiMatchQueryBuilder multiMatchQuery = (MultiMatchQueryBuilder) query;
        assertThat(multiMatchQuery.value(), equalTo("Foo Bar"));
        assertThat(multiMatchQuery.type(), equalTo(MultiMatchQueryBuilder.Type.PHRASE));
        assertThat(multiMatchQuery.lenient(), equalTo(true));
    }

    public void testParseCaseInsensitiveMultipleWords() {
        // Test case-insensitive multiple word queries
        QueryBuilder query = parseKqlQueryCaseInsensitive("Foo Bar Baz");
        assertThat(query, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        assertThat(boolQuery.should(), hasSize(searchableFields().size() - excludedFieldCount()));

        // Verify that each should clause is a match query with multiple words
        List<QueryBuilder> shouldClauses = boolQuery.should();
        for (QueryBuilder clause : shouldClauses) {
            if (clause instanceof MatchQueryBuilder matchQuery) {
                assertThat(matchQuery.value(), equalTo("Foo Bar Baz"));
                assertThat(matchQuery.lenient(), equalTo(true));
            } else if (clause instanceof TermQueryBuilder termQuery) {
                assertThat(termQuery.value(), equalTo("Foo Bar Baz"));
                assertThat(termQuery.caseInsensitive(), equalTo(true));
            }
        }
    }

    public void testParseCaseInsensitiveWildcardWithSpecialChars() {
        // Test case-insensitive wildcard queries with special characters that need escaping
        QueryBuilder query = parseKqlQueryCaseInsensitive("Fo*[bar]");
        assertThat(query, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        assertThat(boolQuery.should(), hasSize(searchableFields().size() - excludedFieldCount()));

        // Verify proper escaping in query string queries
        List<QueryBuilder> shouldClauses = boolQuery.should();
        for (QueryBuilder clause : shouldClauses) {
            if (clause instanceof QueryStringQueryBuilder queryStringQuery) {
                assertThat(queryStringQuery.queryString(), equalTo("Fo*\\[bar\\]"));
            }
        }
    }

    public void testCaseInsensitiveWithEscapedCharacters() {
        // Test case-insensitive queries with escaped characters
        QueryBuilder query = parseKqlQueryCaseInsensitive("Foo\\*Bar");
        assertThat(query, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;

        // Should not be treated as wildcard since asterisk is escaped
        List<QueryBuilder> shouldClauses = boolQuery.should();
        for (QueryBuilder clause : shouldClauses) {
            if (clause instanceof MatchQueryBuilder matchQuery) {
                assertThat(matchQuery.value(), equalTo("Foo*Bar"));
            } else if (clause instanceof TermQueryBuilder termQuery) {
                assertThat(termQuery.value(), equalTo("Foo*Bar"));
            }
        }
    }

    public void testCaseInsensitiveWithCustomDefaultField() {
        // Test case-insensitive queries with a custom default field pattern
        QueryBuilder query = parseKqlQueryCaseInsensitiveWithDefaultField("foo", KEYWORD_FIELD_NAME);

        assertThat(query, instanceOf(TermQueryBuilder.class));
        TermQueryBuilder termQuery = (TermQueryBuilder) query;
        assertThat(termQuery.fieldName(), equalTo(KEYWORD_FIELD_NAME));
        assertThat(termQuery.value(), equalTo("foo"));
        assertThat(termQuery.caseInsensitive(), equalTo(true));
    }

    public void testCaseInsensitiveWithWildcardDefaultField() {
        // Test case-insensitive queries with a wildcard default field pattern
        QueryBuilder query = parseKqlQueryCaseInsensitiveWithDefaultField("foo", "mapped_*");
        assertThat(query, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;

        // Should target all mapped fields that match the pattern (excluding date fields)
        List<String> expectedFields = searchableFields("mapped_*").stream()
            .filter(fieldName -> fieldName.contains("date") == false && fieldName.contains("range") == false)
            .toList();
        assertThat(boolQuery.should(), hasSize(expectedFields.size()));

        // Verify that all should clauses target appropriate fields
        List<QueryBuilder> shouldClauses = boolQuery.should();        // Verify that each should clause is properly configured
        for (QueryBuilder clause : shouldClauses) {
            if (clause instanceof MatchQueryBuilder matchQuery) {
                assertThat(matchQuery.value(), equalTo("foo"));
                assertThat(matchQuery.lenient(), equalTo(true));
            } else if (clause instanceof TermQueryBuilder termQuery) {
                assertThat(termQuery.value(), equalTo("foo"));
                assertThat(termQuery.caseInsensitive(), equalTo(true));
            }
        }
    }

    public void testCaseInsensitiveEmptyResultHandling() {
        // Test behavior when no fields match after filtering (edge case)
        // This creates a scenario where all default fields are date fields
        QueryBuilder query = parseKqlQueryCaseInsensitiveWithDefaultField("test", DATE_FIELD_NAME);
        assertThat(query, instanceOf(MatchNoneQueryBuilder.class));
    }

    /**
     * Helper method to parse KQL query with case-insensitive mode enabled
     */
    private QueryBuilder parseKqlQueryCaseInsensitive(String kqlQuery) {
        KqlParser parser = new KqlParser();
        KqlParsingContext kqlParserContext = KqlParsingContext.builder(createQueryRewriteContext()).caseInsensitive(true).build();
        return parser.parseKqlQuery(kqlQuery, kqlParserContext);
    }

    /**
     * Helper method to parse KQL query with case-insensitive mode and custom default field
     */
    private QueryBuilder parseKqlQueryCaseInsensitiveWithDefaultField(String kqlQuery, String defaultField) {
        KqlParser parser = new KqlParser();
        KqlParsingContext kqlParserContext = KqlParsingContext.builder(createQueryRewriteContext())
            .caseInsensitive(true)
            .defaultField(defaultField)
            .build();
        return parser.parseKqlQuery(kqlQuery, kqlParserContext);
    }

    /**
     * Helper method to count the number of excluded fields in searchable fields.
     * This is used to calculate expected should clause counts since date and range fields
     * are filtered out in case-insensitive queries.
     */
    private int excludedFieldCount() {
        return (int) searchableFields().stream().filter(fieldName -> fieldName.contains("date") || fieldName.contains("range")).count();
    }
}
