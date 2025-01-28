/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

public class KqlParserFieldQueryTests extends AbstractKqlParserTestCase {

    public void testParseFieldQueryWithNoMatchingFields() {
        // Using an unquoted literal
        assertThat(parseKqlQuery(kqlFieldQuery("not_a_field", randomIdentifier())), isA(MatchNoneQueryBuilder.class));
        assertThat(parseKqlQuery(kqlFieldQuery("not_a_field", randomInt())), isA(MatchNoneQueryBuilder.class));
        assertThat(parseKqlQuery(kqlFieldQuery("not_a_field", quoteString(randomIdentifier()))), isA(MatchNoneQueryBuilder.class));

        // Using a quoted string as field name
        assertThat(parseKqlQuery(kqlFieldQuery(quoteString("not_a_field"), randomIdentifier())), isA(MatchNoneQueryBuilder.class));
        assertThat(parseKqlQuery(kqlFieldQuery(quoteString("not_a_field"), randomInt())), isA(MatchNoneQueryBuilder.class));
        assertThat(
            parseKqlQuery(kqlFieldQuery(quoteString("not_a_field"), quoteString(randomIdentifier()))),
            isA(MatchNoneQueryBuilder.class)
        );

        // Not expanding wildcard with quoted string
        assertThat(parseKqlQuery(kqlFieldQuery(quoteString("mapped_*"), randomIdentifier())), isA(MatchNoneQueryBuilder.class));
        assertThat(parseKqlQuery(kqlFieldQuery(quoteString("mapped_*"), randomInt())), isA(MatchNoneQueryBuilder.class));
        assertThat(
            parseKqlQuery(kqlFieldQuery(quoteString("mapped_*"), quoteString(randomIdentifier()))),
            isA(MatchNoneQueryBuilder.class)
        );
    }

    public void testParseUnquotedLiteralKeywordFieldQuery() {
        // Single word
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo")), KEYWORD_FIELD_NAME, "foo");

        // Multiple words
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo bar")), KEYWORD_FIELD_NAME, "foo bar");

        // Escaped keywords
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo \\and bar")), KEYWORD_FIELD_NAME, "foo and bar");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo \\or bar")), KEYWORD_FIELD_NAME, "foo or bar");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "\\not foo bar")), KEYWORD_FIELD_NAME, "not foo bar");

        // Escaped characters
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo \\* bar")), KEYWORD_FIELD_NAME, "foo * bar");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo\\(bar\\)")), KEYWORD_FIELD_NAME, "foo(bar)");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo\\{bar\\}")), KEYWORD_FIELD_NAME, "foo{bar}");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo\\:bar")), KEYWORD_FIELD_NAME, "foo:bar");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo\\<bar")), KEYWORD_FIELD_NAME, "foo<bar");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo\\>bar")), KEYWORD_FIELD_NAME, "foo>bar");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo \\\\ bar")), KEYWORD_FIELD_NAME, "foo \\ bar");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo\\\"bar\\\"")), KEYWORD_FIELD_NAME, "foo\"bar\"");

        // Wrapping terms into parentheses
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "(foo baz)")), KEYWORD_FIELD_NAME, "foo baz");

        // Trailing operators (AND, NOT, OR) are terms of the match query
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "bar AND")), KEYWORD_FIELD_NAME, "bar AND");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "bar OR")), KEYWORD_FIELD_NAME, "bar OR");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "bar NOT")), KEYWORD_FIELD_NAME, "bar NOT");

        // Leading operators (AND, OR) are terms of the match query
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "AND bar")), KEYWORD_FIELD_NAME, "AND bar");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "OR bar")), KEYWORD_FIELD_NAME, "OR bar");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "NOT bar")), KEYWORD_FIELD_NAME, "NOT bar");

        // Lonely operators (AND, NOT, OR)
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "AND AND")), KEYWORD_FIELD_NAME, "AND AND");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "AND OR")), KEYWORD_FIELD_NAME, "AND OR");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "AND NOT")), KEYWORD_FIELD_NAME, "AND NOT");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "OR")), KEYWORD_FIELD_NAME, "OR");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "OR AND")), KEYWORD_FIELD_NAME, "OR AND");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "OR OR")), KEYWORD_FIELD_NAME, "OR OR");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "OR NOT")), KEYWORD_FIELD_NAME, "OR NOT");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "NOT")), KEYWORD_FIELD_NAME, "NOT");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "NOT AND")), KEYWORD_FIELD_NAME, "NOT AND");
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "NOT OR")), KEYWORD_FIELD_NAME, "NOT OR");

        // Check we can use quoted field name as well
        assertThat(
            parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "foo")),
            equalTo(parseKqlQuery(kqlFieldQuery(quoteString(KEYWORD_FIELD_NAME), "foo")))
        );
    }

    public void testParseDateFieldQuery() {
        assertRangeQueryBuilder(parseKqlQuery(kqlFieldQuery(DATE_FIELD_NAME, "2010-06-03")), DATE_FIELD_NAME, (rangeQuery) -> {
            assertThat(rangeQuery.from(), equalTo("2010-06-03"));
            assertThat(rangeQuery.includeLower(), equalTo(true));
            assertThat(rangeQuery.to(), equalTo("2010-06-03"));
            assertThat(rangeQuery.includeUpper(), equalTo(true));

            // Check we can use quoted field name as well
            assertThat(parseKqlQuery(kqlFieldQuery(quoteString(DATE_FIELD_NAME), "2010-06-03")), equalTo(rangeQuery));

            // Check we can use quoted value as well
            assertThat(parseKqlQuery(kqlFieldQuery(DATE_FIELD_NAME, quoteString("2010-06-03"))), equalTo(rangeQuery));
        });
    }

    public void testParseUnquotedLiteralMatchFieldsQuery() {
        for (String fieldName : List.of(TEXT_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME)) {
            // Single word
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo")), fieldName, "foo");

            // Numbers are converted to string
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, 1)), fieldName, "1");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, 1.5)), fieldName, "1.5");

            // Multiple words
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo bar")), fieldName, "foo bar");

            // Escaped keywords
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo \\and bar")), fieldName, "foo and bar");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo \\or bar")), fieldName, "foo or bar");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "\\not foo bar")), fieldName, "not foo bar");

            // Escaped characters
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo \\* bar")), fieldName, "foo * bar");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo\\(bar\\)")), fieldName, "foo(bar)");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo\\{bar\\}")), fieldName, "foo{bar}");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo\\:bar")), fieldName, "foo:bar");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo\\<bar")), fieldName, "foo<bar");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo\\>bar")), fieldName, "foo>bar");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo \\\\ bar")), fieldName, "foo \\ bar");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo\\\"bar\\\"")), fieldName, "foo\"bar\"");

            // Wrapping terms into parentheses
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "(foo baz)")), fieldName, "foo baz");

            // Trailing operators (AND, NOT, OR) are terms of the match query
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "bar AND")), fieldName, "bar AND");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "bar OR")), fieldName, "bar OR");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "bar NOT")), fieldName, "bar NOT");

            // Leading operators (AND, OR) are terms of the match query
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "AND bar")), fieldName, "AND bar");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "OR bar")), fieldName, "OR bar");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "NOT bar")), fieldName, "NOT bar");

            // Lonely operators (AND, NOT, OR)
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "AND")), fieldName, "AND");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "AND AND")), fieldName, "AND AND");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "AND OR")), fieldName, "AND OR");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "AND NOT")), fieldName, "AND NOT");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "OR")), fieldName, "OR");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "OR AND")), fieldName, "OR AND");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "OR OR")), fieldName, "OR OR");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "OR NOT")), fieldName, "OR NOT");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "NOT")), fieldName, "NOT");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "NOT AND")), fieldName, "NOT AND");
            assertMatchQueryBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "NOT OR")), fieldName, "NOT OR");

            // Check we can use quoted field name as well
            assertThat(
                parseKqlQuery(kqlFieldQuery(fieldName, "foo")),
                equalTo(parseKqlQuery(kqlFieldQuery(quoteString(fieldName), "foo")))
            );
        }
    }

    public void testParseQuotedStringKeywordFieldQuery() {
        // Single word
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, quoteString("foo"))), KEYWORD_FIELD_NAME, "foo");

        // Multiple words
        assertTermQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, quoteString("foo bar"))), KEYWORD_FIELD_NAME, "foo bar");

        // Containing unescaped KQL reserved keyword
        assertTermQueryBuilder(
            parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, quoteString("not foo and bar or baz"))),
            KEYWORD_FIELD_NAME,
            "not foo and bar or baz"
        );

        // Containing unescaped KQL reserved characters
        assertTermQueryBuilder(
            parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, quoteString("foo*: {(<bar>})"))),
            KEYWORD_FIELD_NAME,
            "foo*: {(<bar>})"
        );
    }

    public void testParseQuotedStringMatchFieldsQuery() {
        for (String fieldName : List.of(TEXT_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME)) {

            // Single word
            assertMatchPhraseBuilder(parseKqlQuery(kqlFieldQuery(fieldName, quoteString("foo"))), fieldName, "foo");

            // Multiple words
            assertMatchPhraseBuilder(parseKqlQuery(kqlFieldQuery(fieldName, quoteString("foo bar"))), fieldName, "foo bar");

            // Containing unescaped KQL reserved keyword
            assertMatchPhraseBuilder(
                parseKqlQuery(kqlFieldQuery(fieldName, quoteString("not foo and bar or baz"))),
                fieldName,
                "not foo and bar or baz"
            );

            // Containing unescaped KQL reserved characters
            assertMatchPhraseBuilder(parseKqlQuery(kqlFieldQuery(fieldName, quoteString("foo*: {(<bar>})"))), fieldName, "foo*: {(<bar>})");
        }
    }

    public void testParseWildcardMatchFieldQuery() {
        for (String fieldName : List.of(TEXT_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME)) {
            // Single word
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo*")), fieldName, "foo*");
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "*foo")), fieldName, "*foo");
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "fo*o")), fieldName, "fo*o");

            // Multiple words
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "fo* bar")), fieldName, "fo* bar");
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo * bar")), fieldName, "foo * bar");
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "* foo bar")), fieldName, "* foo bar");
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo bar *")), fieldName, "foo bar *");

            // Check Lucene query string special chars are escaped
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo*[bar]")), fieldName, "foo*\\[bar\\]");
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "+foo* -bar")), fieldName, "\\+foo* \\-bar");

            // Trailing operators AND, NOT, OR are terms of the match query
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo* AND")), fieldName, "foo* AND");
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo* OR")), fieldName, "foo* OR");
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "foo* NOT")), fieldName, "foo* NOT");

            // Leading operators AND, NOT, OR are terms of the match query
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "AND foo*")), fieldName, "AND foo*");
            assertQueryStringBuilder(parseKqlQuery(kqlFieldQuery(fieldName, "OR foo*")), fieldName, "OR foo*");
        }
    }

    public void testParseWildcardKeywordFieldQuery() {
        // Single word
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo*")), KEYWORD_FIELD_NAME, "fo*");

        // Multiple words
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo* bar")), KEYWORD_FIELD_NAME, "fo* bar");

        // Escaped keywords
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo* \\and bar")), KEYWORD_FIELD_NAME, "fo* and bar");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo* \\or bar")), KEYWORD_FIELD_NAME, "fo* or bar");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "\\not fo* bar")), KEYWORD_FIELD_NAME, "not fo* bar");

        // Escaped characters
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo* \\* bar")), KEYWORD_FIELD_NAME, "fo* * bar");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo*\\(bar\\)")), KEYWORD_FIELD_NAME, "fo*(bar)");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo*\\{bar\\}")), KEYWORD_FIELD_NAME, "fo*{bar}");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo*\\:bar")), KEYWORD_FIELD_NAME, "fo*:bar");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo*\\<bar")), KEYWORD_FIELD_NAME, "fo*<bar");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo*\\>bar")), KEYWORD_FIELD_NAME, "fo*>bar");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo* \\\\ bar")), KEYWORD_FIELD_NAME, "fo* \\ bar");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo*\\\"bar\\\"")), KEYWORD_FIELD_NAME, "fo*\"bar\"");

        // Wrapping terms into parentheses
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "(fo* baz)")), KEYWORD_FIELD_NAME, "fo* baz");

        // Trailing operators (AND, NOT, OR) are terms of the match query
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo* AND")), KEYWORD_FIELD_NAME, "fo* AND");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo* OR")), KEYWORD_FIELD_NAME, "fo* OR");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "fo* NOT")), KEYWORD_FIELD_NAME, "fo* NOT");

        // Leading operators (AND, OR) are terms of the match query
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "AND fo*")), KEYWORD_FIELD_NAME, "AND fo*");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "OR fo*")), KEYWORD_FIELD_NAME, "OR fo*");
        assertWildcardQueryBuilder(parseKqlQuery(kqlFieldQuery(KEYWORD_FIELD_NAME, "NOT fo*")), KEYWORD_FIELD_NAME, "NOT fo*");
    }

    public void testFieldWildcardFieldQueries() {
        List<String> queries = List.of("foo", "foo bar", quoteString("foo"), "foo*");
        for (String query : queries) {
            for (String fieldNamePattern : List.of("mapped_*", "*")) {
                List<String> searchableFields = searchableFields(fieldNamePattern);
                String kqlQuery = kqlFieldQuery(fieldNamePattern, query);
                BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parseKqlQuery(kqlQuery));
                assertThat(parsedQuery.mustNot(), empty());
                assertThat(parsedQuery.must(), empty());
                assertThat(parsedQuery.filter(), empty());
                assertThat(parsedQuery.minimumShouldMatch(), equalTo("1"));
                assertThat(parsedQuery.should(), hasSize(searchableFields.size()));

                assertThat(
                    parsedQuery.should(),
                    containsInAnyOrder(searchableFields.stream().map(fieldName -> parseKqlQuery(kqlFieldQuery(fieldName, query))).toArray())
                );
            }
        }
    }

    private static String kqlFieldQuery(String field, Object value) {
        return wrapWithRandomWhitespaces(field) + wrapWithRandomWhitespaces(":") + wrapWithRandomWhitespaces(value.toString());
    }

    private static String quoteString(String input) {
        return Strings.format("\"%s\"", input);
    }
}
