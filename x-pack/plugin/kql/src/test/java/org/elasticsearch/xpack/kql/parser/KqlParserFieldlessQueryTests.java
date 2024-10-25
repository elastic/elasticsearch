/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.MultiMatchQueryBuilder;

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

        // Trailing operators AND, NOT, OR are terms of the match query
        assertMultiMatchQuery(parseKqlQuery("foo AND"), "foo AND", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo OR"), "foo OR", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("foo NOT"), "foo NOT", MultiMatchQueryBuilder.Type.BEST_FIELDS);

        // Leading operators AND, NOT, OR are terms of the match query
        assertMultiMatchQuery(parseKqlQuery("AND foo"), "AND foo", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        assertMultiMatchQuery(parseKqlQuery("OR foo"), "OR foo", MultiMatchQueryBuilder.Type.BEST_FIELDS);

        // Lone NOT operator
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

        // Check query string special chars are escaped
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
}
