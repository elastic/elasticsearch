/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

public class KqlParserFieldlessQueryTests extends AbstractKqlParserTestCase {

    public void testLiteralFieldlessQuery() {
        // Single word
        assertMultiMatchQuery(parseKqlQuery("foo"), "foo", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        // Multiple words
        assertMultiMatchQuery(parseKqlQuery("foo bar baz"), "foo bar baz", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        // With an escaped wildcard
        assertMultiMatchQuery(parseKqlQuery("foo \\* baz"), "foo * baz", MultiMatchQueryBuilder.Type.BEST_FIELDS);
        // Wrapping terms into parentheses
        assertMultiMatchQuery(parseKqlQuery("foo baz"), "foo baz", MultiMatchQueryBuilder.Type.BEST_FIELDS);
    }

    public void testWildcardFieldlessQuery() {
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
    }

    public void testMatchPhraseQuery() {
        // Single word
        assertMultiMatchQuery(parseKqlQuery("\"foo\""), "foo", MultiMatchQueryBuilder.Type.PHRASE);
        // Multiple words
        assertMultiMatchQuery(parseKqlQuery("\"foo bar\""), "foo bar", MultiMatchQueryBuilder.Type.PHRASE);
        // Containing unescaped KQL reserved keyword
        assertMultiMatchQuery(parseKqlQuery("\"not foo and bar or baz\""), "not foo and bar or baz", MultiMatchQueryBuilder.Type.PHRASE);
        // Containing unescaped KQL reserved characters
        assertMultiMatchQuery(parseKqlQuery("\"foo*: {(<bar>})\""), "foo*: {(<bar>})", MultiMatchQueryBuilder.Type.PHRASE);
    }

    private void assertMultiMatchQuery(QueryBuilder query, String expectedValue, MultiMatchQueryBuilder.Type expectedType) {
        MultiMatchQueryBuilder multiMatchQuery = asInstanceOf(MultiMatchQueryBuilder.class, query);
        assertThat(multiMatchQuery.fields(), anEmptyMap());
        assertThat(multiMatchQuery.lenient(), equalTo(true));
        assertThat(multiMatchQuery.type(), equalTo(expectedType));
        assertThat(multiMatchQuery.value(), equalTo(expectedValue));
    }

    private void assertQueryStringBuilder(QueryBuilder query, String expectedValue) {
        QueryStringQueryBuilder queryStringQuery = asInstanceOf(QueryStringQueryBuilder.class, query);
        assertThat(queryStringQuery.queryString(), equalTo(expectedValue));
    }
}
