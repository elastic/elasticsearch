/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class KqlParserTests extends AbstractKqlParserTestCase {

    public void testEmptyQueryParsing() {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        {
            // In Kql, an empty query is a match_all query.
            assertThat(parser.parseKqlQuery("", searchExecutionContext), isA(MatchAllQueryBuilder.class));
        }

        for (int runs = 0; runs < 100; runs++) {
            // Also testing that a query that is composed only of whitespace chars returns a match_all query.
            String kqlQuery = randomWhitespaces();
            assertThat(parser.parseKqlQuery(kqlQuery, searchExecutionContext), isA(MatchAllQueryBuilder.class));
        }
    }

    public void testMatchAllQuery() {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        assertThat(parser.parseKqlQuery("*", searchExecutionContext), isA(MatchAllQueryBuilder.class));
        assertThat(parser.parseKqlQuery(wrapWithRandomWhitespaces("*"), searchExecutionContext), isA(MatchAllQueryBuilder.class));
        assertThat(parser.parseKqlQuery("*:*", searchExecutionContext), isA(MatchAllQueryBuilder.class));
        assertThat(
            parser.parseKqlQuery(String.join(wrapWithRandomWhitespaces(":"), "*", "*"), searchExecutionContext),
            isA(MatchAllQueryBuilder.class)
        );
    }

    public void testParenthesizedQuery() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (String baseQuuery : readQueries(SUPPORTED_QUERY_FILE_PATH)) {
            // For each supported query, wrap it into parentheses and check query remains the same.
            // Adding random whitespaces as well and test they are ignored.
            String parenthesizedQuery = wrapWithRandomWhitespaces("(") + baseQuuery + wrapWithRandomWhitespaces(")");
            assertThat(
                parser.parseKqlQuery(parenthesizedQuery, searchExecutionContext),
                equalTo(parser.parseKqlQuery(baseQuuery, searchExecutionContext))
            );
        }
    }

    public void testSupportedQueries() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (String query : readQueries(SUPPORTED_QUERY_FILE_PATH)) {
            try {
                parser.parseKqlQuery(query, searchExecutionContext);
            } catch (Throwable e) {
                throw new AssertionError("Unexpected error during query parsing [ " + query + "]", e);
            }
        }
    }

    public void testUnsupportedQueries() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (String query : readQueries(UNSUPPORTED_QUERY_FILE_PATH)) {
            assertThrows(
                "Was expecting a KqlParsingException exception to be thrown while parsing query [" + query + "]",
                KqlParsingException.class,
                () -> parser.parseKqlQuery(query, searchExecutionContext)
            );
        }
    }

    public void testSyntaxErrorsHandling() {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        {
            KqlParsingException e = assertThrows(
                KqlParsingException.class,
                () -> parser.parseKqlQuery("foo: \"bar", searchExecutionContext)
            );
            assertThat(e.getLineNumber(), equalTo(1));
            assertThat(e.getColumnNumber(), equalTo(6));
            assertThat(e.getMessage(), equalTo("line 1:6: token recognition error at: '\"bar'"));
        }

        {
            KqlParsingException e = assertThrows(
                KqlParsingException.class,
                () -> parser.parseKqlQuery("foo: (bar baz AND qux", searchExecutionContext)
            );
            assertThat(e.getLineNumber(), equalTo(1));
            assertThat(e.getColumnNumber(), equalTo(15));
            assertThat(e.getMessage(), equalTo("line 1:15: missing ')' at 'AND'"));
        }
    }
}
