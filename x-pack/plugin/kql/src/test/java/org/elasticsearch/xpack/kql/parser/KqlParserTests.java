/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class KqlParserTests extends AbstractKqlParserTestCase {

    public void testEmptyQueryParsing() {
        {
            // In Kql, an empty query is a match_all query.
            assertThat(parseKqlQuery(""), isA(MatchAllQueryBuilder.class));
        }

        for (int runs = 0; runs < 100; runs++) {
            // Also testing that a query that is composed only of whitespace chars returns a match_all query.
            String kqlQuery = randomWhitespaces();
            assertThat(parseKqlQuery(kqlQuery), isA(MatchAllQueryBuilder.class));
        }
    }

    public void testMatchAllQuery() {
        assertThat(parseKqlQuery("*"), isA(MatchAllQueryBuilder.class));
        assertThat(parseKqlQuery(wrapWithRandomWhitespaces("*")), isA(MatchAllQueryBuilder.class));
        assertThat(parseKqlQuery("*:*"), isA(MatchAllQueryBuilder.class));
        assertThat(parseKqlQuery(String.join(wrapWithRandomWhitespaces(":"), "*", "*")), isA(MatchAllQueryBuilder.class));
    }

    public void testParenthesizedQuery() throws IOException {
        for (String baseQuuery : readQueries(SUPPORTED_QUERY_FILE_PATH)) {
            // For each supported query, wrap it into parentheses and check query remains the same.
            // Adding random whitespaces as well and test they are ignored.
            String parenthesizedQuery = wrapWithRandomWhitespaces("(") + baseQuuery + wrapWithRandomWhitespaces(")");
            assertThat(parseKqlQuery(parenthesizedQuery), equalTo(parseKqlQuery(baseQuuery)));
        }
    }

    public void testSupportedQueries() throws IOException {
        for (String query : readQueries(SUPPORTED_QUERY_FILE_PATH)) {
            try {
                QueryBuilder parsedQuery = parseKqlQuery(query);

                // leading and trailing whitespaces does not change the query builing result:
                assertThat(parseKqlQuery(wrapWithRandomWhitespaces(query)), equalTo(parsedQuery));
            } catch (Throwable e) {
                throw new AssertionError("Unexpected error during query parsing [ " + query + "]", e);
            }
        }
    }

    public void testUnsupportedQueries() throws IOException {
        for (String query : readQueries(UNSUPPORTED_QUERY_FILE_PATH)) {
            assertThrows(
                "Was expecting a KqlParsingException exception to be thrown while parsing query [" + query + "]",
                KqlParsingException.class,
                () -> parseKqlQuery(query)
            );
        }
    }

    public void testSyntaxErrorsHandling() {
        {
            KqlParsingException e = assertThrows(KqlParsingException.class, () -> parseKqlQuery("foo: \"bar"));
            assertThat(e.getLineNumber(), equalTo(1));
            assertThat(e.getColumnNumber(), equalTo(6));
            assertThat(e.getMessage(), equalTo("line 1:6: token recognition error at: '\"bar'"));
        }

        {
            KqlParsingException e = assertThrows(KqlParsingException.class, () -> parseKqlQuery("foo: (bar baz AND qux"));
            assertThat(e.getLineNumber(), equalTo(1));
            assertThat(e.getColumnNumber(), equalTo(15));
            assertThat(e.getMessage(), equalTo("line 1:15: missing ')' at 'AND'"));
        }
    }
}
