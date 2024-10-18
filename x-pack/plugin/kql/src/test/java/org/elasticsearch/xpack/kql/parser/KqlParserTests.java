/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.AbstractBuilderTestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

public class KqlParserTests extends AbstractBuilderTestCase {

    private static final Predicate<String> BOOLEAN_QUERY_FILTER = (q) -> q.matches("(?i)[^{]*[^\\\\](AND|OR)[^}]*");

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

    public void testParenthesizedQuery() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (String baseQuuery : readQueries("/supported-queries")) {
            // For each supported query, wrap it into parentheses and check query remains the same.
            // Adding random whitespaces as well and test they are ignored.
            String parenthesizedQuery = wrapWithRandomWhitespaces("(") + baseQuuery + wrapWithRandomWhitespaces(")");
            assertThat(
                parser.parseKqlQuery(parenthesizedQuery, searchExecutionContext),
                equalTo(parser.parseKqlQuery(baseQuuery, searchExecutionContext))
            );
        }
    }

    public void testNotQuery() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (String baseQuery : readQueries("/supported-queries")) {
            if (baseQuery.startsWith("NOT") || BOOLEAN_QUERY_FILTER.test(baseQuery)) {
                baseQuery = wrapWithRandomWhitespaces("(") + baseQuery + wrapWithRandomWhitespaces(")");
            }

            String notQuery = wrapWithRandomWhitespaces("NOT ") + baseQuery;

            BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parser.parseKqlQuery(notQuery, searchExecutionContext));

            assertThat(parsedQuery.filter(), empty());
            assertThat(parsedQuery.should(), empty());
            assertThat(parsedQuery.must(), empty());
            assertThat(parsedQuery.mustNot(), hasSize(1));
            assertThat(parsedQuery.mustNot(), hasItem(equalTo((parser.parseKqlQuery(baseQuery, searchExecutionContext)))));

            assertThat(
                parser.parseKqlQuery(
                    "NOT" + wrapWithRandomWhitespaces("(") + baseQuery + wrapWithRandomWhitespaces(")"),
                    searchExecutionContext
                ),
                equalTo(parsedQuery)
            );
        }

        // TODO: when booleanQuery is implemented, add some precedence tests.
    }

    public void testSupportedQueries() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (String query : readQueries("/supported-queries")) {
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

        for (String query : readQueries("/unsupported-queries")) {
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

    private static List<String> readQueries(String source) throws IOException {
        URL url = KqlParserTests.class.getResource(source);
        Objects.requireNonNull(source, "Cannot find resource " + url);

        List<String> queries = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(readFromJarUrl(url), StandardCharsets.UTF_8))) {
            String line;

            while ((line = reader.readLine()) != null) {
                String query = line.trim();
                // ignore comments
                if (query.isEmpty() == false && query.startsWith("//") == false) {
                    queries.add(query);
                }
            }
        }
        return queries;
    }

    @SuppressForbidden(reason = "test reads from jar")
    private static InputStream readFromJarUrl(URL source) throws IOException {
        URLConnection con = source.openConnection();
        // do not to cache files (to avoid keeping file handles around)
        con.setUseCaches(false);
        return con.getInputStream();
    }

    private static String wrapWithRandomWhitespaces(String input) {
        return String.join("", randomWhitespaces(), input, randomWhitespaces());
    }

    private static String randomWhitespaces() {
        return randomWhitespaces(randomInt(20));
    }

    private static String randomWhitespaces(int length) {
        return Stream.generate(() -> randomFrom(" ", "\t", "\n", "\r", "\u3000")).limit(length).collect(Collectors.joining());
    }
}
