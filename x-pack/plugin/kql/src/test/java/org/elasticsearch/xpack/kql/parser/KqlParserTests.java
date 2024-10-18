/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Strings;
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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;

public class KqlParserTests extends AbstractBuilderTestCase {
    private static final String SUPPORTED_QUERY_FILE_PATH = "/supported-queries";
    private static final String UNSUPPORTED_QUERY_FILE_PATH = "/unsupported-queries";
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

    public void testNotQuery() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        for (String baseQuery : readQueries(SUPPORTED_QUERY_FILE_PATH)) {
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
    }

    public void testOrQuery() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        List<String> supportedQueries = readQueries(SUPPORTED_QUERY_FILE_PATH, Predicate.not(BOOLEAN_QUERY_FILTER));

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String orQuery = queryA + wrapWithRandomWhitespaces(randomFrom(" or ", " OR ", " Or ", " oR ")) + queryB;

            BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parser.parseKqlQuery(orQuery, searchExecutionContext));

            assertThat(parsedQuery.filter(), empty());
            assertThat(parsedQuery.must(), empty());
            assertThat(parsedQuery.mustNot(), empty());
            assertThat(parsedQuery.should(), hasSize(2));
            assertThat(parsedQuery.minimumShouldMatch(), equalTo("1"));
            assertThat(
                parsedQuery.should(),
                allOf(
                    hasItem(equalTo((parser.parseKqlQuery(queryA, searchExecutionContext)))),
                    hasItem(equalTo((parser.parseKqlQuery(queryB, searchExecutionContext))))
                )
            );
        }

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String queryC = randomFrom(supportedQueries);
            String orQuery = Strings.format("%s OR %s OR %s", queryA, queryB, queryC);

            BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parser.parseKqlQuery(orQuery, searchExecutionContext));
            assertThat(parsedQuery.should(), hasSize(3));
            assertThat(
                parsedQuery.should(),
                allOf(
                    hasItem(equalTo((parser.parseKqlQuery(queryA, searchExecutionContext)))),
                    hasItem(equalTo((parser.parseKqlQuery(queryB, searchExecutionContext)))),
                    hasItem(equalTo((parser.parseKqlQuery(queryC, searchExecutionContext))))
                )
            );
        }
    }

    public void testAndQuery() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        List<String> supportedQueries = readQueries(SUPPORTED_QUERY_FILE_PATH, Predicate.not(BOOLEAN_QUERY_FILTER));

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String andQuery = queryA + wrapWithRandomWhitespaces(randomFrom(" and ", " AND ", " And ", " anD ")) + queryB;

            BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parser.parseKqlQuery(andQuery, searchExecutionContext));

            assertThat(parsedQuery.filter(), empty());
            assertThat(parsedQuery.should(), empty());
            assertThat(parsedQuery.mustNot(), empty());
            assertThat(parsedQuery.must(), hasSize(2));
            assertThat(
                parsedQuery.must(),
                allOf(
                    hasItem(equalTo((parser.parseKqlQuery(queryA, searchExecutionContext)))),
                    hasItem(equalTo((parser.parseKqlQuery(queryB, searchExecutionContext))))
                )
            );
        }

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String queryC = randomFrom(supportedQueries);
            String andQuery = Strings.format("%s AND %s AND %s", queryA, queryB, queryC);

            BoolQueryBuilder parsedQuery = asInstanceOf(BoolQueryBuilder.class, parser.parseKqlQuery(andQuery, searchExecutionContext));
            assertThat(parsedQuery.must(), hasSize(3));
            assertThat(
                parsedQuery.must(),
                allOf(
                    hasItem(equalTo((parser.parseKqlQuery(queryA, searchExecutionContext)))),
                    hasItem(equalTo((parser.parseKqlQuery(queryB, searchExecutionContext)))),
                    hasItem(equalTo((parser.parseKqlQuery(queryC, searchExecutionContext))))
                )
            );
        }
    }

    public void testOperatorPrecedence() throws IOException {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        List<String> supportedQueries = readQueries(
            SUPPORTED_QUERY_FILE_PATH,
            Predicate.not(BOOLEAN_QUERY_FILTER).and((q -> q.startsWith("NOT") == false))
        );

        for (int runs = 0; runs < 100; runs++) {
            String queryA = randomFrom(supportedQueries);
            String queryB = randomFrom(supportedQueries);
            String queryC = randomFrom(supportedQueries);

            // <QueryA> AND <QueryB> OR <QueryC> is equivalent to <QueryA> AND (<QueryB> OR <QueryC>)
            assertThat(
                parser.parseKqlQuery(Strings.format("%s AND %s OR %s", queryA, queryB, queryC), searchExecutionContext),
                equalTo(parser.parseKqlQuery(Strings.format("%s AND (%s OR %s)", queryA, queryB, queryC), searchExecutionContext))
            );

            // <QueryA> OR <QueryB> AND <QueryC> is equivalent to <QueryA> OR (<QueryB> AND <QueryC>)
            assertThat(
                parser.parseKqlQuery(Strings.format("%s OR %s AND %s", queryA, queryB, queryC), searchExecutionContext),
                equalTo(parser.parseKqlQuery(Strings.format("%s OR (%s AND %s)", queryA, queryB, queryC), searchExecutionContext))
            );

            // <QueryA> AND <QueryB> is equivalent to (NOT <QueryA>) AND <QueryB>
            assertThat(
                parser.parseKqlQuery(Strings.format("NOT %s AND %s", queryA, queryB), searchExecutionContext),
                equalTo(parser.parseKqlQuery(Strings.format("(NOT %s) AND %s", queryA, queryB), searchExecutionContext))
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
            assertThat(e.getMessage(), equalTo("line 1:15: extraneous input 'AND' expecting {')', UNQUOTED_LITERAL, WILDCARD}"));
        }
    }

    private static List<String> readQueries(String source) throws IOException {
        return readQueries(source, Predicates.always());
    }

    private static List<String> readQueries(String source, Predicate<String> filter) throws IOException {
        URL url = KqlParserTests.class.getResource(source);
        Objects.requireNonNull(source, "Cannot find resource " + url);

        List<String> queries = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(readFromJarUrl(url), StandardCharsets.UTF_8))) {
            String line;

            while ((line = reader.readLine()) != null) {
                String query = line.trim();
                // ignore comments
                if (query.isEmpty() == false && query.startsWith("//") == false && filter.test(query)) {
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
