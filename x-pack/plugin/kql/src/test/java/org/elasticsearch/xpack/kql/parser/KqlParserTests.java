/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.SuppressForbidden;
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class KqlParserTests extends AbstractBuilderTestCase {

    public void testEmptyQueryParsing() {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        assertThat(parser.parseKqlQuery("", searchExecutionContext), isA(MatchAllQueryBuilder.class));
    }

    public void testSupportedQueries() throws Exception {
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

    public void testUnsupportedQueries() throws Exception {
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

    private static List<String> readQueries(String source) throws Exception {
        URL url = KqlParserTests.class.getResource(source);
        Objects.requireNonNull(source, "Cannot find resource " + url);

        List<String> queries = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(readFromJarUrl(url), StandardCharsets.UTF_8))) {
            String line;

            while ((line = reader.readLine()) != null) {
                String query = line.trim();
                // ignore comments
                if (query.isEmpty() == false && query.startsWith("//") == false) {
                    queries.add(line.trim());
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
}
