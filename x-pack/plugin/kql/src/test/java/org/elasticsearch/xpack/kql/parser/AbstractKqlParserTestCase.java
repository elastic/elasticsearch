/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.query.QueryBuilder;
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractKqlParserTestCase extends AbstractBuilderTestCase {

    protected static final String SUPPORTED_QUERY_FILE_PATH = "/supported-queries";
    protected static final String UNSUPPORTED_QUERY_FILE_PATH = "/unsupported-queries";
    protected static final Predicate<String> BOOLEAN_QUERY_FILTER = (q) -> q.matches("(?i)[^{]*[^\\\\](AND|OR)[^}]*");

    protected static String wrapWithRandomWhitespaces(String input) {
        return String.join("", randomWhitespaces(), input, randomWhitespaces());
    }

    protected static String randomWhitespaces() {
        return randomWhitespaces(randomInt(20));
    }

    protected static String randomWhitespaces(int length) {
        return Stream.generate(() -> randomFrom(" ", "\t", "\n", "\r", "\u3000")).limit(length).collect(Collectors.joining());
    }

    protected static List<String> readQueries(String source) throws IOException {
        return readQueries(source, Predicates.always());
    }

    protected static List<String> readQueries(String source, Predicate<String> filter) throws IOException {
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

    protected List<String> mappedLeafFields() {
        return Stream.concat(
            Arrays.stream(MAPPED_LEAF_FIELD_NAMES),
            List.of(DATE_FIELD_NAME, INT_FIELD_NAME).stream().map(subfieldName -> OBJECT_FIELD_NAME + "." + subfieldName)
        ).toList();
    }

    protected QueryBuilder parseKqlQuery(String kqlQuery) {
        KqlParser parser = new KqlParser();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        return parser.parseKqlQuery(kqlQuery, searchExecutionContext);
    }
}
