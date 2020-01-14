/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.tree.Source;

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

/**
 * Test for checking the overall grammar by throwing a number of valid queries at the parser to see whether any exception is raised.
 * In time, the queries themselves get to be checked against the actual execution model and eventually against the expected results.
 */
public class GrammarTests extends ESTestCase {

    public void testGrammar() throws Exception {
        EqlParser parser = new EqlParser();
        List<Tuple<String, Integer>> lines = readQueries("/grammar-queries.eql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            try {
                parser.createStatement(q);
            } catch (ParsingException pe) {
                if (pe.getErrorMessage().startsWith("Does not know how to handle")) {
                    // ignore for now
                }
                else {
                    throw new ParsingException(new Source(pe.getLineNumber() + line.v2() - 1, pe.getColumnNumber(), q),
                            pe.getErrorMessage() + " inside statement <{}>", q);
                }
            }
        }
    }

    private static List<Tuple<String, Integer>> readQueries(String source) throws Exception {
        URL url = GrammarTests.class.getResource(source);
        Objects.requireNonNull(source, "Cannot find resource " + url);

        List<Tuple<String, Integer>> queries = new ArrayList<>();

        StringBuilder query = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(readFromJarUrl(url), StandardCharsets.UTF_8))) {
            String line;
            int lineNumber = 1;

            while ((line = reader.readLine()) != null) {
                // ignore comments
                if (line.isEmpty() == false && line.startsWith("//") == false) {
                    query.append(line);

                    if (line.endsWith(";") == true) {
                        query.setLength(query.length() - 1);
                        queries.add(new Tuple<>(query.toString(), lineNumber));
                        query.setLength(0);
                    }
                }
                lineNumber++;
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
