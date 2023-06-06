/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public final class CsvSpecReader {

    private CsvSpecReader() {}

    public static SpecReader.Parser specParser() {
        return new CsvSpecParser();
    }

    public static class CsvSpecParser implements SpecReader.Parser {
        private static final String SCHEMA_PREFIX = "schema::";

        private final StringBuilder earlySchema = new StringBuilder();
        private final StringBuilder query = new StringBuilder();
        private final StringBuilder data = new StringBuilder();
        private CsvTestCase testCase;

        private CsvSpecParser() {}

        @Override
        public Object parse(String line) {
            // read the query
            if (testCase == null) {
                if (line.startsWith(SCHEMA_PREFIX)) {
                    assertThat("Early schema already declared " + earlySchema, earlySchema.length(), is(0));
                    earlySchema.append(line.substring(SCHEMA_PREFIX.length()).trim());
                } else {
                    if (line.endsWith(";")) {
                        // pick up the query
                        testCase = new CsvTestCase();
                        query.append(line.substring(0, line.length() - 1).trim());
                        testCase.query = query.toString();
                        testCase.earlySchema = earlySchema.toString();
                        earlySchema.setLength(0);
                        query.setLength(0);
                    }
                    // keep reading the query
                    else {
                        query.append(line);
                        query.append("\r\n");
                    }
                }
            }
            // read the results
            else {
                // read data
                if (line.toLowerCase(Locale.ROOT).startsWith("warning:")) {
                    testCase.expectedWarnings.add(line.substring("warning:".length()).trim());
                } else if (line.startsWith(";")) {
                    testCase.expectedResults = data.toString();
                    // clean-up and emit
                    CsvTestCase result = testCase;
                    testCase = null;
                    data.setLength(0);
                    return result;
                } else {
                    data.append(line);
                    data.append("\r\n");
                }
            }

            return null;
        }
    }

    public static class CsvTestCase {
        public String query;
        public String earlySchema;
        public String expectedResults;
        public List<String> expectedWarnings = new ArrayList<>();
    }

}
