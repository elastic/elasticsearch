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
import static org.junit.Assert.assertTrue;

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
                } else if (line.toLowerCase(Locale.ROOT).startsWith("ignoreorder:")) {
                    testCase.ignoreOrder = Boolean.parseBoolean(line.substring("ignoreOrder:".length()).trim());
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
        private final List<String> expectedWarnings = new ArrayList<>();
        public boolean ignoreOrder;

        // The emulated-specific warnings must always trail the non-emulated ones, if these are present. Otherwise, the closing bracket
        // would need to be changed to a less common sequence (like `]#` maybe).
        private static final String EMULATED_PREFIX = "#[emulated:";

        /**
         * Returns the warning headers expected to be added by the test. To declare such a header, use the `warning:definition` format
         * in the CSV test declaration. The `definition` can use the `EMULATED_PREFIX` string to specify the format of the warning run on
         * emulated physical operators, if this differs from the format returned by SingleValueQuery.
         * @param forEmulated if true, the tests are run on emulated physical operators; if false, the test case is for queries executed
         *                   on a "full stack" ESQL, having data loaded from Lucene.
         * @return the list of headers that are expected to be returned part of the response.
         */
        public List<String> expectedWarnings(boolean forEmulated) {
            List<String> warnings = new ArrayList<>(expectedWarnings.size());
            for (String warning : expectedWarnings) {
                int idx = warning.toLowerCase(Locale.ROOT).indexOf(EMULATED_PREFIX);
                if (idx >= 0) {
                    assertTrue("Invalid warning spec: closing delimiter (]) missing: `" + warning + "`", warning.endsWith("]"));
                    if (forEmulated) {
                        if (idx + EMULATED_PREFIX.length() < warning.length() - 1) {
                            warnings.add(warning.substring(idx + EMULATED_PREFIX.length(), warning.length() - 1));
                        }
                    } else if (idx > 0) {
                        warnings.add(warning.substring(0, idx));
                    } // else: no warnings expected for non-emulated
                } else {
                    warnings.add(warning);
                }
            }
            return warnings;
        }
    }

}
