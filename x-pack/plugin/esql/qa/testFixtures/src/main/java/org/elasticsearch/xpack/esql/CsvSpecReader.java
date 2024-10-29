/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Pattern;

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
        private final List<String> requiredCapabilities = new ArrayList<>();
        private CsvTestCase testCase;

        private CsvSpecParser() {}

        @Override
        public Object parse(String line) {
            // read the query
            if (testCase == null) {
                if (line.startsWith(SCHEMA_PREFIX)) {
                    assertThat("Early schema already declared " + earlySchema, earlySchema.length(), is(0));
                    earlySchema.append(line.substring(SCHEMA_PREFIX.length()).trim());
                } else if (line.toLowerCase(Locale.ROOT).startsWith("required_capability:")) {
                    requiredCapabilities.add(line.substring("required_capability:".length()).trim());
                } else {
                    if (line.endsWith(";")) {
                        // pick up the query
                        testCase = new CsvTestCase();
                        query.append(line.substring(0, line.length() - 1).trim());
                        testCase.query = query.toString();
                        testCase.earlySchema = earlySchema.toString();
                        testCase.requiredCapabilities = List.copyOf(requiredCapabilities);
                        requiredCapabilities.clear();
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
                String lower = line.toLowerCase(Locale.ROOT);
                if (lower.startsWith("warning:")) {
                    if (testCase.expectedWarningsRegex.isEmpty() == false) {
                        throw new IllegalArgumentException("Cannot mix warnings and regex warnings in CSV SPEC files: [" + line + "]");
                    }
                    testCase.expectedWarnings.add(line.substring("warning:".length()).trim());
                } else if (lower.startsWith("warningregex:")) {
                    if (testCase.expectedWarnings.isEmpty() == false) {
                        throw new IllegalArgumentException("Cannot mix warnings and regex warnings in CSV SPEC files: [" + line + "]");
                    }
                    String regex = line.substring("warningregex:".length()).trim();
                    testCase.expectedWarningsRegexString.add(regex);
                    testCase.expectedWarningsRegex.add(warningRegexToPattern(regex));
                } else if (lower.startsWith("ignoreorder:")) {
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

    private static Pattern warningRegexToPattern(String regex) {
        return Pattern.compile(".*" + regex + ".*");
    }

    public static class CsvTestCase {
        public String query;
        public String earlySchema;
        public String expectedResults;
        private final List<String> expectedWarnings = new ArrayList<>();
        private final List<String> expectedWarningsRegexString = new ArrayList<>();
        private final List<Pattern> expectedWarningsRegex = new ArrayList<>();
        public boolean ignoreOrder;
        public List<String> requiredCapabilities = List.of();

        /**
         * Returns the warning headers expected to be added by the test. To declare such a header, use the `warning:definition` format
         * in the CSV test declaration. The `definition` can use the `EMULATED_PREFIX` string to specify the format of the warning run on
         * emulated physical operators, if this differs from the format returned by SingleValueQuery.
         * @return the list of headers that are expected to be returned part of the response.
         */
        public List<String> expectedWarnings() {
            List<String> warnings = new ArrayList<>(expectedWarnings.size());
            for (String warning : expectedWarnings) {
                warnings.add(warning);
            }
            return warnings;
        }

        /**
         * Modifies the expected warnings.
         * In some cases, we modify the query to run against multiple clusters. As a result, the line/column positions
         * of the expected warnings no longer match the actual warnings. To enable reusing of spec tests, this method
         * allows adjusting the expected warnings.
         */
        public void adjustExpectedWarnings(Function<String, String> updater) {
            expectedWarnings.replaceAll(updater::apply);
            expectedWarningsRegexString.replaceAll(updater::apply);
            expectedWarningsRegex.clear();
            expectedWarningsRegex.addAll(expectedWarningsRegexString.stream().map(CsvSpecReader::warningRegexToPattern).toList());
        }

        public List<Pattern> expectedWarningsRegex() {
            return expectedWarningsRegex;
        }
    }

}
