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

public final class CsvSpecReader {

    private CsvSpecReader() {}

    public static SpecReader.Parser specParser() {
        var ctx = new ParserContext();
        ctx.addOptionParser(new Capability(ctx));
        ctx.addOptionParser(new RequestStored(ctx));
        ctx.addOptionParser(new RequestTimeFilter(ctx));
        ctx.addOptionParser(new Warning(ctx));
        ctx.addOptionParser(new WarningRegex(ctx));
        ctx.addOptionParser(new IgnoreOrder(ctx));
        return ctx;
    }

    private static Pattern warningRegexToPattern(String regex) {
        return Pattern.compile(".*" + regex + ".*");
    }

    public enum WhenLoadsRequestedToStored {
        SKIP,
        IGNORE_ORDER,
        IGNORE_VALUE_ORDER
    }

    private static class ParserContext implements SpecReader.Parser {
        private final StringBuilder query = new StringBuilder();
        private final StringBuilder data = new StringBuilder();
        private final List<String> requiredCapabilities = new ArrayList<>();
        private final List<SpecReader.Parser> optionParsers = new ArrayList<>();
        WhenLoadsRequestedToStored requestStored = WhenLoadsRequestedToStored.IGNORE_VALUE_ORDER;
        String timestampBoundsGte;
        String timestampBoundsLte;
        CsvTestCase testCase;

        private ParserContext() {}

        public <T extends SpecReader.Parser> void addOptionParser(T parser) {
            this.optionParsers.add(parser);
        }

        @Override
        public Object parse(String line) {
            if (testCase == null) {
                return parsePreamble(line);
            }
            return parseResult(line);
        }

        private Object parsePreamble(String line) {
            for (SpecReader.Parser p : optionParsers) {
                if (p.parse(line) != null) return null;
            }
            if (line.endsWith("\\;")) {
                query.append(line, 0, line.length() - 2).append(";\r\n");
            } else if (line.endsWith(";")) {
                query.append(line.substring(0, line.length() - 1).trim());
                testCase = new CsvTestCase();
                testCase.query = query.toString();
                testCase.requiredCapabilities = List.copyOf(requiredCapabilities);
                testCase.requestStored = requestStored;
                testCase.timestampBoundsGte = timestampBoundsGte;
                testCase.timestampBoundsLte = timestampBoundsLte;
                requiredCapabilities.clear();
                requestStored = WhenLoadsRequestedToStored.IGNORE_VALUE_ORDER;
                timestampBoundsGte = null;
                timestampBoundsLte = null;
                query.setLength(0);
            } else {
                query.append(line).append("\r\n");
            }
            return null;
        }

        private Object parseResult(String line) {
            for (SpecReader.Parser p : optionParsers) {
                if (p.parse(line) != null) return null;
            }
            if (line.startsWith(";")) {
                testCase.expectedResults = data.toString();
                CsvTestCase result = testCase;
                testCase = null;
                data.setLength(0);
                return result;
            }
            data.append(line).append("\r\n");
            return null;
        }
    }

    record Capability(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("required_capability:")) {
                state.requiredCapabilities.add(line.substring("required_capability:".length()).trim());
                return Boolean.TRUE;
            }
            return null;
        }
    }

    record RequestStored(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("request_stored:")) {
                String value = lower.substring("request_stored:".length()).trim();
                state.requestStored = switch (value) {
                    case "skip" -> WhenLoadsRequestedToStored.SKIP;
                    case "ignore_order" -> WhenLoadsRequestedToStored.IGNORE_ORDER;
                    case "ignore_value_order" -> WhenLoadsRequestedToStored.IGNORE_VALUE_ORDER;
                    default -> throw new IllegalArgumentException(
                        "Invalid value for request_stored: [" + value + "], it can only be [skip], [ignore_order], or [ignore_value_order]"
                    );
                };
                return Boolean.TRUE;
            }
            return null;
        }
    }

    record RequestTimeFilter(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("request_time_filter:")) {
                String value = line.substring("request_time_filter:".length()).trim();
                int comma = value.indexOf(',');
                if (comma < 0) {
                    throw new IllegalArgumentException(
                        "request_time_filter must be two ISO-8601 instants separated by a comma: [" + value + "]"
                    );
                }
                state.timestampBoundsGte = value.substring(0, comma).trim();
                state.timestampBoundsLte = value.substring(comma + 1).trim();
                if (state.timestampBoundsGte.isEmpty() || state.timestampBoundsLte.isEmpty()) {
                    throw new IllegalArgumentException("request_time_filter values must not be empty: [" + value + "]");
                }
                return Boolean.TRUE;
            }
            return null;
        }
    }

    record Warning(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            if (state.testCase == null) return null;
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("warning:")) {
                if (state.testCase.expectedWarningsRegex.isEmpty() == false) {
                    throw new IllegalArgumentException("Cannot mix warnings and regex warnings in CSV SPEC files: [" + line + "]");
                }
                state.testCase.expectedWarnings.add(line.substring("warning:".length()).trim());
                return Boolean.TRUE;
            }
            return null;
        }
    }

    record WarningRegex(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            if (state.testCase == null) return null;
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("warningregex:")) {
                if (state.testCase.expectedWarnings.isEmpty() == false) {
                    throw new IllegalArgumentException("Cannot mix warnings and regex warnings in CSV SPEC files: [" + line + "]");
                }
                String regex = line.substring("warningregex:".length()).trim();
                state.testCase.expectedWarningsRegexString.add(regex);
                state.testCase.expectedWarningsRegex.add(warningRegexToPattern(regex));
                return Boolean.TRUE;
            }
            return null;
        }
    }

    record IgnoreOrder(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            if (state.testCase == null) return null;
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("ignoreorder:")) {
                String value = lower.substring("ignoreorder:".length()).trim();
                if ("true".equals(value)) {
                    state.testCase.ignoreOrder = true;
                } else if ("false".equals(value) == false) {
                    throw new IllegalArgumentException("Invalid value for ignoreOrder: [" + value + "], it can only be true or false");
                }
                return Boolean.TRUE;
            }
            return null;
        }
    }

    public static class CsvTestCase {
        final List<String> expectedWarnings = new ArrayList<>();
        final List<String> expectedWarningsRegexString = new ArrayList<>();
        final List<Pattern> expectedWarningsRegex = new ArrayList<>();
        public String query;
        public String expectedResults;
        public boolean ignoreOrder;
        /**
         * How to change the test when requesting all values be loaded from stored fields.
         */
        public WhenLoadsRequestedToStored requestStored;
        public List<String> requiredCapabilities = List.of();
        /**
         * When set from a {@code timestamp_bounds:} line in the expected-results section, the REST request includes
         * a Query DSL range on {@code @timestamp} with these bounds (inclusive).
         */
        public String timestampBoundsGte;
        public String timestampBoundsLte;

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

        /**
         * Allows all warnings in the result.
         */
        public void allowAllWarnings() {
            expectedWarnings.clear();
            expectedWarningsRegexString.clear();
            expectedWarningsRegexString.add(".*");
            expectedWarningsRegex.clear();
            expectedWarningsRegex.addAll(expectedWarningsRegexString.stream().map(CsvSpecReader::warningRegexToPattern).toList());
        }

        public List<Pattern> expectedWarningsRegex() {
            return expectedWarningsRegex;
        }

        /**
         * How should we assert the warnings returned by ESQL.
         * @param deduplicateExact Should tests configured with {@code warnings:} deduplicate
         *                         the warnings before asserting? Normally don't do it because
         *                         duplicate warnings are lame. We'd like to fix them all. But
         *                         in multi-node and multi-shard tests we can emit duplicate
         *                         warnings and it isn't worth fixing them now.
         */
        public AssertWarnings assertWarnings(boolean deduplicateExact) {
            if (expectedWarnings.isEmpty() == false) {
                return deduplicateExact
                    ? new AssertWarnings.DeduplicatedStrings(expectedWarnings)
                    : new AssertWarnings.ExactStrings(expectedWarnings);
            }
            if (expectedWarningsRegex.isEmpty() == false) {
                return new AssertWarnings.AllowedRegexes(expectedWarningsRegex);
            }
            return new AssertWarnings.NoWarnings();
        }
    }

}
