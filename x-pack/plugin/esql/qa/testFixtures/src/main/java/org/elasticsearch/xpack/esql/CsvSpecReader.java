/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class CsvSpecReader {

    private CsvSpecReader() {}

    public static SpecReader.Parser specParser() {
        var ctx = new ParserContext();
        ctx.addOptionParser(new Capability(ctx));
        ctx.addOptionParser(new Dataset(ctx));
        ctx.addOptionParser(new Pragma(ctx));
        ctx.addOptionParser(new RequestStored(ctx));
        ctx.addOptionParser(new RequestTimeFilter(ctx));
        ctx.addOptionParser(new Warning(ctx));
        ctx.addOptionParser(new WarningRegex(ctx));
        ctx.addOptionParser(new IgnoreOrder(ctx));
        ctx.addOptionParser(new DocumentsFound(ctx));
        ctx.addOptionParser(new SkipFlattenedRewrite(ctx));
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
        private final List<String> requiredCapabilitiesLocalCluster = new ArrayList<>();
        private final List<String> missingCapabilitiesLocalCluster = new ArrayList<>();
        private final List<String> missingCapabilitiesRemoteCluster = new ArrayList<>();
        private final List<DatasetSource> datasetSources = new ArrayList<>();
        private final List<SpecReader.Parser> optionParsers = new ArrayList<>();
        private final Map<String, String> pragmas = new HashMap<>();
        WhenLoadsRequestedToStored requestStored = WhenLoadsRequestedToStored.IGNORE_VALUE_ORDER;
        String requestTimeRangeGte;
        String requestTimeRangeLte;
        String skipFlattenedRewrite;
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
                testCase.requiredCapabilitiesLocalCluster = List.copyOf(requiredCapabilitiesLocalCluster);
                testCase.missingCapabilitiesLocalCluster = List.copyOf(missingCapabilitiesLocalCluster);
                testCase.missingCapabilitiesRemoteCluster = List.copyOf(missingCapabilitiesRemoteCluster);
                testCase.datasetSources = List.copyOf(datasetSources);
                testCase.pragmas = Map.copyOf(pragmas);
                testCase.requestStored = requestStored;
                testCase.requestTimeRangeGte = requestTimeRangeGte;
                testCase.requestTimeRangeLte = requestTimeRangeLte;
                testCase.skipFlattenedRewrite = skipFlattenedRewrite;
                requiredCapabilities.clear();
                requiredCapabilitiesLocalCluster.clear();
                missingCapabilitiesLocalCluster.clear();
                missingCapabilitiesRemoteCluster.clear();
                datasetSources.clear();
                requestStored = WhenLoadsRequestedToStored.IGNORE_VALUE_ORDER;
                requestTimeRangeGte = null;
                requestTimeRangeLte = null;
                skipFlattenedRewrite = null;
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
            if (lower.startsWith("required_capability_coordinator:")) {
                state.requiredCapabilitiesLocalCluster.add(line.substring("required_capability_coordinator:".length()).trim());
                return Boolean.TRUE;
            }
            if (lower.startsWith("missing_capability_coordinator:")) {
                state.missingCapabilitiesLocalCluster.add(line.substring("missing_capability_coordinator:".length()).trim());
                return Boolean.TRUE;
            }
            if (lower.startsWith("missing_capability_data_node:")) {
                state.missingCapabilitiesRemoteCluster.add(line.substring("missing_capability_data_node:".length()).trim());
                return Boolean.TRUE;
            }
            return null;
        }
    }

    /**
     * A single external source declared by a {@code dataset:} preamble directive of the form
     * {@code dataset: <name>: "<resource>" [WITH {<json>}] [// comment]}. It carries everything the test
     * harness needs to either (a) register a {@code data_source}/{@code dataset} pair and run the spec's
     * {@code FROM <name>} query verbatim on dataset-capable backends, or (b) rebuild the equivalent
     * {@code EXTERNAL "<resource>" WITH {<json>}} query on backends that cannot back a dataset.
     *
     * @param name      the dataset name referenced by the {@code FROM} clause
     * @param resource  the decoded resource URI or {@code {{template}}} placeholder: surrounding quotes
     *                  removed and backslash escapes resolved (e.g. {@code \"} -&gt; {@code "})
     * @param withJson  the brace-delimited JSON options object (e.g. {@code {"header_row": false}}), or
     *                  {@code null} when the directive carries no {@code WITH} clause
     */
    public record DatasetSource(String name, String resource, String withJson) {}

    /**
     * Parses {@code dataset:} preamble directives of the form
     * {@code dataset: <name>: "<resource>" [WITH {<json>}] [// comment]}. Each declares one named external
     * source whose format options are exactly today's EXTERNAL {@code WITH} options; storage connection
     * settings are still injected by the test harness, never written in the spec. The directive is
     * repeatable so a single query can reference multiple datasets.
     * <p>
     * The resource string supports {@code \\}-escapes (so it may contain an embedded {@code "}), and a
     * trailing {@code //} comment is permitted after the resource or after the {@code WITH} object.
     * {@link SpecReader} only strips whole-line comments, so the inline comment is handled here; the
     * scanners are quote/brace aware so a {@code //} inside the resource (e.g. {@code http://...}) or
     * inside a JSON string value is never mistaken for a comment.
     * <p>
     * Like the other preamble directives ({@code Capability}, {@code Pragma}, {@code RequestStored}, ...),
     * this parser deliberately runs in both the preamble and the result phase and carries no
     * {@code state.testCase == null} guard: it accumulates into {@link ParserContext#datasetSources} (the
     * list backing the test currently being assembled), not into {@code state.testCase}. The guard on
     * {@code Warning}/{@code WarningRegex}/{@code IgnoreOrder} exists only because those write into
     * {@code state.testCase.*}; adding it here would stop the directive from firing in the preamble
     * (where {@code testCase == null}) and silently fold it into the query text.
     */
    record Dataset(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("dataset:") == false) {
                return null;
            }
            String rest = line.substring("dataset:".length()).trim();
            int colon = rest.indexOf(':');
            if (colon < 0) {
                throw new IllegalArgumentException(
                    "Invalid dataset directive [" + line + "]: expected 'dataset: <name>: \"<resource>\" [WITH {...}]'"
                );
            }
            String name = rest.substring(0, colon).trim();
            String spec = rest.substring(colon + 1).trim();
            if (name.isEmpty() || spec.startsWith("\"") == false) {
                throw new IllegalArgumentException("Invalid dataset directive [" + line + "]: a name and a quoted resource are required");
            }
            StringBuilder decoded = new StringBuilder();
            int closeQuote = scanQuoted(spec, 1, decoded);
            if (closeQuote < 0) {
                throw new IllegalArgumentException("Invalid dataset directive [" + line + "]: unterminated resource string");
            }
            String resource = decoded.toString();
            String remainder = spec.substring(closeQuote + 1).trim();
            String withJson = null;
            if (remainder.isEmpty() == false && isLineComment(remainder) == false) {
                if (remainder.toLowerCase(Locale.ROOT).startsWith("with") == false) {
                    throw new IllegalArgumentException(
                        "Invalid dataset directive ["
                            + line
                            + "]: expected WITH or a // comment after the resource, got ["
                            + remainder
                            + "]"
                    );
                }
                String afterWith = remainder.substring("with".length()).trim();
                if (afterWith.startsWith("{") == false) {
                    throw new IllegalArgumentException(
                        "Invalid dataset directive [" + line + "]: WITH must be followed by a JSON object, got [" + afterWith + "]"
                    );
                }
                int closeBrace = matchingBrace(afterWith, 0);
                if (closeBrace < 0) {
                    throw new IllegalArgumentException(
                        "Invalid dataset directive [" + line + "]: unterminated WITH JSON object, got [" + afterWith + "]"
                    );
                }
                withJson = afterWith.substring(0, closeBrace + 1);
                String tail = afterWith.substring(closeBrace + 1).trim();
                if (tail.isEmpty() == false && isLineComment(tail) == false) {
                    throw new IllegalArgumentException(
                        "Invalid dataset directive ["
                            + line
                            + "]: unexpected trailing token after WITH JSON object: ["
                            + tail
                            + "]; inline comments must start with //"
                    );
                }
            }
            state.datasetSources.add(new DatasetSource(name, resource, withJson));
            return Boolean.TRUE;
        }
    }

    /**
     * Scans a double-quoted string starting at {@code from} (the index just past the opening quote),
     * decoding backslash escapes ({@code \"} -&gt; {@code "}, {@code \\} -&gt; {@code \}; any other
     * {@code \x} -&gt; {@code x}) into {@code out}. Returns the index of the closing quote, or {@code -1}
     * if the string is unterminated. Mirrors the escape handling in
     * {@code AbstractExternalSourceSpecTestCase.findClosingBrace}.
     */
    private static int scanQuoted(String s, int from, StringBuilder out) {
        for (int i = from; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                if (i + 1 >= s.length()) {
                    return -1;
                }
                out.append(s.charAt(i + 1));
                i++;
            } else if (c == '"') {
                return i;
            } else {
                out.append(c);
            }
        }
        return -1;
    }

    /**
     * Returns the index of the closing brace matching the opening brace at {@code open}, skipping over
     * quoted strings (and their backslash escapes) so braces inside JSON string values are ignored, or
     * {@code -1} if no matching brace is found.
     */
    private static int matchingBrace(String s, int open) {
        int depth = 0;
        boolean inQuotes = false;
        for (int i = open; i < s.length(); i++) {
            char c = s.charAt(i);
            if (inQuotes) {
                if (c == '\\') {
                    i++;
                } else if (c == '"') {
                    inQuotes = false;
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    /** Whether {@code s} begins a {@code //} line comment (the inline-comment marker for spec directives). */
    private static boolean isLineComment(String s) {
        return s.startsWith("//");
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
                state.requestTimeRangeGte = value.substring(0, comma).trim();
                state.requestTimeRangeLte = value.substring(comma + 1).trim();
                if (state.requestTimeRangeGte.isEmpty() || state.requestTimeRangeLte.isEmpty()) {
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

    record DocumentsFound(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("documents_found:")) {
                state.testCase.expectedDocumentsFound = line.substring("documents_found:".length()).trim();
                return Boolean.TRUE;
            }
            return null;
        }
    }

    record Pragma(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("pragma:")) {
                String pragma = lower.substring("pragma:".length()).trim();
                int separator = pragma.indexOf('=');
                if (separator < 0) {
                    throw new IllegalArgumentException("Invalid pragma: [" + pragma + "], it must be in the form of key=value");
                }

                String key = pragma.substring(0, separator).trim();
                String value = pragma.substring(separator + 1).trim();
                state.pragmas.put(key, value);
                return Boolean.TRUE;
            }
            return null;
        }
    }

    /**
     * Marks a test as expected to fail under the {@code keyword}-to-{@code flattened} variant
     * ({@code CsvFlattenedKeywordIT}) because it exercises a known limitation of
     * {@code field_extract()} or of an upstream grammar/engine constraint. The directive is a
     * single line of the form {@code skip_flattened_rewrite: <free-text reason>}. The variant test
     * skips the test (via {@link org.junit.AssumptionViolatedException}) and the reason surfaces
     * in the JUnit XML {@code <skipped>} element so the silence is self-explanatory in CI tooling.
     * The directive is ignored by every other test driver: it lives in the preamble of a test and
     * is recognised only by the variant that opts into it.
     */
    record SkipFlattenedRewrite(ParserContext state) implements SpecReader.Parser {
        @Override
        public Object parse(String line) {
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("skip_flattened_rewrite:")) {
                state.skipFlattenedRewrite = line.substring("skip_flattened_rewrite:".length()).trim();
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
        public String expectedDocumentsFound;
        public boolean ignoreOrder;
        /**
         * How to change the test when requesting all values be loaded from stored fields.
         */
        public WhenLoadsRequestedToStored requestStored;
        /**
         * Capabilities that must be present on all clusters.
         */
        public List<String> requiredCapabilities = List.of();
        /**
         * Capabilities that must be present on the local cluster.
         * (equivalent to {@link CsvTestCase#requiredCapabilities} for single-cluster tests)
         */
        public List<String> requiredCapabilitiesLocalCluster = List.of();
        /**
         * Capabilities that must be missing on the local (coordinating) cluster.
         * (not supported for single-cluster tests)
         */
        public List<String> missingCapabilitiesLocalCluster = List.of();
        /**
         * Capabilities that must be missing on the remote cluster.
         * (not supported for single-cluster tests)
         */
        public List<String> missingCapabilitiesRemoteCluster = List.of();
        /**
         * External sources declared via {@code dataset:} preamble directives, in declaration order.
         * Empty for the vast majority of tests. When non-empty the query is expected to read these
         * via {@code FROM <name>}; the test harness registers the datasets (dataset-capable backends)
         * or rebuilds the equivalent {@code EXTERNAL} query (other backends).
         */
        public List<DatasetSource> datasetSources = List.of();
        /**
         * When set from a {@code timestamp_bounds:} line in the expected-results section, the REST request includes
         * a Query DSL range on {@code @timestamp} with these bounds (inclusive).
         */
        public String requestTimeRangeGte;
        public String requestTimeRangeLte;
        /**
         * Free-text reason carried over from a {@code skip_flattened_rewrite:} preamble line, or
         * {@code null} when the test has no such directive. Consumed by
         * {@code CsvFlattenedKeywordIT} to skip the test as a known limitation of
         * {@code field_extract()} or of an upstream grammar/engine constraint; every other test
         * driver ignores this field.
         */
        public String skipFlattenedRewrite;

        /**
         * Pragmas that must be sent.
         */
        public Map<String, String> pragmas = new HashMap<>();

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
