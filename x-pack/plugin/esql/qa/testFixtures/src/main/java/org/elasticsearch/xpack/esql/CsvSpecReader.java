/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlBaseLexer;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

public final class CsvSpecReader {

    private static final Logger logger = LogManager.getLogger(CsvSpecReader.class);

    /**
     * Parser used solely for the ordering pre-check. Initialised once; set to {@code null} if
     * construction fails so that the check degrades gracefully rather than blocking test loading.
     */
    private static final EsqlParser SPEC_PARSER;
    static {
        EsqlParser p;
        try {
            p = new EsqlParser(new EsqlConfig(new EsqlFunctionRegistry()));
        } catch (Exception e) {
            p = null;
        }
        SPEC_PARSER = p;
    }

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
                validateOrdering(result);
                validateSortDeterminesOrder(result);
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

    /**
     * Checks whether a csv-spec test case with multiple expected rows declares a top-level {@code SORT}.
     * <p>
     * Without an explicit {@code SORT} the row ordering of a multi-row result is undefined and the test
     * is inherently flaky: it may pass consistently on one segment layout and fail on another. The check
     * fires at spec-load time so violations are reported before any test method executes.
     * <p>
     * The check is skipped when ordering is provably irrelevant:
     * <ul>
     *   <li>The test declares {@code ignoreOrder: true}.</li>
     *   <li>The query has no {@code FROM} source (pure {@code ROW}/{@code EVAL} — always deterministic).</li>
     *   <li>All expected data rows are identical strings (any permutation produces the same result).</li>
     * </ul>
     * By default the violation is logged at {@code WARN} level. Set the system property
     * {@code tests.csv.strict.ordering=true} to turn violations into hard failures, which is useful for
     * enforcing the policy on individual modules once their existing violations are cleaned up.
     */
    private static void validateOrdering(CsvTestCase testCase) {
        if (testCase.ignoreOrder) {
            return;
        }
        // Collect data rows: first non-blank non-directive non-comment line is the header; the rest are data.
        List<String> dataRowLines = new ArrayList<>();
        boolean headerSeen = false;
        for (String line : testCase.expectedResults.split("\\r?\\n")) {
            // Mirror SpecReader.shouldSkipLine: blank, // comments, and # comments are not data.
            if (line.isBlank() || line.startsWith("//") || line.startsWith("#")) {
                continue;
            }
            String lower = line.toLowerCase(Locale.ROOT);
            if (lower.startsWith("warning")
                || lower.startsWith("ignoreorder")
                || lower.startsWith("documents_found")
                || lower.startsWith("warningregex")) {
                continue;
            }
            if (headerSeen == false) {
                headerSeen = true;
            } else {
                dataRowLines.add(line);
            }
        }
        if (dataRowLines.size() < 2) {
            return;
        }
        // If all data rows are identical the result set is order-independent: any permutation matches.
        Set<String> distinct = new HashSet<>(dataRowLines);
        if (distinct.size() == 1) {
            return;
        }
        // Pure ROW/EVAL queries (no FROM source) produce deterministic ordering; no SORT needed.
        if (hasFromSource(testCase.query) == false) {
            return;
        }
        if (hasTopLevelSort(testCase.query) == false) {
            String message = "Query has "
                + dataRowLines.size()
                + " expected rows but no top-level SORT. "
                + "Add `| SORT <stable_field>` to the query or set `ignoreOrder: true` in the spec.\n"
                + "Query: "
                + testCase.query;
            if (Boolean.getBoolean("tests.csv.strict.ordering")) {
                throw new IllegalArgumentException(message);
            }
            logger.warn(message);
        }
    }

    /**
     * Parses {@code query} into an ANTLR {@link EsqlBaseParser.QueryContext} without going through
     * {@link EsqlParser} or the AST builder. Bypassing the AST builder avoids side effects such as
     * {@link org.elasticsearch.common.logging.HeaderWarning#addWarning} calls that the builder
     * emits for deprecated syntax (e.g. the old one-word {@code INLINESTATS} keyword).
     *
     * @return the top-level query context, or {@code null} if lexing or parsing fails
     */
    private static EsqlBaseParser.QueryContext antlrParse(String query) {
        try {
            EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(query));
            lexer.removeErrorListeners();
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            EsqlBaseParser parser = new EsqlBaseParser(tokenStream);
            parser.removeErrorListeners();
            return parser.singleStatement().query();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Returns {@code true} when the query's source command reads from an external index
     * ({@code FROM}, {@code METRICS}/{@code TS}, {@code PROMQL}, or {@code EXTERNAL}).
     * Pure {@code ROW}/{@code SHOW} pipelines produce deterministic output and never need a
     * {@code SORT} for stable test results.
     * <p>
     * Uses the raw ANTLR parse tree rather than {@link EsqlParser} to avoid triggering
     * {@link org.elasticsearch.common.logging.HeaderWarning} side effects that the AST builder
     * may emit for deprecated syntax (e.g. the old one-word {@code INLINESTATS} keyword).
     * Returns {@code true} on parse failure so that a bad query is not double-reported here.
     */
    private static boolean hasFromSource(String query) {
        try {
            EsqlBaseParser.QueryContext qCtx = antlrParse(query);
            if (qCtx == null) {
                return true;
            }
            while (qCtx instanceof EsqlBaseParser.CompositeQueryContext composite) {
                qCtx = composite.query();
            }
            if (qCtx instanceof EsqlBaseParser.SingleCommandQueryContext single) {
                EsqlBaseParser.SourceCommandContext src = single.sourceCommand();
                return src.fromCommand() != null
                    || src.timeSeriesCommand() != null
                    || src.promqlCommand() != null
                    || src.externalCommand() != null;
            }
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * Returns {@code true} when the query has a deterministic top-level sort. Returns {@code true}
     * on parse failure so that a bad query is not double-reported here and by the test runner.
     * <p>
     * The check peels through {@code LIMIT} (truncating but order-preserving for the top-N rows) and
     * any sort-preserving processing command ({@code KEEP}, {@code DROP}, {@code RENAME},
     * {@code EVAL}, {@code WHERE}, {@code ENRICH}, regex-extract commands, etc.) before looking for
     * a {@code SORT}. Commands that disrupt the established sort order ({@code STATS},
     * {@code MV_EXPAND}, etc.) stop the search and return {@code false}.
     * <p>
     * Uses the raw ANTLR parse tree rather than {@link EsqlParser} to avoid triggering
     * {@link org.elasticsearch.common.logging.HeaderWarning} side effects.
     */
    private static boolean hasTopLevelSort(String query) {
        try {
            EsqlBaseParser.QueryContext qCtx = antlrParse(query);
            if (qCtx == null) {
                return true;
            }
            while (qCtx instanceof EsqlBaseParser.CompositeQueryContext composite) {
                EsqlBaseParser.ProcessingCommandContext cmd = composite.processingCommand();
                if (cmd.sortCommand() != null) {
                    return true;
                }
                if (isSortDisruptingCommand(cmd)) {
                    return false;
                }
                // Sort-preserving command: peel through to the inner query.
                qCtx = composite.query();
            }
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * Returns {@code true} when the processing command disrupts any sort order established by inner
     * commands, meaning the search for a top-level {@code SORT} should stop here.
     * Commands not listed here are treated as sort-preserving (the conservative assumption).
     */
    private static boolean isSortDisruptingCommand(EsqlBaseParser.ProcessingCommandContext cmd) {
        return cmd.statsCommand() != null
            || cmd.mvExpandCommand() != null
            || cmd.forkCommand() != null
            || cmd.sampleCommand() != null
            || cmd.fuseCommand() != null
            || cmd.dedupCommand() != null
            || cmd.mmrCommand() != null
            || cmd.tsCollapseCommand() != null
            || cmd.highlightCommand() != null
            || cmd.metricsInfoCommand() != null
            || cmd.tsInfoCommand() != null;
    }

    /**
     * Checks that the top-level {@code SORT} keys are sufficient to uniquely determine the order of
     * every row in the expected output.
     * <p>
     * When two adjacent expected rows have identical values for <em>all</em> sort key columns that
     * appear in the expected output header, their relative order is non-deterministic across runs —
     * the sort does not fully break the tie. This produces the same kind of flakiness as a missing
     * {@code SORT}, but is not caught by {@link #validateOrdering} because a {@code SORT} is present.
     * <p>
     * The check is skipped conservatively when:
     * <ul>
     *   <li>Any sort key is a complex expression (not a plain column reference) — ties cannot be
     *       determined without evaluating the expression against actual data.</li>
     *   <li>Any sort key column is absent from the expected output — the values are unknown.</li>
     *   <li>Any cell value is a wildcard or range pattern (e.g. {@code {any}}, {@code 1..5}) —
     *       the cell may match multiple values.</li>
     * </ul>
     */
    private static void validateSortDeterminesOrder(CsvTestCase testCase) {
        if (testCase.ignoreOrder || SPEC_PARSER == null) {
            return;
        }
        if (hasFromSource(testCase.query) == false) {
            return;
        }
        // Skip the full parse when there is no top-level sort: validateOrdering() already warned,
        // and parsing here would trigger HeaderWarning side effects for deprecated syntax.
        if (hasTopLevelSort(testCase.query) == false) {
            return;
        }
        try {
            LogicalPlan plan = SPEC_PARSER.parseQuery(testCase.query);

            // Walk to the top-level OrderBy, skipping Limit and SortPreserving wrappers.
            while (plan instanceof Limit || plan instanceof SortPreserving || plan instanceof Rename) {
                plan = ((UnaryPlan) plan).child();
            }
            if (plan instanceof OrderBy == false) {
                return;
            }
            OrderBy orderBy = (OrderBy) plan;

            // Collect sort key names; bail if any key is a complex expression.
            List<String> sortKeyNames = new ArrayList<>();
            for (Order order : orderBy.order()) {
                if (order.child() instanceof UnresolvedAttribute attr) {
                    sortKeyNames.add(attr.name().toLowerCase(Locale.ROOT));
                } else {
                    return;
                }
            }
            if (sortKeyNames.isEmpty()) {
                return;
            }

            // Parse expected output into rows (header first, then data rows).
            List<String[]> rows = new ArrayList<>();
            boolean headerSeen = false;
            for (String line : testCase.expectedResults.split("\\r?\\n")) {
                if (line.isBlank() || line.startsWith("//") || line.startsWith("#")) {
                    continue;
                }
                String lower = line.toLowerCase(Locale.ROOT);
                if (lower.startsWith("warning")
                    || lower.startsWith("ignoreorder")
                    || lower.startsWith("documents_found")
                    || lower.startsWith("warningregex")) {
                    continue;
                }
                String[] cells = line.split("\\|");
                for (int i = 0; i < cells.length; i++) {
                    cells[i] = cells[i].trim();
                }
                if (headerSeen == false) {
                    headerSeen = true;
                    rows.add(cells);
                } else {
                    rows.add(cells);
                }
            }
            if (rows.size() < 3) {
                // Need at least header + 2 data rows for a meaningful tie check.
                return;
            }

            // Map each sort key name to its column index in the header.
            String[] header = rows.get(0);
            int[] sortKeyIndices = new int[sortKeyNames.size()];
            Arrays.fill(sortKeyIndices, -1);
            for (int col = 0; col < header.length; col++) {
                // Header cells are of the form "name:type"; strip the type.
                String colName = header[col].contains(":") ? header[col].substring(0, header[col].indexOf(':')).trim() : header[col].trim();
                for (int k = 0; k < sortKeyNames.size(); k++) {
                    if (colName.toLowerCase(Locale.ROOT).equals(sortKeyNames.get(k))) {
                        sortKeyIndices[k] = col;
                    }
                }
            }
            // If any sort key is missing from the expected output we cannot check ties.
            for (int idx : sortKeyIndices) {
                if (idx < 0) {
                    return;
                }
            }

            // Check each pair of adjacent data rows for ties on all sort key columns.
            List<String[]> dataRows = rows.subList(1, rows.size());
            // If every data row is identical across all columns, any permutation of
            // sort-key-tied rows produces the same output — not a flakiness risk.
            String[] firstDataRow = dataRows.get(0);
            boolean allRowsIdentical = true;
            for (int i = 1; i < dataRows.size(); i++) {
                if (Arrays.equals(firstDataRow, dataRows.get(i)) == false) {
                    allRowsIdentical = false;
                    break;
                }
            }
            if (allRowsIdentical) {
                return;
            }
            for (int i = 0; i < dataRows.size() - 1; i++) {
                String[] rowA = dataRows.get(i);
                String[] rowB = dataRows.get(i + 1);
                boolean tied = true;
                for (int keyIdx : sortKeyIndices) {
                    if (keyIdx >= rowA.length || keyIdx >= rowB.length) {
                        tied = false;
                        break;
                    }
                    String a = rowA[keyIdx];
                    String b = rowB[keyIdx];
                    // Skip wildcard / range cells — we cannot determine the concrete value.
                    if (a.startsWith("{") || b.startsWith("{") || a.contains("..") || b.contains("..")) {
                        tied = false;
                        break;
                    }
                    if (a.equals(b) == false) {
                        tied = false;
                        break;
                    }
                }
                if (tied) {
                    // If the rows are completely identical across all columns, swapping them
                    // produces the same test output — not a real flakiness risk.
                    if (Arrays.equals(rowA, rowB)) {
                        continue;
                    }
                    String message = "Rows "
                        + (i + 1)
                        + " and "
                        + (i + 2)
                        + " have the same value(s) for sort key(s) "
                        + sortKeyNames
                        + " — their relative order is non-deterministic. "
                        + "Add a tiebreaker to the SORT clause.\n"
                        + "Query: "
                        + testCase.query;
                    if (Boolean.getBoolean("tests.csv.strict.ordering")) {
                        throw new IllegalArgumentException(message);
                    }
                    logger.warn(message);
                }
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            // parse failure — skip the check
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
