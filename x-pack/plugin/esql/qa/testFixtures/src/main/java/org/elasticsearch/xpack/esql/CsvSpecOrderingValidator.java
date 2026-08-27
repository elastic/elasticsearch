/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.parser.EsqlBaseLexer;
import org.elasticsearch.xpack.esql.parser.EsqlBaseParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Static validation of csv-spec files: reports tests whose expected row order is not fully determined
 * by the query.
 * <p>
 * A csv-spec test asserts its expected output as an ordered list of rows. When the query does not pin
 * that order down, the test passes or fails depending on shard layout and executor scheduling. Such
 * tests have historically been muted rather than fixed. Two situations are reported:
 * <ol>
 *   <li>A multi-row result read from an index with no top-level {@code SORT}.</li>
 *   <li>A top-level {@code SORT} whose keys tie between two adjacent expected rows, leaving their
 *       relative order undefined.</li>
 * </ol>
 * Both are skipped when the test declares {@code ignoreOrder: true}.
 * <p>
 * Each query is parsed once, straight into an ANTLR tree. Going through {@code EsqlParser} is
 * deliberately avoided: the AST builder emits {@code HeaderWarning} side effects for deprecated
 * syntax, and it drags in server logging that is not initialised outside a test JVM.
 * <p>
 * This runs as a build-time check rather than during test execution, so the specs are parsed once per
 * build instead of once per test case in every csv-spec task. Invoked from the
 * {@code validateCsvSpecOrdering} Gradle task with the spec directories as arguments; exits non-zero
 * when any violation is found.
 */
public class CsvSpecOrderingValidator {

    /**
     * A sort key simple enough to compare against the expected output: a bare field reference,
     * possibly qualified or backtick-quoted. Anything else (a function call, arithmetic) would have to
     * be evaluated against real data, so the tie check bails out.
     */
    private static final Pattern SIMPLE_FIELD = Pattern.compile("[A-Za-z_@][A-Za-z0-9_.@]*");

    /**
     * Directive lines that may appear interleaved with the expected result rows and must not be
     * mistaken for data.
     */
    private static final Set<String> RESULT_DIRECTIVES = Set.of("warning", "warningregex", "ignoreorder", "documents_found");

    private CsvSpecOrderingValidator() {}

    public static void main(String[] args) throws Exception {
        // SpecReader pulls in EsqlTestUtils, whose logger needs the logging SPI bound first.
        LogConfigurator.configureESLogging();
        if (args.length == 0) {
            throw new IllegalArgumentException("usage: CsvSpecOrderingValidator <spec-dir> [<spec-dir> ...]");
        }

        List<String> violations = new ArrayList<>();
        int specFiles = 0;
        int testCases = 0;

        for (String dir : args) {
            for (Path spec : specFilesIn(Path.of(dir))) {
                specFiles++;
                for (Object[] parsed : SpecReader.readScriptSpec(List.of(spec.toUri().toURL()), CsvSpecReader::specParser)) {
                    // See SpecReader#makeTestCase: { fileName, groupName, testName, lineNumber, result, instructions }
                    String testName = (String) parsed[2];
                    int lineNumber = (Integer) parsed[3];
                    testCases++;
                    String violation = validate((CsvTestCase) parsed[4]);
                    if (violation != null) {
                        violations.add(spec.getFileName() + ":" + lineNumber + " [" + testName + "] " + violation);
                    }
                }
            }
        }

        System.out.println("Checked " + testCases + " test cases in " + specFiles + " csv-spec files.");
        if (violations.isEmpty() == false) {
            System.err.println();
            System.err.println("Found " + violations.size() + " csv-spec test(s) with non-deterministic row order:");
            violations.forEach(v -> System.err.println("  " + v));
            System.err.println();
            System.err.println("Add a SORT (or a tiebreaker to the existing SORT) so the order is fully determined,");
            System.err.println("or declare `ignoreOrder: true` when the order genuinely does not matter.");
            System.exit(1);
        }
    }

    private static List<Path> specFilesIn(Path dir) throws IOException {
        if (Files.isDirectory(dir) == false) {
            throw new IllegalArgumentException("not a directory: " + dir);
        }
        try (Stream<Path> files = Files.walk(dir)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".csv-spec")).sorted(Comparator.naturalOrder()).toList();
        }
    }

    /**
     * Returns a description of the ordering problem in {@code testCase}, or {@code null} when the
     * expected row order is determined, or cannot be judged.
     */
    static String validate(CsvTestCase testCase) {
        if (testCase.ignoreOrder) {
            return null;
        }
        EsqlBaseParser.QueryContext query = antlrParse(testCase.query);
        // A query we cannot parse is already reported by the test runner; do not double-report it here.
        if (query == null || hasFromSource(query) == false) {
            return null;
        }
        List<String[]> rows = expectedRows(testCase.expectedResults);
        // A header plus at least two data rows are needed before order can matter.
        if (rows.size() < 3) {
            return null;
        }
        List<String[]> dataRows = rows.subList(1, rows.size());
        // Identical rows are order-independent: any permutation produces the same expected output.
        if (allIdentical(dataRows)) {
            return null;
        }
        EsqlBaseParser.SortCommandContext sort = topLevelSort(query);
        return sort == null ? dataRows.size() + " expected rows but no top-level SORT" : tiedSortKeys(sort, rows);
    }

    /**
     * Reports adjacent expected rows that tie on every sort key, meaning the {@code SORT} does not
     * fully determine their relative order.
     * <p>
     * Returns {@code null} whenever the comparison cannot be made safely: a sort key that is not a
     * plain field, a sort key absent from the expected output, or a cell holding a wildcard or range
     * pattern whose concrete value is unknown.
     */
    private static String tiedSortKeys(EsqlBaseParser.SortCommandContext sort, List<String[]> rows) {
        List<String> sortKeys = new ArrayList<>();
        for (EsqlBaseParser.OrderExpressionContext order : sort.orderExpression()) {
            String key = order.booleanExpression().getText().replace("`", "");
            if (SIMPLE_FIELD.matcher(key).matches() == false) {
                return null;
            }
            sortKeys.add(key.toLowerCase(Locale.ROOT));
        }

        int[] keyColumns = keyColumns(rows.get(0), sortKeys);
        if (keyColumns == null) {
            return null;
        }

        List<String[]> dataRows = rows.subList(1, rows.size());
        for (int i = 0; i < dataRows.size() - 1; i++) {
            String[] a = dataRows.get(i);
            String[] b = dataRows.get(i + 1);
            // Fully identical rows may be swapped without changing the expected output.
            if (tied(a, b, keyColumns) && Arrays.equals(a, b) == false) {
                return "rows " + (i + 1) + " and " + (i + 2) + " tie on sort key(s) " + sortKeys + "; SORT needs a tiebreaker";
            }
        }
        return null;
    }

    /**
     * Maps each sort key to its column index in the expected output header, or returns {@code null}
     * when any key is absent and the tie check therefore cannot be performed.
     */
    private static int[] keyColumns(String[] header, List<String> sortKeys) {
        int[] columns = new int[sortKeys.size()];
        Arrays.fill(columns, -1);
        for (int column = 0; column < header.length; column++) {
            // Header cells are "name:type".
            int colon = header[column].indexOf(':');
            String name = (colon >= 0 ? header[column].substring(0, colon) : header[column]).trim().toLowerCase(Locale.ROOT);
            int key = sortKeys.indexOf(name);
            if (key >= 0) {
                columns[key] = column;
            }
        }
        return Arrays.stream(columns).anyMatch(c -> c < 0) ? null : columns;
    }

    private static boolean tied(String[] a, String[] b, int[] keyColumns) {
        for (int column : keyColumns) {
            if (column >= a.length || column >= b.length) {
                return false;
            }
            // Wildcard and range cells stand for values we cannot compare.
            if (a[column].startsWith("{") || b[column].startsWith("{") || a[column].contains("..") || b[column].contains("..")) {
                return false;
            }
            if (a[column].equals(b[column]) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean allIdentical(List<String[]> rows) {
        Set<String> distinct = new HashSet<>();
        for (String[] row : rows) {
            distinct.add(String.join("|", row));
        }
        return distinct.size() == 1;
    }

    /**
     * Splits the expected results into cell arrays, header first. Comment and directive lines are
     * dropped so they are not counted as data.
     */
    private static List<String[]> expectedRows(String expectedResults) {
        List<String[]> rows = new ArrayList<>();
        for (String line : expectedResults.split("\\r?\\n")) {
            if (line.isBlank() || SpecReader.shouldSkipLine(line.trim())) {
                continue;
            }
            String directive = line.toLowerCase(Locale.ROOT).split(":", 2)[0].trim();
            if (RESULT_DIRECTIVES.contains(directive)) {
                continue;
            }
            rows.add(Arrays.stream(line.split("\\|")).map(String::trim).toArray(String[]::new));
        }
        return rows;
    }

    /**
     * @return the top-level query context, or {@code null} if the query cannot be parsed
     */
    private static EsqlBaseParser.QueryContext antlrParse(String query) {
        try {
            EsqlBaseLexer lexer = new EsqlBaseLexer(CharStreams.fromString(query));
            lexer.removeErrorListeners();
            EsqlBaseParser parser = new EsqlBaseParser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            return parser.singleStatement().query();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Returns whether the query reads from an index. Pure {@code ROW}/{@code SHOW} pipelines are
     * deterministic and never need a {@code SORT}.
     */
    private static boolean hasFromSource(EsqlBaseParser.QueryContext query) {
        EsqlBaseParser.QueryContext source = query;
        while (source instanceof EsqlBaseParser.CompositeQueryContext composite) {
            source = composite.query();
        }
        if (source instanceof EsqlBaseParser.SingleCommandQueryContext single) {
            EsqlBaseParser.SourceCommandContext command = single.sourceCommand();
            return command.fromCommand() != null
                || command.timeSeriesCommand() != null
                || command.promqlCommand() != null
                || command.externalCommand() != null;
        }
        return false;
    }

    /**
     * Returns the {@code SORT} that survives to the end of the pipeline, peeling through commands that
     * preserve row order and stopping at the first one that does not, or {@code null} if there is
     * none.
     */
    private static EsqlBaseParser.SortCommandContext topLevelSort(EsqlBaseParser.QueryContext query) {
        EsqlBaseParser.QueryContext current = query;
        while (current instanceof EsqlBaseParser.CompositeQueryContext composite) {
            EsqlBaseParser.ProcessingCommandContext command = composite.processingCommand();
            if (command.sortCommand() != null) {
                return command.sortCommand();
            }
            if (isSortDisrupting(command)) {
                return null;
            }
            current = composite.query();
        }
        return null;
    }

    /**
     * Returns whether the command destroys the row order established before it. Anything not listed is
     * assumed to preserve order.
     * <p>
     * {@code LOOKUP JOIN} does not itself reorder rows, but the join's right-hand side is not part of
     * the sort walk, so it is treated as disrupting to skip the check rather than report a false
     * negative.
     */
    private static boolean isSortDisrupting(EsqlBaseParser.ProcessingCommandContext command) {
        return command.statsCommand() != null
            || command.mvExpandCommand() != null
            || command.forkCommand() != null
            || command.sampleCommand() != null
            || command.fuseCommand() != null
            || command.dedupCommand() != null
            || command.mmrCommand() != null
            || command.tsCollapseCommand() != null
            || command.highlightCommand() != null
            || command.metricsInfoCommand() != null
            || command.tsInfoCommand() != null
            || command.joinCommand() != null;
    }
}
