/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.MutableAnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;

/**
 * A utility to analyze PromQL queries from a file and report parsing, analysis,
 * and optimization errors.
 * <p>
 * The input file should contain one query per line, prefixed by a dashboard ID
 * separated by a semicolon. For example:
 * <pre>
 * dashboard1;sum(rate(http_requests_total[5m]))
 * dashboard2;http_requests_total{job="api-server"}
 * </pre>
 * To run the utility, execute the following command:
 * {@code ./gradlew :x-pack:plugin:esql:analyzePromqlQueries -PqueriesFile=<path-to-query-file> -PoutputFile=<path-to-output-file>}
 */
public class PromqlCoverageAnalyzer implements Closeable {

    private final EsqlParser parser = EsqlParser.INSTANCE;
    private final PromqlFakeResolver resolver = new PromqlFakeResolver();
    private final Analyzer analyzer = new Analyzer(
        new MutableAnalyzerContext(
            EsqlTestUtils.TEST_CFG,
            new EsqlFunctionRegistry(),
            Map.of(),
            AnalyzerTestUtils.defaultLookupResolution(),
            new EnrichResolution(),
            EsqlTestUtils.emptyInferenceResolution(),
            TransportVersion.current(),
            QuerySettings.UNMAPPED_FIELDS.defaultValue()
        ),
        TEST_VERIFIER
    );

    private final LogicalPlanOptimizer logicalOptimizer = new LogicalPlanOptimizer(
        new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
    );

    private final BufferedWriter writer;

    public PromqlCoverageAnalyzer(BufferedWriter writer) {
        this.writer = writer;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args[0].isBlank()) {
            throw new IllegalArgumentException("Path to query file is required");
        }
        if (args.length < 2 || args[1].isBlank()) {
            throw new IllegalArgumentException("Path to output file is required");
        }
        Path inputFile = PathUtils.get(args[0]);
        Path outputFile = PathUtils.get(args[1]);

        LogConfigurator.configureWithoutConfig(Settings.builder().put("logger.level", Level.INFO.name()).build());
        LogConfigurator.configureESLogging();
        try (
            PromqlCoverageAnalyzer analyzer = new PromqlCoverageAnalyzer(
                Files.newBufferedWriter(outputFile, TRUNCATE_EXISTING, CREATE, WRITE)
            )
        ) {

            var lineCounter = new AtomicInteger(0);
            try (Stream<String> lines = Files.lines(inputFile)) {
                var results = lines.map(query -> analyzer.tryParse(lineCounter.incrementAndGet(), query)).toList();
                analyzer.writeSummary(results);
                analyzer.writeErrorGroupStats(results);
            }
        }
    }

    private QueryResult tryParse(int lineNumber, String line) {
        Optional<String> parseError = Optional.empty();
        Optional<String> analyzerError = Optional.empty();
        Optional<String> optimizerError = Optional.empty();
        String[] split = line.split(";", 2);
        String dashboardId = split[0];
        String query = split[1];
        // replace control variables with concrete values
        String adjustedQuery = query.replaceAll("\\[\\$\\w+\\]", "[1m]").replaceAll("\\$(\\w+)", "$1").replaceAll("\\$\\{(\\w+)\\}", "$1");
        LogicalPlan plan = null;
        try {
            plan = parser.parseQuery("PROMQL step=10s (" + adjustedQuery + ")");
            try {
                plan = analyzer.analyze(resolver.apply(plan));
                try {
                    plan = logicalOptimizer.optimize(plan);
                } catch (Exception e) {
                    optimizerError = Optional.of(e.getMessage());
                }
            } catch (Exception e) {
                analyzerError = Optional.of(e.getMessage());
            }
        } catch (Exception e) {
            parseError = Optional.of(e.getMessage());
        }
        List<String> errors = Stream.of(parseError, analyzerError, optimizerError)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMap(s -> Stream.of(s.split("\nline \\d+:\\d+: ")))
            .filter(not(String::isBlank))
            .filter(not(e -> e.startsWith("Found") && e.contains("problem")))
            .toList();
        List<String> errorGroups = errors.stream()
            .map(e -> truncateAfter(e, "expecting {"))
            .map(e -> truncateAfter(e, "no viable alternative at input"))
            .map(e -> truncateAfter(e, "Invalid call to dataType on an unresolved object"))
            .map(e -> truncateAfter(e, "Cannot parse regex"))
            .map(e -> truncateAfter(e, "mismatched input"))
            .map(e -> e.contains("optimized incorrectly due to missing references") ? "optimized incorrectly due to missing references" : e)
            // avoid lumping all missing function errors into one group
            .map(
                e -> e.contains("Function [") && e.contains("] does not exist")
                    ? e.replaceAll(".*Function \\[(.*)\\] does not exist.*", "Function $1 does not exist")
                    : e
            )
            .map(e -> e.replaceAll("line \\d+:\\d+: ", ""))
            .map(e -> e.replaceAll("\\[.*\\]", "[...]"))
            .map(e -> e.replaceAll("\\d+", "N"))
            .toList();

        /*if (errorGroups.stream().anyMatch(s -> s.contains("no viable alternative at input"))) {
            System.out.println(query);
        }*/

        return new QueryResult(
            lineNumber,
            Stream.of(parseError, analyzerError, optimizerError).allMatch(Optional::isEmpty),
            dashboardId,
            query,
            errors,
            errorGroups,
            parseError.orElse(null),
            analyzerError.orElse(null),
            optimizerError.orElse(null)
        );
    }

    private void writeSummary(List<QueryResult> results) {
        Map<String, List<QueryResult>> resultsByDashboard = results.stream().collect(groupingBy(QueryResult::dashboardId));
        int successfulDashboards = resultsByDashboard.entrySet()
            .stream()
            .filter(e -> e.getValue().stream().allMatch(QueryResult::success))
            .mapToInt(e -> 1)
            .sum();
        long successfulQueries = results.stream().filter(QueryResult::success).count();
        writeLine("| Successful Queries | Successful Dashboards |");
        writeLine("|-------------------:|----------------------:|");
        writeLine(
            String.format(
                Locale.ROOT,
                "| %.2f%% (%d/%d) | %.2f%% (%d/%d) |",
                (successfulQueries * 100.0 / results.size()),
                successfulQueries,
                results.size(),
                (successfulDashboards * 100.0 / resultsByDashboard.size()),
                successfulDashboards,
                resultsByDashboard.size()
            )
        );
        writeLine("");
    }

    private void writeErrorGroupStats(List<QueryResult> results) {
        Map<String, Integer> countByGroup = results.stream()
            .flatMap(q -> q.errorGroups().stream())
            .filter(Objects::nonNull)
            .collect(groupingBy(e -> e, summingInt(e -> 1)));
        Map<String, List<String>> queriesByErrorGroup = results.stream()
            .filter(not(QueryResult::success))
            .flatMap(r -> r.errorGroups().stream().map(g -> Map.entry(g, r.query())))
            .collect(groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, toList())));
        Map<String, List<String>> errorGroupsByDashboard = results.stream()
            .collect(
                groupingBy(
                    QueryResult::dashboardId,
                    Collectors.flatMapping(q -> q.errorGroups().stream().filter(Objects::nonNull), toList())
                )
            );
        Map<String, Set<String>> distinctErrorGroupsByDashboard = errorGroupsByDashboard.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> Set.copyOf(e.getValue())));
        Map<String, Set<String>> onlyErrorGroupByDashboard = distinctErrorGroupsByDashboard.entrySet()
            .stream()
            .filter(e -> e.getValue().size() == 1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        long totalDashboards = results.stream().map(QueryResult::dashboardId).distinct().count();

        writeLine("| Error Group | Total | Dashboards | Only error | Example Query |");
        writeLine("|-------------|------:|-----------:|-----------:|---------------|");
        countByGroup.entrySet().stream().sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue())).forEach(entry -> {
            String errorGroup = entry.getKey();
            int groupCountTotal = entry.getValue();
            long dashboardCount = distinctErrorGroupsByDashboard.entrySet().stream().filter(e -> e.getValue().contains(errorGroup)).count();
            long onlyErrorCount = onlyErrorGroupByDashboard.entrySet().stream().filter(e -> e.getValue().contains(errorGroup)).count();
            String shortestQueryForGroup = queriesByErrorGroup.get(errorGroup)
                .stream()
                .min(Comparator.comparingInt(String::length))
                .orElse("");
            writeLine(
                String.format(
                    Locale.ROOT,
                    "| %s | %.2f%% (%d) | %.2f%% (%d) | %.2f%% (%d) | `%s` |",
                    errorGroup,
                    groupCountTotal * 100.0 / results.size(),
                    groupCountTotal,
                    dashboardCount * 100.0 / totalDashboards,
                    dashboardCount,
                    onlyErrorCount * 100.0 / totalDashboards,
                    onlyErrorCount,
                    shortestQueryForGroup.replace("|", "\\|")
                )
            );
        });

        writeLine("");
    }

    private String truncateAfter(String s, String truncateAfter) {
        int idx = s.indexOf(truncateAfter);
        if (idx != -1) {
            return s.substring(0, idx + truncateAfter.length());
        }
        return s;
    }

    private void writeLine(String line) {
        try {
            writer.write(line);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write line: " + line, e);
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    record QueryResult(
        int lineNumber,
        boolean success,
        String dashboardId,
        String query,
        List<String> errors,
        List<String> errorGroups,
        String parseError,
        String analyzerError,
        String optimizerError
    ) {}
}
