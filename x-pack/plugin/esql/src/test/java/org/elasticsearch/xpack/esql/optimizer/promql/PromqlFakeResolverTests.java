/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingInt;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;

public class PromqlFakeResolverTests extends AbstractLogicalPlanOptimizerTests {

    private final PromqlFakeResolver resolver = new PromqlFakeResolver();

    public void testSimpleQuery() {
        var attributes = extractAttributes("PROMQL step=1m foo");
        assertThat(gauges(attributes), contains("foo"));
    }

    public void testWithLabelFilter() {
        var attributes = extractAttributes("PROMQL step=1m foo{job=\"api-server\"}");
        assertThat(gauges(attributes), contains("foo"));
        assertThat(labels(attributes), contains("job"));
    }

    public void testWithRateFunction() {
        var attributes = extractAttributes("PROMQL step=1m rate(foo[5m])");
        assertThat(counters(attributes), contains("foo"));
    }

    public void testGroupingAggregate() {
        var attributes = extractAttributes("PROMQL step=1m sum by (job) (foo)");
        assertThat(gauges(attributes), contains("foo"));
        assertThat(labels(attributes), contains("job"));
    }

    private List<Attribute> extractAttributes(String query) {
        var plan = parser.parseQuery(query);
        plan = resolver.apply(plan);
        plan = analyzer.analyze(plan);
        plan = logicalOptimizer.optimize(plan);
        return plan.collect(LeafPlan.class).getFirst().output();
    }

    private List<String> gauges(List<Attribute> attributes) {
        return attributes.stream()
            .filter(Attribute::isMetric)
            .filter(attribute -> attribute.dataType().isNumeric())
            .map(Attribute::name)
            .toList();
    }

    private List<String> counters(List<Attribute> attributes) {
        return attributes.stream()
            .filter(Attribute::isMetric)
            .filter(attribute -> attribute.dataType().isCounter())
            .map(Attribute::name)
            .toList();
    }

    private List<String> labels(List<Attribute> attributes) {
        return attributes.stream()
            .filter(Attribute::isDimension)
            .filter(not(a -> a.name().equals("_timeseries")))
            .map(Attribute::name)
            .toList();
    }

    public void testAnalyzeQueriesFromFile() throws Exception {
        var lineCounter = new AtomicInteger(0);
        try (Stream<String> lines = Files.lines(Path.of(getClass().getResource("/promql/test_queries.csv").getPath()))) {
            var results = lines.map(query -> query.replaceAll("\\[\\$\\w+\\]", "[1m]"))
                .map(query -> query.replaceAll("\\$(\\w+)", "$1"))
                .map(query -> query.replaceAll("\\$\\{(\\w+)\\}", "$1"))
                .map(query -> tryParse(lineCounter.incrementAndGet(), query))
                .toList();

            printSummary(results);
            printErrorGroupStats(results);
            printMissingFunctionStats(results);
        }
    }

    private static void printSummary(
        List<QueryResult> results
    ) {
        Map<String, List<QueryResult>> resultsByDashboard = results.stream().collect(groupingBy(QueryResult::dashboardId));
        int successfulDashboards = resultsByDashboard.entrySet()
            .stream()
            .filter(e -> e.getValue().stream().allMatch(QueryResult::success))
            .mapToInt(e -> 1)
            .sum();
        long successfulQueries = results.stream().filter(QueryResult::success).count();
        System.out.println("| Successful Queries | Successful Dashboards |");
        System.out.println("|-------------------:|----------------------:|");
        System.out.printf(
            Locale.ROOT,
            "| %.2f%% (%d/%d) | %.2f%% (%d/%d) |\n",
            (successfulQueries * 100.0 / results.size()),
            successfulQueries,
            results.size(),
            (successfulDashboards * 100.0 / resultsByDashboard.size()),
            successfulDashboards,
            resultsByDashboard.size()
        );
        System.out.println("\n");
    }

    private static void printErrorGroupStats(List<QueryResult> results) {
        System.out.println("| Error Group | Total | Dashboards | Only error |");
        System.out.println("|-------------|------:|-----------:|-----------:|");
        Map<String, Integer> countByGroup = results.stream()
            .flatMap(q -> q.errorGroups().stream())
            .filter(Objects::nonNull)
            .collect(groupingBy(e -> e, summingInt(e -> 1)));
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

        countByGroup.entrySet()
            .stream()
            .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()))
            .forEach(
                entry -> System.out.printf(
                    Locale.ROOT,
                    "| %s | %d | %d | %d |%n",
                    entry.getKey(),
                    entry.getValue(),
                    distinctErrorGroupsByDashboard.entrySet().stream().filter(e -> e.getValue().contains(entry.getKey())).count(),
                    onlyErrorGroupByDashboard.entrySet().stream().filter(e -> e.getValue().contains(entry.getKey())).count()
                )
            );

        System.out.println("\n");
    }

    private static void printMissingFunctionStats(List<QueryResult> results) {
        Map<String, List<String>> missingFunctionsByDashboard = results.stream()
            .collect(
                groupingBy(
                    QueryResult::dashboardId,
                    Collectors.flatMapping(
                        q -> q.errors()
                            .stream()
                            .filter(e -> e.contains("Function [") && e.contains("] does not exist"))
                            .map(e -> e.replaceAll(".*Function \\[(.*)\\] does not exist.*", "$1")),
                        toList()
                    )
                )
            )
            // filter out dashboards without missing functions
            .entrySet()
            .stream()
            .filter(e -> e.getValue().isEmpty() == false)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, Set<String>> distinctMissingFunctionsByDashboard = missingFunctionsByDashboard.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> Set.copyOf(e.getValue())));

        Map<String, Set<String>> onlyMissingFunctionByDashboard = distinctMissingFunctionsByDashboard.entrySet()
            .stream()
            .filter(e -> e.getValue().size() == 1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, Integer> totalFunctionErrors = missingFunctionsByDashboard.values()
            .stream()
            .flatMap(List::stream)
            .collect(groupingBy(e -> e, summingInt(e -> 1)));

        System.out.println("| Missing Function | Total | Dashboards | Only missing |");
        System.out.println("|------------------|------:|-----------:|-------------:|");
        totalFunctionErrors.entrySet()
            .stream()
            .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue()))
            .forEach(
                entry -> System.out.printf(
                    Locale.ROOT,
                    "| %s | %d | %d | %d |%n",
                    entry.getKey(),
                    entry.getValue(),
                    distinctMissingFunctionsByDashboard.entrySet().stream().filter(e -> e.getValue().contains(entry.getKey())).count(),
                    onlyMissingFunctionByDashboard.entrySet().stream().filter(e -> e.getValue().contains(entry.getKey())).count()
                )
            );
    }

    private QueryResult tryParse(int lineNumber, String line) {
        Optional<String> parseError = Optional.empty();
        Optional<String> analyzerError = Optional.empty();
        Optional<String> optimizerError = Optional.empty();
        String[] split = line.split(";", 2);
        String dashboardId = split[0];
        String query = split[1];
        LogicalPlan plan = null;
        try {
            plan = parser.parseQuery("PROMQL step=10s (" + query + ")");
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
            line,
            errors,
            errorGroups,
            parseError.orElse(null),
            analyzerError.orElse(null),
            optimizerError.orElse(null)
        );
    }

    private static String truncateAfter(String s, String truncateAfter) {
        int idx = s.indexOf(truncateAfter);
        if (idx != -1) {
            return s.substring(0, idx + truncateAfter.length());
        }
        return s;
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
