/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.source;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomCounterField;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomIdentifier;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomMetricsNumericField;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomNumericField;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomNumericOrDateField;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomStringField;

public class PromQLGenerator implements CommandGenerator {

    public static final PromQLGenerator INSTANCE = new PromQLGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        return generateWithIndices(previousCommands, previousOutput, schema, executor, null);
    }

    public CommandDescription generateWithIndices(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        String index
    ) {
        List<Column> acceptableFields = previousOutput.stream()
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .filter(c -> c.type().equals("null") == false)
            .filter(c -> c.type().equals("datetime") == false && c.type().equals("date_nanos") == false)
            .filter(c -> c.name().equals("@timestamp") == false)
            .toList();
        if (index == null) {
            index = generateIndices(schema);
        }
        Tuple<String, String> sr = generateStepAndRange();
        String query = "PROMQL index=" + index + " " + generateCommand(acceptableFields, sr.v1(), sr.v2());
        return new CommandDescription("PROMQL", this, query, Map.of());
    }

    public String generateIndices(QuerySchema schema) {
        // TODO: uncomment things when wildcards (implicit casting with AMD) work https://github.com/elastic/elasticsearch/issues/141472
        int items = 1;
        // int items = randomIntBetween(1, 3);
        List<String> availableIndices = schema.baseIndices();
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < items; i++) {
            // String pattern = indexPattern(availableIndices.get(randomIntBetween(0, availableIndices.size() - 1)));
            String pattern = availableIndices.get(randomIntBetween(0, availableIndices.size() - 1));
            // if (i > 0) {
            // result.append(",");
            // }
            result.append(pattern);
        }
        return result.toString();
    }

    private Tuple<String, String> generateStepAndRange() {
        // TODO: uncomment intervals when this is resolved https://github.com/elastic/elasticsearch/issues/141305
        List<String> intervals = List.of(/* "1s", "5s", "1m", "5m", "10m", */ "30m", "1h");
        int interval = randomIntBetween(0, intervals.size() - 1);
        int secondInterval = randomIntBetween(interval, intervals.size() - 1);
        return new Tuple<>(intervals.get(interval), intervals.get(secondInterval));
    }

    private String generateCommand(List<Column> acceptableFields, String step, String timeRange) {
        // potential formats that can be generated:
        // step=1m network.total_bytes_in
        // step=1m rate(network.total_bytes_in[5m])
        // step=1m max(rate(network.total_bytes_in[5m]))
        // step=1m sum by (cluster) (rate(network.total_bytes_in[5m]))
        // step=1m sum by (pod, cluster) (rate(network.total_bytes_in[5m]))
        // step=1m sum(network.total_bytes_in)

        // sum by (pod, cluster) (SUM_OVER_TIME(network.cost[5m]))
        // [5m] is also dependent on hasInnerAgg
        boolean hasInnerAgg = randomBoolean();
        // SUM by (pod, cluster) (count_over_time(network.cost[5m]))
        boolean hasOuterAgg = randomBoolean();
        // sum BY (POD, CLUSTER) (count_over_time(network.cost[5m]))
        boolean hasGrouping = hasOuterAgg && randomBoolean();

        String resultName = randomIdentifier();
        String outerAgg = generateOuterAgg();
        String grouping = hasGrouping ? "" : generateGrouping(acceptableFields);
        String innerAgg;
        String fieldName;
        String fieldLabels = generateLabels();

        // generate fieldName and innerAgg and modify the above booleans to avoid certain invalid queries
        // TODO: add support for more inner aggregations https://github.com/elastic/elasticsearch/issues/141412
        switch (randomIntBetween(0, 3)) {
            case 0 -> {
                // input can be gauges (including aggregate_metric_double)
                List<String> gaugeAggs = List.of("max_over_time", "min_over_time", "sum_over_time", "count_over_time", "avg_over_time");
                innerAgg = gaugeAggs.get(randomIntBetween(0, gaugeAggs.size() - 1));
                fieldName = randomMetricsNumericField(acceptableFields);

                // TODO: remove when fixed https://github.com/elastic/elasticsearch/issues/141010
                if (acceptableFields.stream().anyMatch(c -> c.name().equals(fieldName) && c.type().equals("aggregate_metric_double"))) {
                    hasInnerAgg = true;
                }
            }
            case 1 -> {
                // input can be a counter
                List<String> counterAggs = List.of("increase", "irate", "rate", "first_over_time", "last_over_time");
                innerAgg = counterAggs.get(randomIntBetween(0, counterAggs.size() - 1));
                fieldName = randomCounterField(acceptableFields);
            }
            case 2 -> {
                // input can be gauges (excluding aggregate_metric_double)
                // TODO: should be combined with case 0 when fixed https://github.com/elastic/elasticsearch/issues/141010
                List<String> gaugeAggs = List.of(
                    "idelta",
                    "delta",
                    "deriv",
                    "first_over_time",
                    "last_over_time",
                    "absent_over_time",
                    "present_over_time"
                );
                fieldName = randomNumericField(acceptableFields);
                innerAgg = gaugeAggs.get(randomIntBetween(0, gaugeAggs.size() - 1));
            }
            default -> {
                fieldName = randomBoolean() ? randomStringField(acceptableFields) : randomNumericOrDateField(acceptableFields);
                List<String> innerAggs = List.of("count_over_time", "absent_over_time", "present_over_time");
                innerAgg = innerAggs.get(randomIntBetween(0, innerAggs.size() - 1));
                hasInnerAgg = true;
            }
        }
        ;
        if (hasInnerAgg && List.of("absent_over_time", "present_over_time").contains(innerAgg)) {
            hasOuterAgg = hasOuterAgg && outerAgg.equals("sum") == false && outerAgg.equals("avg") == false;
        }

        if (hasOuterAgg) {
            if (hasInnerAgg) {
                // step=1m asdf=(sum by (pod, cluster) (rate(network.total_bytes_in{pod="one"}[5m])))
                // step=1m asdf=(sum (rate(network.total_bytes_in{pod="one"}[5m])))
                return String.format(
                    "step=%s %s=(%s %s (%s(%s%s[%s])))",
                    step,
                    resultName,
                    outerAgg,
                    grouping,
                    innerAgg,
                    fieldName,
                    fieldLabels,
                    timeRange
                );
            }
            // step=1m asdf=(sum by (pod) (network.total_bytes_in{pod="one"}))
            // step=1m asdf=(sum (network.total_bytes_in{pod="one"}))
            return String.format("step=%s %s=(%s %s (%s%s))", step, resultName, outerAgg, grouping, fieldName, fieldLabels);
        }

        if (hasInnerAgg) {
            // step=1m asdf=(rate(network.total_bytes_in{pod="one"}[5m]))
            return String.format("step=%s %s=(%s(%s%s[%s]))", step, resultName, innerAgg, fieldName, fieldLabels, timeRange);
        }

        // step=1m asdf=(network.total_bytes_in{pod="one"})
        return String.format("step=%s %s=(%s%s)", step, resultName, fieldName, fieldLabels);
    }

    private String generateOuterAgg() {
        // TODO: add support for more outer aggregations https://github.com/elastic/elasticsearch/issues/141412
        List<String> outerAggs = List.of("max", "min", "sum", "count", "avg");
        return outerAggs.get(randomIntBetween(0, outerAggs.size() - 1));
    }

    private String generateGrouping(List<Column> acceptableFields) {
        // hardcoded dimensions
        List<String> dimensions = new ArrayList<>();
        dimensions.add("cluster");
        dimensions.add("pod");
        if (acceptableFields.stream().anyMatch(field -> field.name().equals("region"))) {
            dimensions.add("region");
        }
        Collections.shuffle(dimensions);
        int count = randomIntBetween(1, dimensions.size());
        StringBuilder grouping = new StringBuilder();
        grouping.append("by (");
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                grouping.append(",");
            }
            grouping.append(dimensions.get(i));
        }
        grouping.append(")");
        return grouping.toString();
    }

    private String generateLabels() {
        // potential label patterns that can be generated:
        // {cluster="staging"}
        // {cluster="staging",pod="two"}
        // {cluster!="staging"}
        // Note: fields and their values are hardcoded

        int count = randomIntBetween(1, 2);
        if (count == 0) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        result.append("{");
        Map<String, List<String>> dimensions = Map.of("cluster", List.of("staging", "prod", "qa"), "pod", List.of("one", "two", "three"));
        List<Map.Entry<String, List<String>>> entryList = new ArrayList<>(dimensions.entrySet());
        Collections.shuffle(entryList, new Random());
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                result.append(",");
            }
            var current = entryList.get(i);
            result.append(current.getKey());
            if (randomBoolean()) {
                result.append("!");
            }
            result.append("=\"");
            result.append(current.getValue().get(randomIntBetween(0, current.getValue().size() - 1)));
            result.append("\"");
        }

        result.append("}");
        return result.toString();
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription command,
        List<Column> previousColumns,
        List<List<Object>> previousOutput,
        List<Column> columns,
        List<List<Object>> output
    ) {
        return VALIDATION_OK;
    }
}
