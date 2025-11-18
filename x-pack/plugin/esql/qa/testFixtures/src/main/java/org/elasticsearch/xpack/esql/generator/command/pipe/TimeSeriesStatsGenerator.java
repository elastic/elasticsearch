/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomDateField;

public class TimeSeriesStatsGenerator implements CommandGenerator {

    public static final String STATS = "stats";
    public static final CommandGenerator INSTANCE = new TimeSeriesStatsGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        // generates stats in the form of:
        // `STATS some_aggregation(some_field) by optional_grouping_field, non_optional = bucket(time_field, 5minute)`
        // where `some_aggregation` can be a time series aggregation in the form of agg1(agg2_over_time(some_field)),
        // or a regular aggregation.
        // There is a variable number of aggregations per pipe

        List<Column> nonNull = previousOutput.stream()
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .filter(x -> x.type().equals("null") == false)
            .collect(Collectors.toList());
        if (nonNull.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }
        String timestamp = randomDateField(nonNull);
        // if there's no timestamp field left, there's nothing to bucket on
        if (timestamp == null) {
            return EMPTY_DESCRIPTION;
        }

        // TODO: Switch back to using nonNull as possible arguments for aggregations. Using the timestamp field in both the bucket as well
        // as as an argument in an aggregation causes all sorts of bizarre errors with confusing messages.
        List<Column> acceptableFields = nonNull.stream()
            .filter(c -> c.type().equals("datetime") == false && c.type().equals("date_nanos") == false)
            .filter(c -> c.name().equals("@timestamp") == false)
            .toList();

        StringBuilder cmd = new StringBuilder(" | stats ");

        // TODO: increase range max to 5
        int nStats = randomIntBetween(1, 2);
        for (int i = 0; i < nStats; i++) {
            String name;
            if (randomBoolean()) {
                name = EsqlQueryGenerator.randomIdentifier();
            } else {
                name = EsqlQueryGenerator.randomName(acceptableFields);
                if (name == null) {
                    name = EsqlQueryGenerator.randomIdentifier();
                }
            }
            // generate the aggregation
            String expression = randomBoolean()
                ? EsqlQueryGenerator.metricsAgg(acceptableFields)
                : EsqlQueryGenerator.agg(acceptableFields);
            if (i > 0) {
                cmd.append(",");
            }
            cmd.append(" ");
            cmd.append(name);
            cmd.append(" = ");
            cmd.append(expression);
        }

        cmd.append(" by ");
        if (randomBoolean()) {
            var col = EsqlQueryGenerator.randomGroupableName(acceptableFields);
            if (col != null) {
                cmd.append(col + ", ");
            }
        }
        // TODO: add alternative time buckets
        // TODO: replace name of bucket with half chance of being EsqlQueryGenerator.randomName(previousOutput) if
        // is fixed https://github.com/elastic/elasticsearch/issues/134796
        cmd.append(EsqlQueryGenerator.randomIdentifier() + " = bucket(" + timestamp + ",1hour)");
        return new CommandDescription(STATS, this, cmd.toString(), Map.of());
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription commandDescription,
        List<Column> previousColumns,
        List<List<Object>> previousOutput,
        List<Column> columns,
        List<List<Object>> output
    ) {
        // TODO validate columns
        return VALIDATION_OK;
    }
}
