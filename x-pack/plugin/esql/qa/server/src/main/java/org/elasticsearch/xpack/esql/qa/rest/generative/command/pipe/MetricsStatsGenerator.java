/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe;

import org.elasticsearch.xpack.esql.qa.rest.generative.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.CommandGenerator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.qa.rest.generative.EsqlQueryGenerator.randomDateField;

public class MetricsStatsGenerator implements CommandGenerator {

    public static final String STATS = "stats";
    public static final CommandGenerator INSTANCE = new MetricsStatsGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        // generates stats in the form of:
        // `STATS some_aggregation(some_field) by optional_grouping_field, non_optional = bucket(time_field, 5minute)`
        // where `some_aggregation` can be a time series aggregation, or a regular aggregation
        // There is a variable number of aggregations per command

        List<EsqlQueryGenerator.Column> nonNull = previousOutput.stream()
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

        StringBuilder cmd = new StringBuilder(" | stats ");

        // TODO: increase range max to 5
        int nStats = randomIntBetween(1, 2);
        for (int i = 0; i < nStats; i++) {
            String name;
            if (randomBoolean()) {
                name = EsqlQueryGenerator.randomIdentifier();
            } else {
                name = EsqlQueryGenerator.randomName(previousOutput);
                if (name == null) {
                    name = EsqlQueryGenerator.randomIdentifier();
                }
            }
            // generate the aggregation
            String expression = randomBoolean() ? EsqlQueryGenerator.metricsAgg(nonNull) : EsqlQueryGenerator.agg(nonNull);
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
            var col = EsqlQueryGenerator.randomGroupableName(nonNull);
            if (col != null) {
                cmd.append(col + ", ");
            }
        }
        // TODO: add alternative time buckets
        cmd.append(
            (randomBoolean() ? EsqlQueryGenerator.randomIdentifier() : EsqlQueryGenerator.randomName(previousOutput))
                + " = bucket("
                + timestamp
                + ",1hour)"
        );
        return new CommandDescription(STATS, this, cmd.toString(), Map.of());
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription commandDescription,
        List<EsqlQueryGenerator.Column> previousColumns,
        List<List<Object>> previousOutput,
        List<EsqlQueryGenerator.Column> columns,
        List<List<Object>> output
    ) {
        // TODO validate columns
        return VALIDATION_OK;
    }
}
