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

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.StatsGenerator.addStatsAggregations;

public class TopNStatsGenerator implements CommandGenerator {

    public static final String TOP_N_STATS = "top_n_stats";
    public static final CommandGenerator INSTANCE = new TopNStatsGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        List<EsqlQueryGenerator.Column> nonNull = previousOutput.stream()
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .filter(x -> x.type().equals("null") == false)
            .collect(Collectors.toList());
        if (nonNull.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }
        StringBuilder cmd = new StringBuilder(" | stats ");
        addStatsAggregations(previousOutput, nonNull, cmd);

        var col = EsqlQueryGenerator.randomGroupableName(nonNull);
        if (col == null) {
            // Either a TopN + Stats with groupings, or nothing
            return EMPTY_DESCRIPTION;
        }
        cmd.append(" by " + col);
        cmd.append(" | sort " + col + " " + randomFrom("", " ASC", " DESC"));
        cmd.append(" | limit " + randomIntBetween(1, 1000));

        return new CommandDescription(TOP_N_STATS, this, cmd.toString(), Map.of());
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
