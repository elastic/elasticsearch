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

public class StatsGenerator implements CommandGenerator {

    public static final String STATS = "stats";
    public static final CommandGenerator INSTANCE = new StatsGenerator();

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
        int nStats = randomIntBetween(1, 5);
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
            String expression = EsqlQueryGenerator.agg(nonNull);
            if (i > 0) {
                cmd.append(",");
            }
            cmd.append(" ");
            cmd.append(name);
            cmd.append(" = ");
            cmd.append(expression);
        }
        if (randomBoolean()) {
            var col = EsqlQueryGenerator.randomGroupableName(nonNull);
            if (col != null) {
                cmd.append(" by " + col);
            }
        }
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
