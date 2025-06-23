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

import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class ForkGenerator implements CommandGenerator {

    public static final String FORK = "fork";
    public static final CommandGenerator INSTANCE = new ForkGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        // FORK can only be allowed once - so we skip adding another FORK if we already have one
        // otherwise, most generated queries would only result in a validation error
        for (CommandDescription command : previousCommands) {
            if (command.commandName().equals(FORK)) {
                return new CommandDescription(FORK, this, " ", Map.of());
            }
        }

        int n = randomIntBetween(2, 8);

        String cmd = " | FORK " + "( WHERE true ) ".repeat(n) + " | WHERE _fork == \"fork" + randomIntBetween(1, n) + "\" | DROP _fork";

        return new CommandDescription(FORK, this, cmd, Map.of());
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription command,
        List<EsqlQueryGenerator.Column> previousColumns,
        List<List<Object>> previousOutput,
        List<EsqlQueryGenerator.Column> columns,
        List<List<Object>> output
    ) {
        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}
