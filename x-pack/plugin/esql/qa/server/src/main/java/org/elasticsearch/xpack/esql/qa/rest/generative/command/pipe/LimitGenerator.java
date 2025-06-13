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

public class LimitGenerator implements CommandGenerator {

    public static final String LIMIT = "limit";
    public static final CommandGenerator INSTANCE = new LimitGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        int limit = randomIntBetween(0, 15000);
        String cmd = " | limit " + limit;
        return new CommandDescription(LIMIT, this, cmd, Map.ofEntries(Map.entry(LIMIT, limit)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription commandDescription,
        List<EsqlQueryGenerator.Column> previousColumns,
        List<List<Object>> previousOutput,
        List<EsqlQueryGenerator.Column> columns,
        List<List<Object>> output
    ) {
        int limit = (int) commandDescription.context().get(LIMIT);
        boolean defaultLimit = false;
        for (CommandDescription previousCommand : previousCommands) {
            if (previousCommand.commandName().equals(LIMIT)) {
                defaultLimit = true;
            }
        }

        if (previousOutput.size() > limit && output.size() != limit || defaultLimit && previousOutput.size() < output.size()) {
            return new ValidationResult(false, "Expecting [" + limit + "] records, got [" + output.size() + "]");
        }
        return CommandGenerator.expectSameColumns(previousColumns, columns);
    }
}
