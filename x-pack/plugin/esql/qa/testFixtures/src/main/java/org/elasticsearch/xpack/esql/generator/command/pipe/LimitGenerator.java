/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class LimitGenerator implements CommandGenerator {

    public static final String LIMIT = "limit";
    public static final CommandGenerator INSTANCE = new LimitGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        int limit = randomIntBetween(0, 2000);
        String cmd = " | limit " + limit;
        return new CommandDescription(LIMIT, this, cmd, Map.ofEntries(Map.entry(LIMIT, limit)));
    }

    @Override
    @SuppressWarnings("unchecked")
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription commandDescription,
        List<Column> previousColumns,
        List<List<Object>> previousOutput,
        List<Column> columns,
        List<List<Object>> output
    ) {
        int limit = (int) commandDescription.context().get(LIMIT);

        if (output.size() > limit) {
            return new ValidationResult(false, "Expecting at most [" + limit + "] records, got [" + output.size() + "]");
        }
        return CommandGenerator.expectSameColumns(previousCommands, previousColumns, columns);
    }
}
