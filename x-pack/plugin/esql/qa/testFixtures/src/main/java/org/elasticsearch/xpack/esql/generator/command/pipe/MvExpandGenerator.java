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

public class MvExpandGenerator implements CommandGenerator {

    public static final String MV_EXPAND = "mv_expand";

    public static final CommandGenerator INSTANCE = new MvExpandGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        String toExpand = EsqlQueryGenerator.randomName(previousOutput);
        if (toExpand == null) {
            return EMPTY_DESCRIPTION; // no columns to expand
        }
        String cmdString = " | mv_expand " + toExpand;
        return new CommandDescription(MV_EXPAND, this, cmdString, Map.of());
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
        if (commandDescription == EMPTY_DESCRIPTION) {
            return VALIDATION_OK;
        }
        return CommandGenerator.expectSameColumns(previousCommands, previousColumns, columns);
    }

}
