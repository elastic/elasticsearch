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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DropAllGenerator implements CommandGenerator {

    public static final String DROP_ALL = "drop_all";
    public static final String DROPPED_COLUMNS = "dropped_columns";

    public static final CommandGenerator INSTANCE = new DropAllGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        Set<String> droppedColumns = new HashSet<>();
        String name = EsqlQueryGenerator.randomStringField(previousOutput);
        if (name == null || name.isEmpty()) {
            return CommandGenerator.EMPTY_DESCRIPTION;
        }

        String cmdString = " | keep " + name + " | drop " + name;
        return new CommandDescription(DROP_ALL, this, cmdString, Map.ofEntries(Map.entry(DROPPED_COLUMNS, droppedColumns)));
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
        if (commandDescription == EMPTY_DESCRIPTION) {
            return VALIDATION_OK;
        }

        if (columns.size() > 0) {
            return new ValidationResult(
                false,
                "Expecting no columns, got [" + columns.stream().map(Column::name).collect(Collectors.joining(", ")) + "]"
            );
        }

        return VALIDATION_OK;
    }

}
