/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.List;

public class InlineStatsGenerator extends StatsGenerator {
    public static final String INLINE_STATS = "inline stats";
    public static final CommandGenerator INSTANCE = new InlineStatsGenerator();

    @Override
    public String commandName() {
        return INLINE_STATS;
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

        int prevCols = previousColumns.size();

        if (previousColumns.stream().anyMatch(x -> x.name().equals("<all-fields-projected>"))) {
            // known bug https://github.com/elastic/elasticsearch/issues/121741
            prevCols--;
        }

        if (prevCols > columns.size()) {
            return new ValidationResult(false, "Expecting at least [" + prevCols + "] columns, got [" + columns.size() + "]");
        }
        return VALIDATION_OK;
    }
}
