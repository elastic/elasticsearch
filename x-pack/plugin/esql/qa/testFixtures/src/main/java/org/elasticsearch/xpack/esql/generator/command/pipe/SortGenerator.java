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

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class SortGenerator implements CommandGenerator {

    public static final String SORT = "sort";
    public static final CommandGenerator INSTANCE = new SortGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        int n = randomIntBetween(1, previousOutput.size());
        Set<String> proj = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String col = EsqlQueryGenerator.randomSortableName(previousOutput);
            if (col == null) {
                return EMPTY_DESCRIPTION; // no sortable columns
            }
            proj.add(col);
        }
        String cmd = " | sort "
            + proj.stream()
                .map(x -> x + randomFrom("", " ASC", " DESC") + randomFrom("", " NULLS FIRST", " NULLS LAST"))
                .collect(Collectors.joining(", "));
        return new CommandDescription(SORT, this, cmd, Map.of());
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
        return CommandGenerator.expectSameColumns(previousCommands, previousColumns, columns);
    }
}
