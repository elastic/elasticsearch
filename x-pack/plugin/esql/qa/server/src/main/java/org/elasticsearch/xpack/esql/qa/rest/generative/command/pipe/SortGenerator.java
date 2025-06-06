/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe;

import org.elasticsearch.xpack.esql.qa.rest.generative.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.CommandGenerator;

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
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
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
        List<EsqlQueryGenerator.Column> previousColumns,
        List<List<Object>> previousOutput,
        List<EsqlQueryGenerator.Column> columns,
        List<List<Object>> output
    ) {
        if (previousOutput.size() != output.size()) {
            return new ValidationResult(false, "Expecting [" + previousOutput.size() + "] records, got [" + output.size() + "]");
        }
        return CommandGenerator.expectSameColumns(previousColumns, columns);
    }
}
