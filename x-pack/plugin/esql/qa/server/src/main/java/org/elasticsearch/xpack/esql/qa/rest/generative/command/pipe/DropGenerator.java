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

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class DropGenerator implements CommandGenerator {

    public static final String DROP = "drop";
    public static final String DROPPED_COLUMNS = "dropped_columns";

    public static final CommandGenerator INSTANCE = new DropGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        if (previousOutput.size() < 2) {
            return CommandGenerator.EMPTY_DESCRIPTION; // don't drop all of them, just do nothing
        }
        Set<String> droppedColumns = new HashSet<>();
        int n = randomIntBetween(1, previousOutput.size() - 1);
        Set<String> proj = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String name = EsqlQueryGenerator.randomRawName(previousOutput);
            if (name == null) {
                continue;
            }
            if (name.length() > 1 && name.startsWith("`") == false && randomIntBetween(0, 100) < 10) {
                if (randomBoolean()) {
                    name = name.substring(0, randomIntBetween(1, name.length() - 1)) + "*";
                } else {
                    name = "*" + name.substring(randomIntBetween(1, name.length() - 1));
                }
            } else if (name.startsWith("`") == false && (randomBoolean() || name.isEmpty())) {
                name = "`" + name + "`";
            }
            proj.add(name);
            droppedColumns.add(EsqlQueryGenerator.unquote(name));
        }
        if (proj.isEmpty()) {
            return CommandGenerator.EMPTY_DESCRIPTION;
        }
        String cmdString = " | drop " + proj.stream().collect(Collectors.joining(", "));
        return new CommandDescription(DROP, this, cmdString, Map.ofEntries(Map.entry(DROPPED_COLUMNS, droppedColumns)));
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
        if (commandDescription == EMPTY_DESCRIPTION) {
            return VALIDATION_OK;
        }
        Set<String> droppedColumns = (Set<String>) commandDescription.context().get(DROPPED_COLUMNS);
        List<String> resultColNames = columns.stream().map(EsqlQueryGenerator.Column::name).toList();
        // expected column names are unquoted already
        for (String droppedColumn : droppedColumns) {
            if (resultColNames.contains(droppedColumn)) {
                return new ValidationResult(false, "Column [" + droppedColumn + "] was not dropped");
            }
        }
        // TODO awaits fix https://github.com/elastic/elasticsearch/issues/120272
        // return CommandGenerator.expectSameRowCount(previousOutput, output);
        return VALIDATION_OK;
    }

}
