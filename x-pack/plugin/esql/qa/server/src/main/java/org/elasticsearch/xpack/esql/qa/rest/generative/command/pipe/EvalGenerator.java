/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe;

import org.elasticsearch.xpack.esql.qa.rest.generative.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.CommandGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class EvalGenerator implements CommandGenerator {

    public static final String EVAL = "eval";
    public static final String NEW_COLUMNS = "new_columns";
    public static final CommandGenerator INSTANCE = new EvalGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        StringBuilder cmd = new StringBuilder(" | eval ");
        int nFields = randomIntBetween(1, 10);
        // TODO pass newly created fields to next expressions
        var newColumns = new ArrayList<>();
        for (int i = 0; i < nFields; i++) {
            String name;
            if (randomBoolean()) {
                name = EsqlQueryGenerator.randomIdentifier();
            } else {
                name = EsqlQueryGenerator.randomName(previousOutput);
                if (name == null) {
                    name = EsqlQueryGenerator.randomIdentifier();
                }
            }
            String expression = EsqlQueryGenerator.expression(previousOutput);
            if (i > 0) {
                cmd.append(",");
            }
            cmd.append(" ");
            cmd.append(name);
            newColumns.remove(unquote(name));
            newColumns.add(unquote(name));
            cmd.append(" = ");
            cmd.append(expression);
        }
        String cmdString = cmd.toString();
        return new CommandDescription(EVAL, this, cmdString, Map.ofEntries(Map.entry(NEW_COLUMNS, newColumns)));
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
        List<String> expectedColumns = (List<String>) commandDescription.context().get(NEW_COLUMNS);
        List<String> resultColNames = columns.stream().map(EsqlQueryGenerator.Column::name).toList();
        List<String> lastColumns = resultColNames.subList(resultColNames.size() - expectedColumns.size(), resultColNames.size());
        lastColumns = lastColumns.stream().map(EvalGenerator::unquote).toList();
        // expected column names are unquoted already
        if (columns.size() < expectedColumns.size() || lastColumns.equals(expectedColumns) == false) {
            return new ValidationResult(
                false,
                "Expecting the following as last columns ["
                    + String.join(", ", expectedColumns)
                    + "] but got ["
                    + String.join(", ", resultColNames)
                    + "]"
            );
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }

    private static String unquote(String colName) {
        if (colName.startsWith("`") && colName.endsWith("`")) {
            return colName.substring(1, colName.length() - 1);
        }
        return colName;
    }
}
