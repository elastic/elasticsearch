/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class WhereGenerator implements CommandGenerator {

    public static final String WHERE = "where";
    public static final CommandGenerator INSTANCE = new WhereGenerator();

    public static String randomExpression(
        int nConditions,
        List<Column> previousOutput,
        List<CommandDescription> previousCommands,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        // TODO more complex conditions
        List<String> exps = new ArrayList<>(nConditions);
        for (int i = 0; i < nConditions; i++) {
            String exp = EsqlQueryGenerator.booleanExpression(previousOutput, previousCommands, schema, executor, context);
            if (exp == null) {
                return null;
            }
            exps.add(exp);
        }

        var result = new StringBuilder();
        for (int i = 0; i < exps.size(); i++) {
            if (i > 0) {
                result.append(randomBoolean() ? " AND " : " OR ");
            }
            if (randomBoolean()) {
                result.append(" NOT ");
            }
            result.append(exps.get(i));
        }
        return result.toString();
    }

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        String expression = randomExpression(randomIntBetween(1, 5), previousOutput, previousCommands, schema, executor, context);
        if (expression == null) {
            return EMPTY_DESCRIPTION;
        }
        return new CommandDescription(WHERE, this, " | where " + expression, Map.of());
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
