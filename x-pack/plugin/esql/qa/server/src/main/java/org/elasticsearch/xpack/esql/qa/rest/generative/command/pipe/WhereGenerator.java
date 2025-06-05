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

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class WhereGenerator implements CommandGenerator {

    public static final String WHERE = "where";
    public static final CommandGenerator INSTANCE = new WhereGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        // TODO more complex conditions
        StringBuilder result = new StringBuilder(" | where ");
        int nConditions = randomIntBetween(1, 5);
        for (int i = 0; i < nConditions; i++) {
            String exp = EsqlQueryGenerator.booleanExpression(previousOutput);
            if (exp == null) {
                // cannot generate expressions, just skip
                return EMPTY_DESCRIPTION;
            }
            if (i > 0) {
                result.append(randomBoolean() ? " AND " : " OR ");
            }
            if (randomBoolean()) {
                result.append(" NOT ");
            }
            result.append(exp);
        }

        String cmd = result.toString();
        return new CommandDescription(WHERE, cmd, Map.of());
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
        if (previousOutput.size() < output.size()) {
            return new ValidationResult(false, "Expecting at most [" + previousOutput.size() + "] records, got [" + output.size() + "]");
        }
        return CommandGenerator.expectSameColumns(previousColumns, columns);
    }
}
