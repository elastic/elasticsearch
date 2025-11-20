/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.source;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class FromGenerator implements CommandGenerator {

    public static final FromGenerator INSTANCE = new FromGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        StringBuilder result = new StringBuilder("from ");
        int items = randomIntBetween(1, 3);
        List<String> availableIndices = schema.baseIndices();
        for (int i = 0; i < items; i++) {
            String pattern = EsqlQueryGenerator.indexPattern(availableIndices.get(randomIntBetween(0, availableIndices.size() - 1)));
            if (i > 0) {
                result.append(",");
            }
            result.append(pattern);
        }
        String query = result.toString();
        return new CommandDescription("from", this, query, Map.of());
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
        return VALIDATION_OK;
    }
}
