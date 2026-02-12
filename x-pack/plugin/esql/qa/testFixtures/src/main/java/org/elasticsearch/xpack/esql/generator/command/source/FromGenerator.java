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
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.shouldAddUnmappedFieldWithProbabilityIncrease;

public class FromGenerator implements CommandGenerator {

    public static final FromGenerator INSTANCE = new FromGenerator();

    /**
     * Context key used to indicate whether SET unmapped_fields="nullify" was included in the FROM command.
     * When true, unmapped field names can be used in downstream commands and functions.
     */
    public static final String UNMAPPED_FIELDS_ENABLED = "unmappedFieldsEnabled";

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        boolean useUnmappedFields = shouldAddUnmappedFieldWithProbabilityIncrease(3);
        StringBuilder result = new StringBuilder();
        if (useUnmappedFields) {
            result.append("SET unmapped_fields=\"nullify\";");
        }
        result.append("from ");
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
        return new CommandDescription("from", this, query, Map.of(UNMAPPED_FIELDS_ENABLED, useUnmappedFields));
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
