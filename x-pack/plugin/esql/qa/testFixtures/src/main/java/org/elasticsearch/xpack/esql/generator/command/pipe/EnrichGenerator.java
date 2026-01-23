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

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomFrom;

public class EnrichGenerator implements CommandGenerator {

    public static final String ENRICH = "enrich";
    public static final CommandGenerator INSTANCE = new EnrichGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        String field = EsqlQueryGenerator.randomKeywordField(previousOutput);
        if (field == null || schema.enrichPolicies().isEmpty()) {
            return EMPTY_DESCRIPTION;
        }

        // TODO add WITH
        String cmdString = " | enrich "
            + randomFrom(EsqlQueryGenerator.policiesOnKeyword(schema.enrichPolicies())).policyName()
            + " on "
            + field;
        return new CommandDescription(ENRICH, this, cmdString, Map.of());
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

        if (previousColumns.size() > columns.size()) {
            return new ValidationResult(false, "Expecting at least [" + previousColumns.size() + "] columns, got [" + columns.size() + "]");
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}
