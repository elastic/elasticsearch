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

import static org.elasticsearch.test.ESTestCase.randomFrom;

public class EnrichGenerator implements CommandGenerator {

    public static final String ENRICH = "enrich";
    public static final CommandGenerator INSTANCE = new EnrichGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
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
        return new CommandDescription(ENRICH, cmdString, Map.of());
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
        if (commandDescription == EMPTY_DESCRIPTION) {
            return VALIDATION_OK;
        }

        // TODO validate columns
        return CommandGenerator.expectSameRowCount(previousOutput, output);
    }
}
