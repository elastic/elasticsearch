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

public class DissectGenerator implements CommandGenerator {

    public static final String DISSECT = "dissect";
    public static final CommandGenerator INSTANCE = new DissectGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        String field = EsqlQueryGenerator.randomStringField(previousOutput);
        if (field == null) {
            return EMPTY_DESCRIPTION;// no strings to dissect, just skip
        }
        StringBuilder result = new StringBuilder(" | dissect ");
        result.append(field);
        result.append(" \"");
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            if (i > 0) {
                result.append(" ");
            }
            result.append("%{");
            String fieldName;
            if (randomBoolean()) {
                fieldName = EsqlQueryGenerator.randomIdentifier();
            } else {
                fieldName = EsqlQueryGenerator.randomRawName(previousOutput);
                if (fieldName == null) {
                    fieldName = EsqlQueryGenerator.randomIdentifier();
                }
            }
            result.append(fieldName);
            result.append("}");
        }
        result.append("\"");
        String cmdString = result.toString();
        return new CommandDescription(DISSECT, this, cmdString, Map.of());
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

        if (previousColumns.size() > columns.size()) {
            return new ValidationResult(false, "Expecting at least [" + previousColumns.size() + "] columns, got [" + columns.size() + "]");
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}
