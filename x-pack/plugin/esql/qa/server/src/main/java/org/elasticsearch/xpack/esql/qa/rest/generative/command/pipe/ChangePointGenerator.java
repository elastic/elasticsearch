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

import static org.elasticsearch.xpack.esql.qa.rest.generative.EsqlQueryGenerator.randomAttributeOrIdentifier;

public class ChangePointGenerator implements CommandGenerator {
    public static final String CHANGE_POINT = "change_point";
    public static final CommandGenerator INSTANCE = new ChangePointGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        String timestampField = EsqlQueryGenerator.randomDateField(previousOutput);
        String numericField = EsqlQueryGenerator.randomNumericField(previousOutput);
        if (timestampField == null || numericField == null) {
            return EMPTY_DESCRIPTION;
        }
        String alias1 = randomAttributeOrIdentifier(previousOutput);
        String alias2 = randomAttributeOrIdentifier(previousOutput);
        while (alias1.equals(alias2)) {
            alias2 = randomAttributeOrIdentifier(previousOutput);
        }

        String cmd = " | CHANGE_POINT " + numericField + " ON " + timestampField + " AS " + alias1 + ", " + alias2;

        return new CommandDescription(CHANGE_POINT, this, cmd, Map.of());
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription command,
        List<EsqlQueryGenerator.Column> previousColumns,
        List<List<Object>> previousOutput,
        List<EsqlQueryGenerator.Column> columns,
        List<List<Object>> output
    ) {
        return CommandGenerator.expectAtLeastSameColumnNumber(previousColumns, columns);
    }
}
