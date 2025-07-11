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

public class UntableGenerator implements CommandGenerator {

    public static final String UNTABLE = "untable";
    public static final CommandGenerator INSTANCE = new UntableGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        int n = randomIntBetween(1, previousOutput.size());
        Set<String> columns = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String col = EsqlQueryGenerator.randomKeywordField(previousOutput);
            if (col == null) {
                return EMPTY_DESCRIPTION; // no sortable columns
            }
            columns.add(col);
        }
        String keyCol = randomName(previousOutput, null);
        String valueCol = randomName(previousOutput, keyCol);

        String cmd = " | UNTABLE " + keyCol + " FOR " + valueCol + " IN (" + columns.stream().collect(Collectors.joining(", ")) + ")";
        return new CommandDescription(UNTABLE, this, cmd, Map.of());
    }

    private static String randomName(List<EsqlQueryGenerator.Column> previousOutput, String excluding) {
        String name;
        if (randomBoolean()) {
            name = EsqlQueryGenerator.randomIdentifier();
        } else {
            name = EsqlQueryGenerator.randomName(previousOutput);
            if (name == null || name.equals(excluding)) {
                name = EsqlQueryGenerator.randomIdentifier();
            }
        }
        return name;
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
        return VALIDATION_OK;
    }
}
