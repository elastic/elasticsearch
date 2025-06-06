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

public class KeepGenerator implements CommandGenerator {

    public static final String KEEP = "keep";

    public static final CommandGenerator INSTANCE = new KeepGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        int n = randomIntBetween(1, previousOutput.size());
        Set<String> proj = new HashSet<>();
        for (int i = 0; i < n; i++) {
            if (randomIntBetween(0, 100) < 5) {
                proj.add("*");
            } else {
                String name = EsqlQueryGenerator.randomName(previousOutput);
                if (name == null) {
                    continue;
                }
                if (name.length() > 1 && name.startsWith("`") == false && randomIntBetween(0, 100) < 10) {
                    if (randomBoolean()) {
                        name = name.substring(0, randomIntBetween(1, name.length() - 1)) + "*";
                    } else {
                        name = "*" + name.substring(randomIntBetween(1, name.length() - 1));
                    }
                }
                proj.add(name);
            }
        }
        if (proj.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }
        String cmdString = " | keep " + proj.stream().collect(Collectors.joining(", "));
        return new CommandDescription(KEEP, this, cmdString, Map.of());
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
        if (commandDescription == EMPTY_DESCRIPTION) {
            return VALIDATION_OK;
        }

        // TODO awaits fix https://github.com/elastic/elasticsearch/issues/120272
        // return CommandGenerator.expectSameRowCount(previousOutput, output);
        return VALIDATION_OK;
    }

}
