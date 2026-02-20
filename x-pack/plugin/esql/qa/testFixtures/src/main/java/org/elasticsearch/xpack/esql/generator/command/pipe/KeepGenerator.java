/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.FunctionGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class KeepGenerator implements CommandGenerator {

    public static final String KEEP = "keep";

    public static final CommandGenerator INSTANCE = new KeepGenerator();
    public static final String[] UNMAPPED_FIELD_NAMES = {
        "unmapped_field_foo",
        "unmapped_field_foobar",
        "unmapped_field_bar",
        "unmapped_field_baz" };

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        boolean unmappedFieldsEnabled = FunctionGenerator.isUnmappedFieldsEnabled(previousCommands);
        int n = randomIntBetween(1, previousOutput.size());
        Set<String> proj = new HashSet<>();
        for (int i = 0; i < n; i++) {
            var x = randomIntBetween(0, 100);
            if (x < 5) {
                proj.add("*");
            } else if (x >= 95 && unmappedFieldsEnabled) {
                String name = randomUnmappedFieldName();
                proj.add(name);
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

    public static String randomUnmappedFieldName() {
        return randomFrom(UNMAPPED_FIELD_NAMES);
    }

    @Override
    @SuppressWarnings("unchecked")
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

        if (previousColumns.size() < columns.size()) {
            return new ValidationResult(false, "Expecting at most [" + previousColumns.size() + "] columns, got [" + columns.size() + "]");
        }

        return VALIDATION_OK;
    }

}
