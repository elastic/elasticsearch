/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe;

import org.elasticsearch.xpack.esql.qa.rest.generative.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.CommandGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class RenameGenerator implements CommandGenerator {

    public static final String RENAME = "rename";

    public static final CommandGenerator INSTANCE = new RenameGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    ) {
        int n = randomIntBetween(1, Math.min(3, previousOutput.size()));
        List<String> proj = new ArrayList<>();

        Map<String, String> nameToType = new HashMap<>();
        for (EsqlQueryGenerator.Column column : previousOutput) {
            nameToType.put(column.name(), column.type());
        }
        List<String> names = new ArrayList<>(
            previousOutput.stream()
                .filter(EsqlQueryGenerator::fieldCanBeUsed)
                .map(EsqlQueryGenerator.Column::name)
                .collect(Collectors.toList())
        );
        if (names.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }
        for (int i = 0; i < n; i++) {
            if (names.isEmpty()) {
                break;
            }
            var name = randomFrom(names);
            if (nameToType.get(name).endsWith("_range")) {
                // ranges are not fully supported yet
                continue;
            }
            names.remove(name);

            String newName;
            if (names.isEmpty() || randomBoolean()) {
                newName = EsqlQueryGenerator.randomIdentifier();
                names.add(newName);
            } else {
                newName = names.get(randomIntBetween(0, names.size() - 1));
            }
            nameToType.put(newName, nameToType.get(name));
            if (randomBoolean() && name.startsWith("`") == false) {
                name = "`" + name + "`";
            }
            if (randomBoolean() && newName.startsWith("`") == false) {
                newName = "`" + newName + "`";
            }
            proj.add(name + " AS " + newName);
        }
        if (proj.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }
        String cmdString = " | rename " + proj.stream().collect(Collectors.joining(", "));
        return new CommandDescription(RENAME, this, cmdString, Map.of());
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
        if (previousColumns.size() < columns.size()) {
            return new ValidationResult(false, "Expecting at most [" + previousColumns.size() + "] columns, got [" + columns.size() + "]");
        }
        return CommandGenerator.expectSameRowCount(previousOutput, output);
    }

}
