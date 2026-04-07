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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class LimitByGenerator implements CommandGenerator {
    public static final CommandGenerator INSTANCE = new LimitByGenerator();
    public static final String LIMIT_BY = "limit_by";

    private static final String LIMIT_CONTEXT = "limit";
    private static final String GROUPINGS_CONTEXT = "groupings";

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        List<Column> groupable = previousOutput.stream()
            .filter(EsqlQueryGenerator::groupable)
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .toList();
        if (groupable.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }

        int limit = randomIntBetween(0, 100);
        int groupingCount = randomIntBetween(1, Math.min(3, groupable.size()));
        Set<String> groupings = new LinkedHashSet<>();
        for (int i = 0; i < groupingCount; i++) {
            String col = EsqlQueryGenerator.randomGroupableName(groupable);
            if (col != null) {
                groupings.add(col);
            }
        }
        if (groupings.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }

        String cmd = " | LIMIT " + limit + " BY " + String.join(", ", groupings);
        return new CommandDescription(LIMIT_BY, this, cmd, Map.of(LIMIT_CONTEXT, limit, GROUPINGS_CONTEXT, List.copyOf(groupings)));
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
        int limit = (int) commandDescription.context().get(LIMIT_CONTEXT);

        if (limit == 0 && output.isEmpty() == false) {
            return new ValidationResult(false, "LIMIT 0 BY should return no rows, got [" + output.size() + "]");
        }

        ValidationResult columnsResult = CommandGenerator.expectSameColumns(previousCommands, previousColumns, columns);
        if (columnsResult.success() == false) {
            return columnsResult;
        }

        return validatePerGroupRowCounts(commandDescription, columns, output, limit);
    }

    @SuppressWarnings("unchecked")
    private static ValidationResult validatePerGroupRowCounts(
        CommandDescription commandDescription,
        List<Column> columns,
        List<List<Object>> output,
        int limit
    ) {
        List<String> groupings = (List<String>) commandDescription.context().get(GROUPINGS_CONTEXT);

        List<Integer> groupingIndices = new ArrayList<>(groupings.size());
        for (String grouping : groupings) {
            String rawName = EsqlQueryGenerator.unquote(grouping);
            int idx = -1;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).name().equals(rawName)) {
                    idx = i;
                    break;
                }
            }
            if (idx == -1) {
                return new ValidationResult(
                    false,
                    "LIMIT "
                        + limit
                        + " BY: grouping column ["
                        + rawName
                        + "] was not found in the output schema. Available columns: "
                        + columns.stream().map(Column::name).toList()
                );
            }
            groupingIndices.add(idx);
        }

        Map<List<Object>, Integer> groupCounts = new HashMap<>();
        for (List<Object> row : output) {
            List<Object> key = new ArrayList<>(groupingIndices.size());
            for (int idx : groupingIndices) {
                Object value = row.get(idx);
                key.add(value);
            }
            groupCounts.merge(key, 1, Integer::sum);
        }

        for (var entry : groupCounts.entrySet()) {
            if (entry.getValue() > limit) {
                return new ValidationResult(
                    false,
                    "LIMIT "
                        + limit
                        + " BY: group "
                        + entry.getKey()
                        + " has ["
                        + entry.getValue()
                        + "] rows, expected at most ["
                        + limit
                        + "]"
                );
            }
        }

        return VALIDATION_OK;
    }
}
