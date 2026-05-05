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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.constantExpression;

/**
 * Generates a ROW source command, e.g. {@code ROW a = 1, b = ["hello", "world"], c = true}.
 * <p>
 *     Generators that require index-backed fields, like full-text search, already guard themselves via {@link FromGenerator#isFromSource}.
 * </p>
 */
public class RowGenerator implements CommandGenerator {

    public static final RowGenerator INSTANCE = new RowGenerator();

    private static final String COLUMNS = "columns";

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        int nFields = randomIntBetween(1, 10);
        StringBuilder result = new StringBuilder("ROW ");
        Set<String> names = new LinkedHashSet<>();

        for (int i = 0; i < nFields; i++) {
            // 25% of the time, reuse an existing field name. The last one wins.
            String name = (i > 0 && randomIntBetween(0, 3) == 0) ? randomFrom(names) : EsqlQueryGenerator.randomIdentifier();
            names.remove(name);
            names.add(name);

            if (i > 0) {
                result.append(", ");
            }
            result.append(name).append(" = ").append(constantExpression());
        }
        return new CommandDescription("row", this, result.toString(), Map.of(COLUMNS, List.copyOf(names)));
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
        if (output.size() != 1) {
            return new ValidationResult(false, "ROW should return exactly 1 row, got [" + output.size() + "]");
        }
        List<String> expectedNames = (List<String>) commandDescription.context().get(COLUMNS);
        List<String> actualNames = columns.stream().map(Column::name).toList();
        if (actualNames.equals(expectedNames) == false) {
            return new ValidationResult(false, "ROW columns mismatch: expected " + expectedNames + " but got " + actualNames);
        }
        return VALIDATION_OK;
    }
}
