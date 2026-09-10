/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.GenerativeFeature;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.unquote;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.isUnmappedFieldsEnabled;

public class EvalGenerator implements CommandGenerator {

    public static final String EVAL = "eval";
    public static final String NEW_COLUMNS = "new_columns";
    /**
     * Set when {@link GenerativeFeature#IN_SUBQUERY} is enabled so {@link #validateOutput} can use the looser presence check.
     * {@code validateOutput} does not receive {@link GenerationContext}.
     */
    public static final String IN_SUBQUERY_ENABLED = "inSubqueryEnabled";
    public static final CommandGenerator INSTANCE = new EvalGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        StringBuilder cmd = new StringBuilder(" | eval ");
        int nFields = randomIntBetween(1, 10);
        Map<String, Column> usablePrevious = previousOutput.stream().collect(Collectors.toMap(Column::name, c -> c, (c1, c2) -> c1));
        // TODO pass newly created fields to next expressions
        var newColumns = new ArrayList<>();
        for (int i = 0; i < nFields; i++) {
            String name;
            if (randomBoolean()) {
                name = EsqlQueryGenerator.randomIdentifier();
            } else {
                name = EsqlQueryGenerator.randomName(previousOutput);
                if (name == null) {
                    name = EsqlQueryGenerator.randomIdentifier();
                }
            }
            List<Column> usableColumns = usablePrevious.values().stream().toList();
            String expression = EsqlQueryGenerator.maybeInSubqueryBooleanExpression(usableColumns, schema, executor, context);
            if (expression == null) {
                // Occasionally generate a null field (EVAL field = null) to test NULL data type handling
                if (randomIntBetween(0, 100) < 10) {
                    expression = "null";
                } else {
                    expression = EsqlQueryGenerator.expression(usableColumns, true, previousCommands);
                }
            }
            if (i > 0) {
                cmd.append(",");
            }
            cmd.append(" ");
            cmd.append(name);
            String rawName = unquote(name);
            newColumns.remove(rawName);
            newColumns.add(rawName);
            cmd.append(" = ");
            cmd.append(expression);

            // there could be collisions in many ways, remove all of them
            usablePrevious.remove(name);
            usablePrevious.remove("`" + name + "`");
            usablePrevious.remove(rawName);
        }
        String cmdString = cmd.toString();
        return new CommandDescription(
            EVAL,
            this,
            cmdString,
            Map.ofEntries(
                Map.entry(NEW_COLUMNS, newColumns),
                Map.entry(IN_SUBQUERY_ENABLED, context.isFeatureEnabled(GenerativeFeature.IN_SUBQUERY))
            )
        );
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
        List<String> expectedColumns = (List<String>) commandDescription.context().get(NEW_COLUMNS);
        List<String> resultColNames = columns.stream().map(Column::name).toList();
        if (isUnmappedFieldsEnabled(previousCommands) == false) {
            ValidationResult columnsResult = Boolean.TRUE.equals(commandDescription.context().get(IN_SUBQUERY_ENABLED))
                ? validateInSubqueryEvalColumns(expectedColumns, resultColNames)
                : validateBaselineEvalColumns(expectedColumns, resultColNames, columns.size());
            if (columnsResult.success() == false) {
                return columnsResult;
            }
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }

    /**
     * Baseline check from main: assigned names must appear as the last columns, in assignment order. This assumes every assignment appends
     * a column, which is false when EVAL overwrites an existing field.
     */
    private static ValidationResult validateBaselineEvalColumns(
        List<String> expectedColumns,
        List<String> resultColNames,
        int columnCount
    ) {
        if (columnCount >= expectedColumns.size()) {
            List<String> lastColumns = resultColNames.subList(resultColNames.size() - expectedColumns.size(), resultColNames.size());
            if (lastColumns.equals(expectedColumns)) {
                return VALIDATION_OK;
            }
        }
        return new ValidationResult(
            false,
            "Expecting the following as last columns ["
                + String.join(", ", expectedColumns)
                + "] but got ["
                + String.join(", ", resultColNames)
                + "]"
        );
    }

    /**
     * IN subquery EVAL may overwrite an existing field (left in place) or leak synthetic mark columns, so only require that every assigned
     * name is present.
     */
    private static ValidationResult validateInSubqueryEvalColumns(List<String> expectedColumns, List<String> resultColNames) {
        if (resultColNames.containsAll(expectedColumns)) {
            return VALIDATION_OK;
        }
        return new ValidationResult(
            false,
            "Expecting columns [" + String.join(", ", expectedColumns) + "] but got [" + String.join(", ", resultColNames) + "]"
        );
    }
}
