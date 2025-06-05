/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative.command;

import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.qa.rest.generative.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeRestTest;

import java.util.List;
import java.util.Map;

/**
 * Implement this if you want to your command to be tested by the random query generator.
 * Then add it to the right list in {@link EsqlQueryGenerator}
 *
 * The i
 */
public interface CommandGenerator {

    /**
     * @param commandName   the name of the command that is being generated
     * @param commandString the full command string, including the "|"
     * @param context       additional information that could be useful for output validation.
     *                      This will be passed to validateOutput after the query execution, together with the query output
     */
    record CommandDescription(String commandName, String commandString, Map<String, Object> context) {}

    record QuerySchema(
        List<String> baseIndices,
        List<GenerativeRestTest.LookupIdx> lookupIndices,
        List<CsvTestsDataLoader.EnrichConfig> enrichPolicies
    ) {}

    record ValidationResult(boolean success, String errorMessage) {}

    CommandDescription EMPTY_DESCRIPTION = new CommandDescription("<empty>", "", Map.of());

    ValidationResult VALIDATION_OK = new ValidationResult(true, null);

    /**
     * Implement this method to generate a command, that will be appended to an existing query and then executed.
     * See also {@link CommandDescription}
     *
     * @param previousCommands the list of the previous commands in the query
     * @param previousOutput the output returned by the query so far.
     * @param schema The columns returned by the query so far. It contains name and type information for each column.
     * @return All the details about the generated command. See {@link CommandDescription}.
     * If something goes wrong and for some reason you can't generate a command, you should return {@link CommandGenerator#EMPTY_DESCRIPTION}
     */
    CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<EsqlQueryGenerator.Column> previousOutput,
        QuerySchema schema
    );

    /**
     * This will be invoked after the query execution.
     * You are expected to put validation logic in here.
     * @param previousCommands The list of commands before the last generated one.
     * @param command The description of the command you just generated.
     *                It also contains the context information you stored during command generation.
     * @param previousColumns The output schema of the original query (without last generated command).
     *                        It contains name and type information for each column, see {@link EsqlQueryGenerator.Column}
     * @param previousOutput The output of the original query (without last generated command), as a list (rows) of lists (columns) of values
     * @param columns The output schema of the full query (WITH last generated command).
     * @param output The output of the full query (WITH last generated command), as a list (rows) of lists (columns) of values
     * @return The result of the output validation. If the validation succeeds, you should return {@link CommandGenerator#VALIDATION_OK}
     */
    ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription command,
        List<EsqlQueryGenerator.Column> previousColumns,
        List<List<Object>> previousOutput,
        List<EsqlQueryGenerator.Column> columns,
        List<List<Object>> output
    );

    static ValidationResult expectSameRowCount(List<List<Object>> previousOutput, List<List<Object>> output) {
        if (output.size() != previousOutput.size()) {
            return new ValidationResult(false, "Expecting [" + previousOutput.size() + "] rows, but got [" + output.size() + "]");
        }

        return VALIDATION_OK;
    }

    static ValidationResult expectSameColumns(List<EsqlQueryGenerator.Column> previousColumns, List<EsqlQueryGenerator.Column> columns) {

        if (previousColumns.stream().anyMatch(x -> x.name().contains("<all-fields-projected>"))) {
            return VALIDATION_OK; // known bug
        }

        if (previousColumns.size() != columns.size()) {
            return new ValidationResult(false, "Expecting [" + previousColumns.size() + "] columns, got [" + columns.size() + "]");
        }

        // TODO awaits fix: https://github.com/elastic/elasticsearch/issues/129000
        // List<String> prevColNames = previousColumns.stream().map(EsqlQueryGenerator.Column::name).toList();
        // List<String> newColNames = columns.stream().map(EsqlQueryGenerator.Column::name).toList();
        // if (prevColNames.equals(newColNames) == false) {
        // return new ValidationResult(false,
        // "Expecting the following columns ["
        // + String.join(", ", prevColNames)
        // + "] columns, got ["
        // + String.join(", ", newColNames) + "]");
        // }

        return VALIDATION_OK;
    }
}
