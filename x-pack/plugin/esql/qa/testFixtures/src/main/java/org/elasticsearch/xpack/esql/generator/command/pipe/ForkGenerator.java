/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class ForkGenerator implements CommandGenerator {

    public static final String FORK = "fork";
    public static final CommandGenerator INSTANCE = new ForkGenerator();

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        // FORK can only be allowed once - so we skip adding another FORK if we already have one
        // otherwise, most generated queries would only result in a validation error
        StringBuilder completeCommand = new StringBuilder();
        for (CommandDescription command : previousCommands) {
            if (command.commandName().equals(FORK)) {
                return EMPTY_DESCRIPTION;
            }

            completeCommand.append(command.commandString());
        }

        final int branchCount = randomIntBetween(2, 8);
        final int branchToRetain = randomIntBetween(1, branchCount);

        StringBuilder forkCmd = new StringBuilder(" | FORK ");
        for (int i = 0; i < branchCount; i++) {
            var expr = WhereGenerator.randomExpression(randomIntBetween(1, 2), previousOutput, previousCommands);
            if (expr == null) {
                expr = "true";
            }
            forkCmd.append(" (").append("where ").append(expr);

            var exec = new EsqlQueryGenerator.Executor() {
                @Override
                public void run(CommandGenerator generator, CommandDescription current) {
                    final String command = current.commandString();

                    // Try appending new command to parent of Fork. If we successfully execute (without exception) AND still retain the same
                    // schema, we append the command. Enforcing the same schema is stricter than the Fork needs (it only needs types to be
                    // the same on columns which are present), but given we currently generate independent sub-pipelines, this way we can
                    // generate more valid Fork queries.
                    final QueryExecuted result = previousResult == null
                        ? executor.execute(command, 0)
                        : executor.execute(previousResult.query() + command, previousResult.depth());
                    previousResult = result;

                    continueExecuting = result.exception() == null && result.outputSchema().equals(previousOutput);
                    if (continueExecuting) {
                        previousCommands.add(current);
                    }
                }

                @Override
                public List<CommandDescription> previousCommands() {
                    return previousCommands;
                }

                @Override
                public boolean continueExecuting() {
                    return continueExecuting;
                }

                @Override
                public List<Column> currentSchema() {
                    return previousOutput;
                }

                final List<CommandGenerator.CommandDescription> previousCommands = new ArrayList<>();
                boolean continueExecuting;
                QueryExecuted previousResult;
            };

            var gen = new CommandGenerator() {
                @Override
                public CommandDescription generate(
                    List<CommandDescription> previousCommands,
                    List<Column> previousOutput,
                    QuerySchema schema,
                    QueryExecutor executor
                ) {
                    return new CommandDescription(FORK, this, completeCommand.toString(), Map.of());
                }

                @Override
                public ValidationResult validateOutput(
                    List<CommandDescription> previousCommands,
                    CommandDescription command,
                    List<Column> previousColumns,
                    List<List<Object>> previousOutput,
                    List<Column> columns,
                    List<List<Object>> output
                ) {
                    return VALIDATION_OK;
                }
            };

            EsqlQueryGenerator.generatePipeline(3, gen, schema, exec, false, executor);
            if (exec.previousCommands().size() > 1) {
                String previousCmd = exec.previousCommands()
                    .stream()
                    .skip(1)
                    .map(CommandDescription::commandString)
                    .collect(Collectors.joining(" "));
                forkCmd.append(previousCmd);
            }

            forkCmd.append(")");
        }
        forkCmd.append(" | WHERE _fork == \"fork").append(branchToRetain).append("\" | DROP _fork");

        // System.out.println("Generated fork command: " + forkCmd);
        return new CommandDescription(FORK, this, forkCmd.toString(), Map.of());
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription command,
        List<Column> previousColumns,
        List<List<Object>> previousOutput,
        List<Column> columns,
        List<List<Object>> output
    ) {
        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}
