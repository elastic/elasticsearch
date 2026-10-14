/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.QuerySchema;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class ForkGeneratorTests extends ESTestCase {

    private static final List<Column> COLUMNS = List.of(
        new Column("type", "keyword", List.of("keyword"), true),
        new Column("scalerank", "integer", List.of("integer"), true)
    );

    private static final Pattern KEPT_BRANCH = Pattern.compile("WHERE _fork == \"fork(\\d+)\"");

    private static final String BRANCH_START = " (where ";

    /**
     * Every generated branch command is accepted, because the executor reports success with an unchanged schema
     * for any query. That keeps the branches populated, which is all this test needs.
     */
    private static final QueryExecutor ACCEPT_EVERYTHING = (query, depth) -> new QueryExecuted(query, depth, COLUMNS, List.of(), null);

    public void testRecordsTheCommandsOfTheKeptBranch() {
        CommandDescription from = new CommandDescription("from", FromGenerator.INSTANCE, "from airports", Map.of());
        QuerySchema schema = new QuerySchema(List.of("airports"), List.of(), List.of(), Set.of());
        int withKeptCommands = 0;
        for (int i = 0; i < 20; i++) {
            CommandDescription fork = ForkGenerator.INSTANCE.generate(
                List.of(from),
                COLUMNS,
                schema,
                ACCEPT_EVERYTHING,
                GenerationContext.root(Set.of())
            );
            List<CommandDescription> kept = ForkGenerator.keptBranchCommands(fork);
            String keptCommands = kept.stream().map(CommandDescription::commandString).collect(Collectors.joining(" "));

            assertThat(keptBranch(fork.commandString()), endsWith(keptCommands + ")"));
            if (kept.isEmpty() == false) {
                withKeptCommands++;
            }
        }
        assertThat(withKeptCommands, greaterThan(0));
    }

    public void testKeptBranchCommandsOfCommandWithoutRecordIsEmpty() {
        CommandDescription fork = new CommandDescription(ForkGenerator.FORK, ForkGenerator.INSTANCE, " | FORK (where true)", Map.of());

        assertThat(ForkGenerator.keptBranchCommands(fork), is(empty()));
    }

    private static String keptBranch(String forkCommand) {
        Matcher matcher = KEPT_BRANCH.matcher(forkCommand);
        assertTrue(forkCommand, matcher.find());
        int kept = Integer.parseInt(matcher.group(1));
        int start = -1;
        for (int i = 0; i < kept; i++) {
            start = forkCommand.indexOf(BRANCH_START, start + 1);
        }
        int next = forkCommand.indexOf(BRANCH_START, start + 1);
        int end = next == -1 ? forkCommand.indexOf(" | WHERE _fork") : next;
        return forkCommand.substring(start, end);
    }
}
