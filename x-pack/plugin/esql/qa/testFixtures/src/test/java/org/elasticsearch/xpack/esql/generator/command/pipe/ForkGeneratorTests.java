/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class ForkGeneratorTests extends ESTestCase {

    private static final CommandDescription PREFIX = new CommandDescription(
        ForkGenerator.FORK,
        FromGenerator.INSTANCE,
        "from idx | eval x = 1",
        Map.of()
    );
    private static final CommandDescription WHERE = new CommandDescription("where", WhereGenerator.INSTANCE, " | where true", Map.of());

    /**
     * Freshly executed schemas mark every column index-mapped; the input schema marks the {@code EVAL}-derived column as not.
     */
    public void testBranchKeepsCommandsWhenInputHasDerivedColumn() {
        List<Column> executed = List.of(new Column("a", "keyword", null), new Column("x", "integer", null));
        List<Column> input = List.of(new Column("a", "keyword", null, true), new Column("x", "integer", null, false));
        List<String> queries = new ArrayList<>();
        var branch = new ForkGenerator.BranchExecutor((query, depth) -> {
            queries.add(query);
            return new QueryExecuted(query, depth, executed, List.of(), null);
        }, input);

        branch.run(PREFIX.generator(), PREFIX);
        assertTrue(branch.continueExecuting());
        branch.run(WHERE.generator(), WHERE);
        assertTrue(branch.continueExecuting());
        assertThat(branch.previousCommands(), contains(PREFIX, WHERE));
        assertThat(queries, contains("from idx | eval x = 1", "from idx | eval x = 1 | where true"));
    }

    public void testBranchRejectsCommandThatChangesSchema() {
        List<Column> input = List.of(new Column("a", "keyword", null), new Column("x", "integer", List.of("integer", "long")));
        List<List<Column>> mismatches = List.of(
            List.of(new Column("a", "keyword", null)),
            List.of(new Column("a", "keyword", null), new Column("y", "integer", List.of("integer", "long"))),
            List.of(new Column("a", "keyword", null), new Column("x", "long", List.of("integer", "long"))),
            List.of(new Column("a", "keyword", null), new Column("x", "integer", null))
        );
        for (List<Column> changed : mismatches) {
            var branch = new ForkGenerator.BranchExecutor(
                (query, depth) -> new QueryExecuted(query, depth, changed, List.of(), null),
                input
            );

            branch.run(PREFIX.generator(), PREFIX);
            assertFalse(changed.toString(), branch.continueExecuting());
            assertThat(branch.previousCommands(), empty());
        }
    }

    public void testBranchRejectsFailedCommand() {
        List<Column> input = List.of(new Column("a", "keyword", null));
        var branch = new ForkGenerator.BranchExecutor(
            (query, depth) -> new QueryExecuted(query, depth, null, null, new RuntimeException("boom")),
            input
        );

        branch.run(PREFIX.generator(), PREFIX);
        assertFalse(branch.continueExecuting());
        assertThat(branch.previousCommands(), empty());
    }
}
