/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;
import org.elasticsearch.xpack.esql.generator.command.pipe.ForkGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.LimitGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.MvExpandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.SortGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.StatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class FullTextFunctionGeneratorTests extends ESTestCase {

    private static final List<Column> COLUMNS = List.of(new Column("type", "keyword", List.of("keyword"), true));

    private static final CommandDescription FROM = new CommandDescription("from", FromGenerator.INSTANCE, "from airports", Map.of());

    public void testNoFullTextAfterForkWhoseKeptBranchHasForbiddenCommand() {
        String forbidden = randomFrom(MvExpandGenerator.MV_EXPAND, LimitGenerator.LIMIT, StatsGenerator.STATS);

        assertThat(FullTextFunctionGenerator.fullTextFunction(COLUMNS, List.of(FROM, fork(command(forbidden)))), nullValue());
    }

    public void testFullTextAfterForkWhoseKeptBranchHasOnlyAllowedCommands() {
        assertThat(FullTextFunctionGenerator.fullTextFunction(COLUMNS, List.of(FROM, fork(command(SortGenerator.SORT)))), notNullValue());
    }

    public void testFullTextAfterForkWithEmptyKeptBranch() {
        assertThat(FullTextFunctionGenerator.fullTextFunction(COLUMNS, List.of(FROM, fork())), notNullValue());
    }

    private static CommandDescription command(String name) {
        return new CommandDescription(name, null, " | " + name, Map.of());
    }

    private static CommandDescription fork(CommandDescription... keptBranch) {
        return new CommandDescription(
            ForkGenerator.FORK,
            ForkGenerator.INSTANCE,
            " | FORK (where true)",
            Map.of(ForkGenerator.KEPT_BRANCH_COMMANDS, List.of(keptBranch))
        );
    }
}
