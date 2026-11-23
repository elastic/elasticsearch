/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.generator.command.CommandGenerator.EMPTY_DESCRIPTION;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class DedupGeneratorTests extends ESTestCase {

    public void testSkipsUnionTypeColumn() {
        assumeTrue("requires DEDUP", EsqlCapabilities.Cap.DEDUP_COMMAND.isEnabled());
        CommandDescription description = generate(
            List.of(new Column("first_name", "keyword", List.of()), new Column("emp_no", "unsupported", List.of("integer", "long")))
        );
        assertThat(description, is(sameInstance(EMPTY_DESCRIPTION)));
    }

    public void testGeneratesOverUnsupportedColumn() {
        assumeTrue("requires DEDUP", EsqlCapabilities.Cap.DEDUP_COMMAND.isEnabled());
        CommandDescription description = generate(
            List.of(new Column("env", "keyword", List.of()), new Column("client_cidr", "unsupported", List.of("ip_range")))
        );
        assertThat(description, is(not(sameInstance(EMPTY_DESCRIPTION))));
        assertThat(description.commandString(), is(" | DEDUP "));
    }

    public void testSkipsUnsupportedColumnAfterUnionAll() {
        assumeTrue("requires DEDUP", EsqlCapabilities.Cap.DEDUP_COMMAND.isEnabled());
        CommandDescription description = generate(
            List.of(fromWithUnionAll()),
            List.of(new Column("env", "keyword", List.of()), new Column("client_cidr", "unsupported", List.of("ip_range")))
        );
        assertThat(description, is(sameInstance(EMPTY_DESCRIPTION)));
    }

    public void testGeneratesAfterUnionAllWithoutUnsupportedColumn() {
        assumeTrue("requires DEDUP", EsqlCapabilities.Cap.DEDUP_COMMAND.isEnabled());
        CommandDescription description = generate(List.of(fromWithUnionAll()), List.of(new Column("env", "keyword", List.of())));
        assertThat(description.commandString(), is(" | DEDUP "));
    }

    private static CommandDescription fromWithUnionAll() {
        return new CommandDescription("from", null, "FROM (FROM a), (FROM b)", Map.of(FromGenerator.HAS_UNION_ALL, true));
    }

    private static CommandDescription generate(List<Column> previousOutput) {
        return generate(List.of(), previousOutput);
    }

    private static CommandDescription generate(List<CommandDescription> previousCommands, List<Column> previousOutput) {
        return DedupGenerator.INSTANCE.generate(previousCommands, previousOutput, null, null, null);
    }
}
