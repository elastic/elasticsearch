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

import java.util.List;

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

    private static CommandDescription generate(List<Column> previousOutput) {
        return DedupGenerator.INSTANCE.generate(List.of(), previousOutput, null, null, null);
    }
}
