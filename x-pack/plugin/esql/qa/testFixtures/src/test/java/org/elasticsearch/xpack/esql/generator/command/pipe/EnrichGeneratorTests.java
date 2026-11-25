/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;
import org.elasticsearch.xpack.esql.generator.command.source.EqlGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class EnrichGeneratorTests extends ESTestCase {

    private static final CommandDescription FROM = new CommandDescription("from", FromGenerator.INSTANCE, "from idx", Map.of());
    private static final CommandDescription EQL = new CommandDescription(
        EqlGenerator.EQL,
        EqlGenerator.INSTANCE,
        "EQL idx \"any where true\"",
        Map.of()
    );
    private static final CommandDescription WHERE = new CommandDescription("where", WhereGenerator.INSTANCE, " | where true", Map.of());

    public void testNoModeAfterFrom() {
        assertThat(prefixes(List.of(FROM)), equalTo(Set.of("")));
        assertThat(prefixes(List.of(FROM, WHERE)), equalTo(Set.of("")));
    }

    public void testEveryModeButRemoteAfterAnEqlSource() {
        assertThat(prefixes(List.of(EQL)), equalTo(Set.of("", "_any:", "_coordinator:")));
        assertThat(prefixes(List.of(EQL, WHERE)), equalTo(Set.of("", "_any:", "_coordinator:")));
    }

    private static Set<String> prefixes(List<CommandDescription> previousCommands) {
        Set<String> prefixes = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            prefixes.add(EnrichGenerator.modePrefix(previousCommands));
        }
        return prefixes;
    }
}
