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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class EnrichGeneratorTests extends ESTestCase {

    private static final CommandDescription FROM = new CommandDescription("from", FromGenerator.INSTANCE, "from idx", Map.of());
    private static final CommandDescription EQL = eql(false);
    private static final CommandDescription WHERE = new CommandDescription("where", WhereGenerator.INSTANCE, " | where true", Map.of());

    public void testNoModeAfterFrom() {
        assertThat(prefixes(List.of(FROM)), equalTo(Set.of("")));
        assertThat(prefixes(List.of(FROM, WHERE)), equalTo(Set.of("")));
    }

    public void testEveryModeRightAfterAnEqlSource() {
        assertThat(prefixes(List.of(EQL)), equalTo(Set.of("", "_any:", "_coordinator:", "_remote:")));
    }

    public void testNoRemoteModeLaterInAnEqlPipeline() {
        Set<String> prefixes = prefixes(List.of(EQL, WHERE));
        assertThat(prefixes, not(hasItem("_remote:")));
        assertThat(prefixes, hasItem("_coordinator:"));
    }

    public void testNoRemoteModeAfterAnEqlUnion() {
        assertThat(prefixes(List.of(eql(true))), not(hasItem("_remote:")));
    }

    private static Set<String> prefixes(List<CommandDescription> previousCommands) {
        Set<String> prefixes = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            prefixes.add(EnrichGenerator.modePrefix(previousCommands));
        }
        return prefixes;
    }

    private static CommandDescription eql(boolean union) {
        return new CommandDescription(
            EqlGenerator.EQL,
            EqlGenerator.INSTANCE,
            "EQL idx \"any where true\"",
            Map.of(FromGenerator.HAS_UNION_ALL, union)
        );
    }
}
