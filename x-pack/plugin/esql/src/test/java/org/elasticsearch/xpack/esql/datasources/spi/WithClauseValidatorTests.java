/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

public class WithClauseValidatorTests extends ESTestCase {

    public void testNullConfigIsAccepted() {
        WithClauseValidator.check(null, List.of(Set.of()));
    }

    public void testEmptyConfigIsAccepted() {
        WithClauseValidator.check(Map.of(), List.of(Set.of()));
    }

    public void testFullyClaimedConfigIsAccepted() {
        Map<String, Object> config = Map.of("a", 1, "b", 2);
        WithClauseValidator.check(config, List.of(Set.of("a"), Set.of("b")));
    }

    public void testUnknownKeyIsRejected() {
        Map<String, Object> config = Map.of("a", 1, "typo", 2);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> WithClauseValidator.check(config, List.of(Set.of("a")))
        );
        assertThat(e.getMessage(), allOf(containsString("typo"), containsString("unknown option ")));
    }

    public void testMultipleUnknownsReportedSorted() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("zebra_typo", 1);
        config.put("alpha_typo", 2);
        config.put("known", 3);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> WithClauseValidator.check(config, List.of(Set.of("known")))
        );
        assertThat(e.getMessage(), containsString("[alpha_typo, zebra_typo]"));
    }

    public void testRecognisedSetUnionMentionedInError() {
        Map<String, Object> config = Map.of("typo", 1, "a", 1, "b", 2);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> WithClauseValidator.check(config, List.of(Set.of("a"), Set.of("b"), Set.of("c")))
        );
        assertThat(e.getMessage(), allOf(containsString("a"), containsString("b"), containsString("c")));
    }

    public void testSingleVsPluralWording() {
        IllegalArgumentException one = expectThrows(
            IllegalArgumentException.class,
            () -> WithClauseValidator.check(Map.of("typo", 1), List.of(Set.of()))
        );
        assertThat(one.getMessage(), containsString("unknown option ["));

        Map<String, Object> two = new HashMap<>();
        two.put("typo_a", 1);
        two.put("typo_b", 2);
        IllegalArgumentException many = expectThrows(
            IllegalArgumentException.class,
            () -> WithClauseValidator.check(two, List.of(Set.of()))
        );
        assertThat(many.getMessage(), containsString("unknown options ["));
    }

    public void testNoClaimedSetsRejectsAnyConfigKey() {
        Map<String, Object> config = Map.of("anything", 1);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> WithClauseValidator.check(config, List.of()));
        assertThat(e.getMessage(), containsString("anything"));
    }
}
