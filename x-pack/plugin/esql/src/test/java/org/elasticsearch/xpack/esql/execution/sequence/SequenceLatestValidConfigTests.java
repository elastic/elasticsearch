/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SequenceLatestValidConfigTests extends ESTestCase {
    public void testProfiledPredicatesPreserveConfigurationAndMeasureOnFailure() {
        var clock = new AtomicLong();
        var profiler = new SequenceRuntimeProfiler(true, 2, clock::get);
        var demands = List.<List<SequenceLatestValidConfig.ValueDemand>>of(List.of(), List.of());
        var groups = List.of(new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), values -> {
            clock.addAndGet(7);
            throw new IllegalStateException("predicate failure");
        }));
        var production = SequenceLatestValidConfig.bounded(8, demands, groups);
        var testOnly = SequenceLatestValidConfig.boundedForTests(8, SequenceLatestValidConfig.SEARCH_STEP_LIMIT, demands, groups);
        var unbounded = SequenceLatestValidConfig.unboundedForTests(demands, groups);
        for (var original : List.of(production, testOnly, unbounded)) {
            var profiled = original.withProfiledPredicates(profiler);
            assertEquals(original.maxCandidates(), profiled.maxCandidates());
            assertEquals(original.searchStepLimit(), profiled.searchStepLimit());
            assertEquals(original.mandatoryDemands(), profiled.mandatoryDemands());
            assertEquals(1, profiled.groups().getFirst().ownStage());
            assertEquals(List.of(0), profiled.groups().getFirst().referencedStages());
            var failure = expectThrows(IllegalStateException.class, () -> profiled.groups().getFirst().predicate().test(null));
            assertEquals("predicate failure", failure.getMessage());
            if (original == production) {
                profiled.validateForProduction(2);
            } else {
                expectThrows(IllegalArgumentException.class, () -> profiled.validateForProduction(2));
            }
        }
        assertEquals(21, profiler.snapshot().conditionNanos());
    }

    public void testProfiledPredicatePreservesResult() {
        var clock = new AtomicLong();
        var profiler = new SequenceRuntimeProfiler(true, 2, clock::get);
        var config = SequenceLatestValidConfig.defaults(
            List.of(List.of(), List.of()),
            List.of(new SequenceLatestValidConfig.PredicateGroup(1, List.of(0), values -> {
                clock.addAndGet(3);
                return true;
            }))
        );
        assertTrue(config.withProfiledPredicates(profiler).groups().getFirst().predicate().test(null));
        assertEquals(3, profiler.snapshot().conditionNanos());
    }
}
