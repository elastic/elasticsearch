/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceCondition;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

/** Keeps the 10,000-row matcher counter and ownership checks in the ordinary suite. */
public class SequenceRememberedArrivalCostTests extends ESTestCase {
    /** The legacy path checks only the final eight rows, while Ki also checks each arrival. */
    public void testTenThousandArrivalChecks() {
        for (boolean experimental : List.of(false, true)) {
            runPoint(10_000, experimental);
        }
    }

    private void runPoint(int rows, boolean experimental) {
        Settings settings = Settings.builder()
            .put("indices.breaker.request.limit", "32mb")
            .put("indices.breaker.total.use_real_memory", false)
            .build();
        CircuitBreaker breaker = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
        SequenceMemoryBudget budget = new SequenceMemoryBudget(20_000_000);
        var scope = budget.newScope();
        CircuitBreaker scoped = scope.breaker(breaker);
        MockBlockFactory realBlocks = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
        BlockFactory ownedBlocks = scope.blockFactory(realBlocks);
        DriverContext context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, realBlocks, null);
        try (var compiler = SequenceConditionCompiler.compile(sequence(), 8, FoldContext.small(), context, null, scoped, budget)) {
            var config = compiler.config().withRememberedChains(experimental);
            try (
                var matcher = new SequenceLatestValidMatcher(
                    3,
                    TimeValue.timeValueHours(1),
                    scoped,
                    () -> false,
                    BytesRef::compareTo,
                    null,
                    config
                )
            ) {
                add(matcher, config, ownedBlocks, scoped, 0, 0, 1);
                for (int row = 1; row <= rows; row++) {
                    add(matcher, config, ownedBlocks, scoped, 1, row, 1);
                }
                add(matcher, config, ownedBlocks, scoped, 2, rows + 1, 2);
                var counters = matcher.latestValidCounters();
                var admission = matcher.admissionCounters();
                assertEquals(8L, counters.searchSteps());
                assertEquals(8L, counters.maxSearchStepsPerFinalRow());
                assertEquals(experimental ? rows + 8L : 8L, counters.conditionEvaluations());
                assertEquals(experimental ? rows : 0L, admission.conditionEvaluationsOnArrival());
                assertEquals(experimental ? rows : 0L, admission.arrivalSearchSteps());
                assertEquals(experimental ? 9L : 0L, admission.peakCandidatesKeptPerKey());
                assertEquals(rows - 8L, counters.candidatesDroppedAtBound());
                assertEquals(0L, admission.candidatesNotKept());
                assertEquals(0L, counters.searchesAbandonedAtWorkLimit());
                assertEquals(9, matcher.retainedCandidates());
            }
        } finally {
            context.finish();
            context.getSnapshot().close();
            realBlocks.ensureAllBlocksAreReleased();
            assertEquals(0L, budget.currentBytes());
            assertEquals(0L, breaker.getUsed());
        }
    }

    private static void add(
        SequenceLatestValidMatcher matcher,
        SequenceLatestValidConfig config,
        BlockFactory blocks,
        CircuitBreaker breaker,
        int stage,
        int tick,
        int value
    ) {
        try (Page page = new Page(blocks.newConstantIntBlockWith(value, 1))) {
            SequenceCandidateValues cells = SequenceCandidateValues.copyRow(
                page,
                0,
                config.mandatoryDemands().get(stage),
                new int[] { 0 },
                blocks,
                breaker
            );
            try (
                var match = matcher.match(
                    stage,
                    new BytesRef("k"),
                    new SequenceOrdinal(tick, 0, tick),
                    new BytesRef("event-" + tick),
                    cells
                )
            ) {
                assertNull(match);
            }
        }
    }

    private static Sequence sequence() {
        Attribute a = new ReferenceAttribute(Source.EMPTY, null, "a", DataType.INTEGER);
        Attribute b = new ReferenceAttribute(Source.EMPTY, null, "b", DataType.INTEGER);
        Attribute c = new ReferenceAttribute(Source.EMPTY, null, "c", DataType.INTEGER);
        var first = input(0, a);
        var middle = input(1, b);
        var last = input(2, c);
        // Middle equality passes. Final equality fails as soon as the middle stage is bound, before stage zero.
        LogicalPlan second = new SequenceCondition(
            Source.EMPTY,
            local(b),
            1,
            new Equals(Source.EMPTY, first.reference(), middle.reference()),
            List.of(0),
            List.of(first, middle)
        );
        LogicalPlan third = new SequenceCondition(
            Source.EMPTY,
            local(c),
            2,
            new Equals(Source.EMPTY, middle.reference(), last.reference()),
            List.of(1),
            List.of(middle, last)
        );
        return new Sequence(
            Source.EMPTY,
            List.of(
                new SequenceStage(Source.EMPTY, local(a), List.of()),
                new SequenceStage(Source.EMPTY, second, List.of()),
                new SequenceStage(Source.EMPTY, third, List.of())
            ),
            TimeValue.timeValueHours(1).nanos()
        );
    }

    private static SequenceCondition.Input input(int stage, Attribute source) {
        return new SequenceCondition.Input(
            new ReferenceAttribute(Source.EMPTY, null, source.name(), source.dataType(), Nullability.TRUE, new NameId(), true),
            stage,
            source
        );
    }

    private static LocalRelation local(Attribute attribute) {
        return new LocalRelation(Source.EMPTY, List.of(attribute), EmptyLocalSupplier.EMPTY);
    }
}
