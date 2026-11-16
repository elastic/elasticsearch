/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

/** Mandatory cells keep their complete typed position even after the input page is released. */
public class SequenceCandidateValuesTests extends ESTestCase {
    /** Fresh retained cells outlive their input and join their container in one simultaneous held estimate. */
    public void testFreshCellsShareTheAlwaysActiveBudget() {
        CircuitBreaker breaker = breaker("1mb");
        MockBlockFactory blocks = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
        SequenceMemoryBudget memory = new SequenceMemoryBudget(1_000_000);
        CircuitBreaker explicit = memory.newScope().breaker(breaker);
        SequenceCandidateValues values;
        try (
            Page source = new Page(
                1,
                BlockUtils.fromArrayRow(TestBlockFactory.getNonBreakingInstance(), new Object[] { List.of(4, 5), null })
            )
        ) {
            values = SequenceCandidateValues.copyRow(
                source,
                0,
                List.of(demand("many"), demand("null")),
                new int[] { 0, 1 },
                memory.newScope().blockFactory(blocks),
                explicit
            );
            assertNotSame(source.getBlock(0), values.block(0));
            assertEquals(blocks.maxPrimitiveArrayBytes(), values.block(0).blockFactory().maxPrimitiveArrayBytes());
        }
        long expected = values.containerBytes() + values.block(0).ramBytesUsed() + values.block(1).ramBytesUsed();
        assertEquals(expected, memory.currentBytes());
        assertTrue(memory.peakBytes() >= expected);

        values.close();
        values.close();
        assertEquals(0, memory.currentBytes());

        assertEquals(0, breaker.getUsed());
        blocks.ensureAllBlocksAreReleased();
    }

    /** Both early admission failure and a later invalid channel leave no copied-cell ownership behind. */
    public void testPartialCopyAndContainerFailureRefundTheBudget() {
        for (String limit : List.of("1mb", "1b")) {
            CircuitBreaker breaker = breaker(limit);
            MockBlockFactory blocks = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
            SequenceMemoryBudget memory = new SequenceMemoryBudget(1_000_000);
            CircuitBreaker explicit = memory.newScope().breaker(breaker);
            try (Page source = new Page(1, BlockUtils.fromArrayRow(TestBlockFactory.getNonBreakingInstance(), new Object[] { 4, 5 }))) {
                Class<? extends Exception> failure = limit.equals("1b") ? CircuitBreakingException.class : IllegalArgumentException.class;
                expectThrows(
                    failure,
                    () -> SequenceCandidateValues.copyRow(
                        source,
                        0,
                        List.of(demand("a"), demand("b")),
                        new int[] { 0, -1 },
                        memory.newScope().blockFactory(blocks),
                        explicit
                    )
                );
                assertEquals(0, memory.currentBytes());

                assertEquals(0, breaker.getUsed());
                assertEquals(limit.equals("1b"), memory.peakBytes() == 0);
            }
            blocks.ensureAllBlocksAreReleased();
        }
    }

    /** Repeated copies use one owner layout, admission size and supplied factory. */
    public void testCopiesUseOneOwnerAndTheSuppliedFactory() {
        CircuitBreaker breaker = breaker("1mb");
        MockBlockFactory blocks = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
        try (Page source = new Page(1, BlockUtils.fromArrayRow(TestBlockFactory.getNonBreakingInstance(), new Object[] { 4 }))) {
            long container;
            try (var values = SequenceCandidateValues.copyRow(source, 0, List.of(demand("a")), new int[] { 0 }, blocks, breaker)) {
                assertEquals(SequenceCandidateValues.class, values.getClass());
                assertSame(blocks, values.block(0).blockFactory());
                container = values.containerBytes();
            }
            try (var values = SequenceCandidateValues.copyRow(source, 0, List.of(demand("a")), new int[] { 0 }, blocks, breaker)) {
                assertEquals(SequenceCandidateValues.class, values.getClass());
                assertEquals(container, values.containerBytes());
                assertSame(blocks, values.block(0).blockFactory());
            }
        }
        assertEquals(0, breaker.getUsed());
        blocks.ensureAllBlocksAreReleased();
    }

    public void testOwnedNullAndMultivaluedCellsOutliveInputAndReleaseExactlyOnce() {
        CircuitBreaker breaker = breaker("1mb");
        BlockFactory blocks = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
        SequenceCandidateValues values;
        try (
            Page source = new Page(
                1,
                BlockUtils.fromArrayRow(TestBlockFactory.getNonBreakingInstance(), new Object[] { List.of(4, 5), null })
            )
        ) {
            values = SequenceCandidateValues.copyRow(
                source,
                0,
                List.of(demand("many"), demand("null")),
                new int[] { 0, 1 },
                blocks,
                breaker
            );
        }
        try (values) {
            assertEquals(2, values.size());
            IntBlock many = (IntBlock) values.block(0);
            assertEquals(1, many.getPositionCount());
            assertEquals(2, many.getValueCount(0));
            int first = many.getFirstValueIndex(0);
            assertEquals(4, many.getInt(first));
            assertEquals(5, many.getInt(first + 1));
            assertTrue(values.block(1).isNull(0));
            assertTrue(breaker.getUsed() > values.containerBytes());
        }
        values.close();
        expectThrows(IllegalStateException.class, values::size);
        assertEquals(0, breaker.getUsed());
    }

    public void testPartialCopyAndContainerAdmissionFailureReleaseEverything() {
        for (String limit : List.of("1mb", "1b")) {
            CircuitBreaker breaker = breaker(limit);
            BlockFactory blocks = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
            try (Page source = new Page(1, BlockUtils.fromArrayRow(TestBlockFactory.getNonBreakingInstance(), new Object[] { 4, 5 }))) {
                if (limit.equals("1b")) {
                    expectThrows(
                        CircuitBreakingException.class,
                        () -> SequenceCandidateValues.copyRow(
                            source,
                            0,
                            List.of(demand("a"), demand("b")),
                            new int[] { 0, 1 },
                            blocks,
                            breaker
                        )
                    );
                } else {
                    expectThrows(
                        IllegalArgumentException.class,
                        () -> SequenceCandidateValues.copyRow(
                            source,
                            0,
                            List.of(demand("a"), demand("b")),
                            new int[] { 0, -1 },
                            blocks,
                            breaker
                        )
                    );
                }
                assertEquals(0, breaker.getUsed());
            }
        }
    }

    public void testConfigurationValidatesBoundsAndGroupedReferences() {
        List<List<SequenceLatestValidConfig.ValueDemand>> demands = List.of(List.of(), List.of());
        assertEquals(8, SequenceLatestValidConfig.defaults(demands, List.of()).maxCandidates().orElseThrow());
        assertTrue(SequenceLatestValidConfig.unboundedForTests(demands, List.of()).maxCandidates().isEmpty());
        expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.bounded(0, demands, List.of()));
        expectThrows(IllegalArgumentException.class, () -> new SequenceLatestValidConfig.PredicateGroup(1, List.of(), rows -> true));
        expectThrows(IllegalArgumentException.class, () -> new SequenceLatestValidConfig.PredicateGroup(2, List.of(1, 0), rows -> true));
        expectThrows(IllegalArgumentException.class, () -> new SequenceLatestValidConfig.PredicateGroup(1, List.of(1), rows -> true));
        var group = new SequenceLatestValidConfig.PredicateGroup(2, List.of(0, 1), rows -> true);
        assertEquals(0, group.minReferencedStage());
        expectThrows(IllegalArgumentException.class, () -> SequenceLatestValidConfig.defaults(demands, List.of(group)));
        var duplicate = demand("a");
        expectThrows(
            IllegalArgumentException.class,
            () -> SequenceLatestValidConfig.defaults(List.of(List.of(duplicate, duplicate), List.of()), List.of())
        );
    }

    private static SequenceLatestValidConfig.ValueDemand demand(String name) {
        return new SequenceLatestValidConfig.ValueDemand(new ReferenceAttribute(Source.EMPTY, null, name, DataType.INTEGER));
    }

    private static CircuitBreaker breaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }
}
