/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceCondition;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;

/** Native scalar evaluators and the real matcher share typed, separately owned one-position candidate values. */
public class SequenceConditionCompilerTests extends ESTestCase {
    /** Returned evaluator blocks are Source-owned even when a failed admission closes them immediately. */
    public void testReturnedBooleanBlockAdmissionFailureReleasesResultAndBorrowedInputs() throws Exception {
        Attribute first = attribute("first", DataType.INTEGER);
        Attribute own = attribute("own", DataType.INTEGER);
        var previous = input(0, first);
        var current = input(1, own);
        var condition = new SequenceCondition(
            Source.EMPTY,
            local(own),
            1,
            new Equals(Source.EMPTY, previous.reference(), current.reference()),
            List.of(0),
            List.of(previous, current)
        );
        Sequence sequence = sequence(local(first), condition);
        SequenceMemoryBudget budget = new SequenceMemoryBudget(100_000);
        try (Resources resources = new Resources("1mb")) {
            CircuitBreaker scoped = budget.newScope().breaker(resources.breaker);
            try (
                var compiler = SequenceConditionCompiler.compile(
                    sequence,
                    16,
                    FoldContext.small(),
                    resources.context,
                    null,
                    scoped,
                    budget
                );
                var left = resources.blocks.newConstantIntBlockWith(4, 1);
                var right = resources.blocks.newConstantIntBlockWith(4, 1)
            ) {
                var compiled = SequenceConditionCompiler.class.getDeclaredField("compiled");
                compiled.setAccessible(true);
                Object group = ((List<?>) compiled.get(compiler)).getFirst();
                var pageBytesField = group.getClass().getDeclaredField("pageBytes");
                pageBytesField.setAccessible(true);
                long filler = 100_000 - budget.currentBytes() - pageBytesField.getLong(group);
                // A synthetic occupied reservation leaves room for the real input Page but no returned BooleanBlock.
                scoped.addEstimateBytesAndMaybeBreak(filler, "occupied-before-condition");
                try {
                    long before = budget.currentBytes();
                    long requestBefore = resources.breaker.getUsed();
                    CircuitBreakingException failure = expectThrows(
                        CircuitBreakingException.class,
                        () -> compiler.config().groups().getFirst().predicate().test((stage, slot) -> stage == 0 ? left : right)
                    );
                    assertThat(failure.getMessage(), containsString("esql.sequence.max_memory"));
                    assertEquals(before, budget.currentBytes());
                    assertEquals(requestBefore, resources.breaker.getUsed());
                } finally {
                    scoped.addWithoutBreaking(-filler);
                }
            }
        }
        assertEquals(0, budget.currentBytes());
    }

    /** F4 4A waits for both earlier rows before evaluating arithmetic over their stored cells. */
    public void testConjunctReadingTwoEarlierStagesUsesBothRowsInTheRealMatcher() {
        Attribute x = attribute("x", DataType.INTEGER);
        Attribute y = attribute("y", DataType.INTEGER);
        Attribute z = attribute("z", DataType.INTEGER);
        var first = input(0, x);
        var second = input(1, y);
        var own = input(2, z);
        var condition = new SequenceCondition(
            Source.EMPTY,
            local(z),
            2,
            new Equals(Source.EMPTY, own.reference(), new Add(Source.EMPTY, first.reference(), second.reference(), EsqlTestUtils.TEST_CFG)),
            List.of(0, 1),
            List.of(first, second, own)
        );
        Sequence sequence = sequence(local(x), local(y), condition);
        try (Resources resources = new Resources("1mb"); var compiler = compile(sequence, resources)) {
            var config = compiler.config();
            assertEquals(List.of(1, 1, 1), config.mandatoryDemands().stream().map(List::size).toList());
            assertEquals(1, config.groups().size());
            assertEquals(0, config.groups().getFirst().minReferencedStage());
            assertEquals(List.of(0, 1), config.groups().getFirst().referencedStages());
            try (
                var matcher = new SequenceLatestValidMatcher(
                    3,
                    TimeValue.timeValueSeconds(10),
                    resources.breaker,
                    () -> false,
                    SequenceKeyCodec::compare,
                    null,
                    config
                )
            ) {
                assertNull(add(matcher, config, resources, 0, 0, "a", 1));
                assertNull(add(matcher, config, resources, 1, 1, "b", 2));
                try (SequenceMatch match = add(matcher, config, resources, 2, 2, "c", 3)) {
                    assertNotNull(match);
                    assertEquals(List.of("a", "b", "c"), match.handles().stream().map(BytesRef::utf8ToString).toList());
                }
            }
        }
    }

    /** F4 4B keeps independent pruning depths instead of compiling a single delayed predicate per stage. */
    public void testSeparateConjunctsKeepTheirOriginalBindingGroups() {
        Attribute early = attribute("b", DataType.INTEGER);
        Attribute near = attribute("a", DataType.INTEGER);
        Attribute ownA = attribute("a", DataType.INTEGER);
        Attribute ownB = attribute("b", DataType.INTEGER);
        var a = input(3, ownA);
        var previous = input(2, near);
        var b = input(3, ownB);
        var first = input(0, early);
        LogicalPlan child = local(ownA, ownB);
        child = new SequenceCondition(
            Source.EMPTY,
            child,
            3,
            new Equals(Source.EMPTY, a.reference(), previous.reference()),
            List.of(2),
            List.of(a, previous)
        );
        child = new SequenceCondition(
            Source.EMPTY,
            child,
            3,
            new Equals(Source.EMPTY, b.reference(), first.reference()),
            List.of(0),
            List.of(b, first)
        );
        try (
            Resources resources = new Resources("1mb");
            var compiler = compile(sequence(local(early), local(), local(near), child), resources)
        ) {
            assertEquals(2, compiler.config().groups().size());
            assertEquals(
                Set.of(0, 2),
                compiler.config().groups().stream().map(group -> group.minReferencedStage()).collect(Collectors.toSet())
            );
            assertEquals(List.of(1, 0, 1, 2), compiler.config().mandatoryDemands().stream().map(List::size).toList());
        }
    }

    /** Lowest-reference grouping distinguishes conjuncts reading {2} and {1, 2}, as numbered in query text. */
    public void testGroupingUsesLowestReferencedStageRatherThanHighest() {
        Attribute first = attribute("x", DataType.INTEGER);
        Attribute second = attribute("y", DataType.INTEGER);
        Attribute own = attribute("z", DataType.INTEGER);
        var near = input(1, second);
        LogicalPlan child = new SequenceCondition(
            Source.EMPTY,
            local(own),
            2,
            new Equals(Source.EMPTY, near.reference(), new Literal(Source.EMPTY, 1, DataType.INTEGER)),
            List.of(1),
            List.of(near)
        );
        var far = input(0, first);
        var otherNear = input(1, second);
        child = new SequenceCondition(
            Source.EMPTY,
            child,
            2,
            new Equals(Source.EMPTY, far.reference(), otherNear.reference()),
            List.of(0, 1),
            List.of(far, otherNear)
        );
        try (Resources resources = new Resources("1mb"); var compiler = compile(sequence(local(first), local(second), child), resources)) {
            assertEquals(2, compiler.config().groups().size());
            assertEquals(
                Set.of(List.of(1), List.of(0, 1)),
                compiler.config()
                    .groups()
                    .stream()
                    .map(SequenceLatestValidConfig.PredicateGroup::referencedStages)
                    .collect(Collectors.toSet())
            );
        }
    }

    /** Scalar folding must not move a conjunct's search boundary or force storage of an eliminated input. */
    public void testFoldedAndOwnOnlyResidualsKeepOriginalReferencedStagesWithoutExtraDemands() {
        Attribute removed = attribute("removed", DataType.BOOLEAN);
        Attribute flag = attribute("flag", DataType.BOOLEAN);
        var earlier = input(0, removed);
        var own = input(1, flag);
        for (Expression residual : List.of(own.reference(), Literal.FALSE, Literal.NULL)) {
            boolean folded = residual != own.reference();
            var condition = new SequenceCondition(Source.EMPTY, local(flag), 1, residual, List.of(0), List.of(earlier, own));
            try (
                Resources resources = new Resources("1mb");
                var compiler = compile(sequence(local(), condition), resources);
                Page page = page(resources, true)
            ) {
                var config = compiler.config();
                assertEquals(List.of(0, folded ? 0 : 1), config.mandatoryDemands().stream().map(List::size).toList());
                assertEquals(List.of(0), config.groups().getFirst().referencedStages());
                assertEquals(0, config.groups().getFirst().minReferencedStage());
                assertEquals(folded == false, config.groups().getFirst().predicate().test((stage, slot) -> {
                    assertEquals(1, stage);
                    assertEquals(0, slot);
                    return page.getBlock(0);
                }));
            }
        }
    }

    public void testOnlySingleNonNullTruePassesAndInputReferencesSurviveRepeatedEvaluation() {
        Attribute flag = attribute("flag", DataType.BOOLEAN);
        var earlier = input(0, flag);
        var condition = new SequenceCondition(Source.EMPTY, local(), 1, earlier.reference(), List.of(0), List.of(earlier));
        try (Resources resources = new Resources("1mb"); var compiler = compile(sequence(local(flag), condition), resources)) {
            var predicate = compiler.config().groups().getFirst().predicate();
            for (Object value : new Object[] { true, false, null, List.of(true, false), List.of(true, true) }) {
                try (Page page = page(resources, value)) {
                    long bytes = resources.breaker.getUsed();
                    for (int repeat = 0; repeat < 3; repeat++) {
                        assertEquals(Boolean.TRUE.equals(value), predicate.test((stage, slot) -> page.getBlock(0)));
                        assertFalse(page.getBlock(0).isReleased());
                        assertEquals(bytes, resources.breaker.getUsed());
                    }
                }
            }
        }
    }

    public void testSharedGroupUsesOneEvaluatorAndOneDemandPerStageValue() {
        Attribute firstFlag = attribute("flag", DataType.BOOLEAN);
        Attribute ownFlag = attribute("flag", DataType.BOOLEAN);
        LogicalPlan child = local(ownFlag);
        for (int conjunct = 0; conjunct < 2; conjunct++) {
            var first = input(0, firstFlag);
            var own = input(1, ownFlag);
            child = new SequenceCondition(
                Source.EMPTY,
                child,
                1,
                new Equals(Source.EMPTY, own.reference(), first.reference()),
                List.of(0),
                List.of(first, own)
            );
        }
        try (
            Resources resources = new Resources("1mb");
            var compiler = compile(sequence(local(firstFlag), child), resources);
            Page page = page(resources, true, false)
        ) {
            assertEquals(1, compiler.config().groups().size());
            assertEquals(List.of(1, 1), compiler.config().mandatoryDemands().stream().map(List::size).toList());
            assertFalse(compiler.config().groups().getFirst().predicate().test((stage, slot) -> page.getBlock(stage)));
            assertFalse(page.getBlock(0).isReleased());
            assertFalse(page.getBlock(1).isReleased());
        }
    }

    public void testNativeMultivalueFunctionReadsCompleteTypedEarlierValue() {
        Attribute many = attribute("value", DataType.INTEGER);
        Attribute ownValue = attribute("value", DataType.INTEGER);
        var earlier = input(0, many);
        var own = input(1, ownValue);
        var condition = new SequenceCondition(
            Source.EMPTY,
            local(ownValue),
            1,
            new MvContains(Source.EMPTY, earlier.reference(), own.reference()),
            List.of(0),
            List.of(earlier, own)
        );
        try (
            Resources resources = new Resources("1mb");
            var compiler = compile(sequence(local(many), condition), resources);
            Page page = page(resources, List.of(1, 2), 2)
        ) {
            assertTrue(compiler.config().groups().getFirst().predicate().test((stage, slot) -> page.getBlock(stage)));
            assertEquals(2, page.getBlock(0).getValueCount(0));
            assertFalse(page.getBlock(0).isReleased());
        }
    }

    public void testWarningsHaveOneDedicatedBudgetAcrossRepeatedEvaluations() {
        Source source = new Source(1, 0, "TO_INTEGER(MEMBER(1, text)) == 1");
        Attribute text = attribute("text", DataType.KEYWORD);
        var earlier = input(0, text);
        var condition = new SequenceCondition(
            source,
            local(),
            1,
            new Equals(source, new ToInteger(source, earlier.reference()), new Literal(Source.EMPTY, 1, DataType.INTEGER)),
            List.of(0),
            List.of(earlier)
        );
        try (Resources resources = new Resources("1mb")) {
            resources.context.setWarningsBudget(new Warnings.Budget());
            try (var compiler = compile(sequence(local(text), condition), resources)) {
                for (int row = 0; row < 100; row++) {
                    try (Page page = page(resources, new BytesRef("bad-" + row))) {
                        assertFalse(compiler.config().groups().getFirst().predicate().test((stage, slot) -> page.getBlock(0)));
                    }
                }
            }
            resources.context.finish();
            assertEquals(21, resources.context.warnings().size());
            assertThat(resources.context.warnings().getFirst(), containsString("Only first 20 failures recorded"));
        }
    }

    public void testCancellationAndIncompleteInputPageReleaseEveryBorrowedReference() {
        Attribute firstFlag = attribute("first", DataType.BOOLEAN);
        Attribute ownFlag = attribute("own", DataType.BOOLEAN);
        var first = input(0, firstFlag);
        var own = input(1, ownFlag);
        var condition = new SequenceCondition(
            Source.EMPTY,
            local(ownFlag),
            1,
            new And(Source.EMPTY, first.reference(), own.reference()),
            List.of(0),
            List.of(first, own)
        );
        try (
            Resources resources = new Resources("1mb");
            var compiler = compile(sequence(local(firstFlag), condition), resources);
            Page page = page(resources, true)
        ) {
            long bytes = resources.breaker.getUsed();
            var predicate = compiler.config().groups().getFirst().predicate();
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> predicate.test((stage, slot) -> {
                if (stage == 1) {
                    throw new IllegalStateException("controlled missing own row");
                }
                return page.getBlock(0);
            }));
            assertEquals("controlled missing own row", error.getMessage());
            assertFalse(page.getBlock(0).isReleased());
            assertEquals(bytes, resources.breaker.getUsed());
            resources.context.initializeEarlyTerminationChecker(
                () -> { throw new TaskCancelledException("controlled condition cancellation"); }
            );
            expectThrows(TaskCancelledException.class, () -> predicate.test((stage, slot) -> page.getBlock(0)));
            assertEquals(bytes, resources.breaker.getUsed());
        }
    }

    public void testConstructionAdmissionFailureAndTypedInputFailureReleaseAllAccounting() {
        Attribute flag = attribute("flag", DataType.BOOLEAN);
        var earlier = input(0, flag);
        var condition = new SequenceCondition(Source.EMPTY, local(), 1, earlier.reference(), List.of(0), List.of(earlier));
        Sequence sequence = sequence(local(flag), condition);
        for (String limit : List.of("1b", "3kb")) {
            try (Resources resources = new Resources(limit)) {
                expectThrows(CircuitBreakingException.class, () -> compile(sequence, resources));
                assertEquals(0, resources.breaker.getUsed());
            }
        }
        try (Resources resources = new Resources("1mb"); var compiler = compile(sequence, resources); Page page = page(resources, 17)) {
            long bytes = resources.breaker.getUsed();
            expectThrows(
                IllegalStateException.class,
                () -> compiler.config().groups().getFirst().predicate().test((stage, slot) -> page.getBlock(0))
            );
            assertFalse(page.getBlock(0).isReleased());
            assertEquals(bytes, resources.breaker.getUsed());
        }
    }

    private static SequenceConditionCompiler compile(Sequence sequence, Resources resources) {
        return SequenceConditionCompiler.compile(sequence, 16, FoldContext.small(), resources.context, null);
    }

    private static Attribute attribute(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    private static SequenceCondition.Input input(int stage, Attribute source) {
        return new SequenceCondition.Input(
            new ReferenceAttribute(Source.EMPTY, null, source.name(), source.dataType(), Nullability.TRUE, new NameId(), true),
            stage,
            source
        );
    }

    private static LocalRelation local(Attribute... output) {
        return new LocalRelation(Source.EMPTY, List.of(output), EmptyLocalSupplier.EMPTY);
    }

    private static Sequence sequence(LogicalPlan... children) {
        return new Sequence(
            Source.EMPTY,
            java.util.Arrays.stream(children).map(child -> (LogicalPlan) new SequenceStage(Source.EMPTY, child, List.of())).toList(),
            TimeValue.timeValueSeconds(10).nanos()
        );
    }

    private static Page page(Resources resources, Object... values) {
        return new Page(1, BlockUtils.fromArrayRow(resources.blocks, values));
    }

    private static SequenceMatch add(
        SequenceLatestValidMatcher matcher,
        SequenceLatestValidConfig config,
        Resources resources,
        int stage,
        int time,
        String handle,
        Object value
    ) {
        try (Page page = page(resources, value)) {
            SequenceCandidateValues values = SequenceCandidateValues.copyRow(
                page,
                0,
                config.mandatoryDemands().get(stage),
                new int[] { 0 },
                resources.blocks,
                resources.breaker
            );
            return matcher.match(stage, new BytesRef("k"), new SequenceOrdinal(time, 0, time), new BytesRef(handle), values);
        }
    }

    private static final class Resources implements Releasable {
        private final CircuitBreaker breaker;
        private final MockBlockFactory blocks;
        private final DriverContext context;

        private Resources(String limit) {
            Settings settings = Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build();
            breaker = new HierarchyCircuitBreakerService(
                CircuitBreakerMetrics.NOOP,
                settings,
                List.of(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            ).getBreaker(CircuitBreaker.REQUEST);
            blocks = new MockBlockFactory(BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker));
            context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blocks, null);
        }

        @Override
        public void close() {
            context.finish();
            context.getSnapshot().close();
            assertEquals(0, breaker.getUsed());
            blocks.ensureAllBlocksAreReleased();
        }
    }
}
