/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceCondition;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.SequenceExec;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Qualification-only fault checks use real compiler, source admission, contexts and memory accounting. */
public class SequenceCleanupFailureTests extends ESTestCase {
    public void testCompileFailureKeepsPrimaryWhenEarlierEvaluatorCleanupFails() {
        var primary = new IllegalArgumentException("primary compile failure");
        var secondary = new IllegalStateException("secondary evaluator close failure");
        AtomicInteger closes = new AtomicInteger();
        Expression accepted = expression(context -> evaluator(closes, null, secondary));
        Expression rejected = expression(context -> { throw primary; });
        var sequence = sequence(local(), condition(1, accepted), condition(2, rejected));
        try (Resources resources = new Resources()) {
            Exception failure = expectThrows(
                IllegalArgumentException.class,
                () -> SequenceConditionCompiler.compile(sequence, 8, FoldContext.small(), resources.context, null)
            );
            assertSame(primary, failure);
            assertEquals(1, failure.getSuppressed().length);
            assertSame(secondary, failure.getSuppressed()[0]);
            assertEquals(1, closes.get());
            assertEquals(0, resources.breaker.getUsed());
        }
    }

    public void testEvaluatorAdmissionKeepsPrimaryWhenUnacceptedEvaluatorCleanupFails() {
        var primary = new IllegalArgumentException("primary evaluator accounting failure");
        var secondary = new IllegalStateException("secondary unaccepted evaluator close failure");
        AtomicInteger closes = new AtomicInteger();
        var sequence = sequence(local(), condition(1, expression(context -> evaluator(closes, primary, secondary))));
        try (Resources resources = new Resources()) {
            Exception failure = expectThrows(
                IllegalArgumentException.class,
                () -> SequenceConditionCompiler.compile(sequence, 8, FoldContext.small(), resources.context, null)
            );
            assertSame(primary, failure);
            assertEquals(1, failure.getSuppressed().length);
            assertSame(secondary, failure.getSuppressed()[0]);
            assertEquals(1, closes.get());
            assertEquals(0, resources.breaker.getUsed());
        }
    }

    public void testFactoryKeepsPrimaryCancellationAndSuppressesCompilerCleanup() {
        var secondary = new IllegalStateException("secondary factory evaluator close failure");
        AtomicInteger closes = new AtomicInteger();
        var sequence = sequence(local(), condition(1, expression(context -> evaluator(closes, null, secondary)))).resolveSchema();
        assertTrue("factory fixture must reach source admission", sequence.resolved());
        CancellableTask task = new CancellableTask(1, "test", "sequence", "cleanup", TaskId.EMPTY_TASK_ID, Map.of());
        TaskCancelHelper.cancel(task, "controlled constructor cancellation");
        try (Resources resources = new Resources()) {
            var factory = new SequenceSourceOperator.Factory(
                new SequenceExec(Source.EMPTY, sequence),
                null,
                null,
                task,
                EsqlTestUtils.TEST_CFG,
                null,
                null,
                null,
                null
            );
            Exception failure = expectThrows(TaskCancelledException.class, () -> factory.getWithConditions(resources.context, 8, null));
            assertEquals(1, failure.getSuppressed().length);
            assertSame(secondary, failure.getSuppressed()[0]);
            assertEquals(1, closes.get());
            assertEquals(0, resources.breaker.getUsed());
        }
    }

    // Native evaluators do not deliberately fail close; this literal substitutes only the faulting evaluator factory.
    private static Expression expression(ExpressionEvaluator.Factory factory) {
        return new Literal(Source.EMPTY, true, DataType.BOOLEAN) {
            @Override
            public ExpressionEvaluator.Factory toEvaluator(EvaluatorMapper.ToEvaluator context) {
                return factory;
            }
        };
    }

    private static ExpressionEvaluator evaluator(AtomicInteger closes, RuntimeException accounting, RuntimeException cleanup) {
        return new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                throw new AssertionError("cleanup qualification does not evaluate rows");
            }

            @Override
            public long baseRamBytesUsed() {
                if (accounting != null) {
                    throw accounting;
                }
                return 0;
            }

            @Override
            public void close() {
                closes.incrementAndGet();
                throw cleanup;
            }
        };
    }

    private static SequenceCondition condition(int stage, Expression expression) {
        return new SequenceCondition(Source.EMPTY, local(), stage, expression, List.of(0), List.of());
    }

    private static LocalRelation local() {
        return new LocalRelation(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
    }

    private static Sequence sequence(LogicalPlan... stages) {
        return new Sequence(
            Source.EMPTY,
            java.util.Arrays.stream(stages).map(child -> (LogicalPlan) new SequenceStage(Source.EMPTY, child, List.of())).toList(),
            10_000_000
        );
    }

    private static final class Resources implements Releasable {
        private final CircuitBreaker breaker;
        private final MockBlockFactory blocks;
        private final DriverContext context;

        private Resources() {
            Settings settings = Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "1mb")
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
