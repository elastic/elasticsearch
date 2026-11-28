/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@link FilterOperator} end to end (predicate evaluation plus row selection) for boolean
 * predicates, so that both the {@code AND}/{@code OR} evaluator and the all-true/all-false shortcut in
 * {@link FilterOperator} show up. Each page also carries a {@code long} payload column that is filtered.
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class FilterBenchmark {
    // Must run before BlockFactory.<clinit>, which logs and NPEs without a configured logging provider.
    static {
        BenchmarkLogging.configure();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    private static final int BLOCK_LENGTH = 8 * 1024;

    static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    public enum Operation {
        /** A single boolean column, roughly half {@code true} at pseudo-random positions. */
        MIXED_50,
        /** A single boolean column that is all {@code true}. */
        ALL_TRUE,
        /** A single boolean column that is all {@code false}. */
        ALL_FALSE,
        /** {@code lhs AND rhs} over two boolean columns; {@code true} on every sixth row. */
        AND_BOOLEAN,
        /** {@code OR} of 16 {@code a == c AND b == c}, none of which match, so every page is dropped. */
        OR_OF_ANDS_16
    }

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            // Smoke test all the expected values and force loading subclasses more like prod
            selfTest();
        }
    }

    static void selfTest() {
        Logger log = LogManager.getLogger(FilterBenchmark.class);
        for (Operation operation : Operation.values()) {
            log.info("self testing {}", operation);
            run(operation);
        }
    }

    @Param
    public Operation operation;

    private static ExpressionEvaluator evaluator(Operation operation) {
        return switch (operation) {
            case MIXED_50, ALL_TRUE, ALL_FALSE -> {
                FieldAttribute test = booleanField("test");
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, test, layout(test)).get(driverContext);
            }
            case AND_BOOLEAN -> {
                FieldAttribute lhs = booleanField("lhs");
                FieldAttribute rhs = booleanField("rhs");
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new And(Source.EMPTY, lhs, rhs), layout(lhs, rhs)).get(driverContext);
            }
            case OR_OF_ANDS_16 -> {
                FieldAttribute a = longField("a");
                FieldAttribute b = longField("b");
                List<Expression> ands = new ArrayList<>(16);
                for (int i = 0; i < 16; i++) {
                    Literal v = new Literal(Source.EMPTY, -1L - i, DataType.LONG);
                    ands.add(new And(Source.EMPTY, new Equals(Source.EMPTY, a, v), new Equals(Source.EMPTY, b, v)));
                }
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, Predicates.combineOr(ands), layout(a, b)).get(driverContext);
            }
        };
    }

    /**
     * The predicate columns come first (matching the evaluator layout), followed by a {@code long} payload column.
     */
    private static Page page(Operation operation) {
        return switch (operation) {
            case MIXED_50 -> {
                Random random = new Random(42);
                boolean[] values = new boolean[BLOCK_LENGTH];
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    values[i] = random.nextBoolean();
                }
                yield new Page(blockFactory.newBooleanArrayVector(values, BLOCK_LENGTH).asBlock(), payload());
            }
            case ALL_TRUE, ALL_FALSE -> {
                boolean[] values = new boolean[BLOCK_LENGTH];
                Arrays.fill(values, operation == Operation.ALL_TRUE);
                yield new Page(blockFactory.newBooleanArrayVector(values, BLOCK_LENGTH).asBlock(), payload());
            }
            case AND_BOOLEAN -> {
                var lhs = blockFactory.newBooleanVectorFixedBuilder(BLOCK_LENGTH);
                var rhs = blockFactory.newBooleanVectorFixedBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    lhs.appendBoolean(i, (i & 1) == 0);
                    rhs.appendBoolean(i, i % 3 == 0);
                }
                yield new Page(lhs.build().asBlock(), rhs.build().asBlock(), payload());
            }
            case OR_OF_ANDS_16 -> {
                var a = blockFactory.newLongVectorFixedBuilder(BLOCK_LENGTH);
                var b = blockFactory.newLongVectorFixedBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    a.appendLong(i, i);
                    b.appendLong(i, i);
                }
                yield new Page(a.build().asBlock(), b.build().asBlock(), payload());
            }
        };
    }

    private static LongBlock payload() {
        var payload = blockFactory.newLongVectorFixedBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            payload.appendLong(i, i);
        }
        return payload.build().asBlock();
    }

    private static int expectedPositions(Operation operation, Page input) {
        return switch (operation) {
            case MIXED_50 -> {
                var test = input.<BooleanBlock>getBlock(0).asVector();
                int count = 0;
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (test.getBoolean(i)) {
                        count++;
                    }
                }
                yield count;
            }
            case ALL_TRUE -> BLOCK_LENGTH;
            case ALL_FALSE, OR_OF_ANDS_16 -> 0;
            case AND_BOOLEAN -> (BLOCK_LENGTH + 5) / 6;
        };
    }

    private static void checkExpected(Operation operation, Page input, Page output) {
        int expected = expectedPositions(operation, input);
        int actual = output == null ? 0 : output.getPositionCount();
        if (actual != expected) {
            throw new AssertionError("[" + operation + "] expected [" + expected + "] positions but was [" + actual + "]");
        }
        if (output != null) {
            LongBlock payload = output.getBlock(output.getBlockCount() - 1);
            for (int p = 1; p < payload.getPositionCount(); p++) {
                if (payload.getLong(p) <= payload.getLong(p - 1)) {
                    throw new AssertionError("[" + operation + "] payload out of order at [" + p + "]");
                }
            }
        }
    }

    private static FieldAttribute booleanField(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.BOOLEAN, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static FieldAttribute longField(String name) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static Layout layout(FieldAttribute... fields) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(Arrays.asList(fields));
        return layout.build();
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void run(Blackhole bh) {
        bh.consume(run(operation));
    }

    private static int run(Operation operation) {
        Page page = page(operation);
        try (var operator = new FilterOperator(evaluator(operation))) {
            Page output = null;
            int selected = 0;
            for (int i = 0; i < 1024; i++) {
                if (output != null) {
                    output.releaseBlocks();
                }
                // FilterOperator releases its input, so hand it a shallow copy that holds its own references.
                operator.addInput(page.shallowCopy());
                output = operator.getOutput();
                selected += output == null ? 0 : output.getPositionCount();
            }
            // We only check the last one
            checkExpected(operation, page, output);
            if (output != null) {
                output.releaseBlocks();
            }
            return selected;
        } finally {
            page.releaseBlocks();
        }
    }
}
