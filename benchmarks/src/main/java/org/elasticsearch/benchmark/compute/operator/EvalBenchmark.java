/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class EvalBenchmark {
    private static final BlockFactory blockFactory = BlockFactory.getInstance(
        new NoopCircuitBreaker("noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    private static final int BLOCK_LENGTH = 8 * 1024;

    static final DriverContext driverContext = new DriverContext(
        BigArrays.NON_RECYCLING_INSTANCE,
        BlockFactory.getInstance(new NoopCircuitBreaker("noop"), BigArrays.NON_RECYCLING_INSTANCE)
    );

    static {
        // Smoke test all the expected values and force loading subclasses more like prod
        try {
            for (String operation : EvalBenchmark.class.getField("operation").getAnnotationsByType(Param.class)[0].value()) {
                run(operation);
            }
        } catch (NoSuchFieldException e) {
            throw new AssertionError();
        }
    }

    @Param(
        {
            "abs",
            "add",
            "add_double",
            "case_1_eager",
            "case_1_lazy",
            "date_trunc",
            "equal_to_const",
            "long_equal_to_long",
            "long_equal_to_int",
            "mv_min",
            "mv_min_ascending",
            "rlike" }
    )
    public String operation;

    private static Operator operator(String operation) {
        return new EvalOperator(driverContext.blockFactory(), evaluator(operation));
    }

    private static EvalOperator.ExpressionEvaluator evaluator(String operation) {
        return switch (operation) {
            case "abs" -> {
                FieldAttribute longField = longField();
                yield EvalMapper.toEvaluator(new Abs(Source.EMPTY, longField), layout(longField)).get(driverContext);
            }
            case "add" -> {
                FieldAttribute longField = longField();
                yield EvalMapper.toEvaluator(
                    new Add(Source.EMPTY, longField, new Literal(Source.EMPTY, 1L, DataType.LONG)),
                    layout(longField)
                ).get(driverContext);
            }
            case "add_double" -> {
                FieldAttribute doubleField = doubleField();
                yield EvalMapper.toEvaluator(
                    new Add(Source.EMPTY, doubleField, new Literal(Source.EMPTY, 1D, DataType.DOUBLE)),
                    layout(doubleField)
                ).get(driverContext);
            }
            case "case_1_eager", "case_1_lazy" -> {
                FieldAttribute f1 = longField();
                FieldAttribute f2 = longField();
                Expression condition = new Equals(Source.EMPTY, f1, new Literal(Source.EMPTY, 1L, DataType.LONG));
                Expression lhs = f1;
                Expression rhs = f2;
                if (operation.endsWith("lazy")) {
                    lhs = new Add(Source.EMPTY, lhs, new Literal(Source.EMPTY, 1L, DataType.LONG));
                    rhs = new Add(Source.EMPTY, rhs, new Literal(Source.EMPTY, 1L, DataType.LONG));
                }
                yield EvalMapper.toEvaluator(new Case(Source.EMPTY, condition, List.of(lhs, rhs)), layout(f1, f2)).get(driverContext);
            }
            case "date_trunc" -> {
                FieldAttribute timestamp = new FieldAttribute(
                    Source.EMPTY,
                    "timestamp",
                    new EsField("timestamp", DataType.DATETIME, Map.of(), true)
                );
                yield EvalMapper.toEvaluator(
                    new DateTrunc(Source.EMPTY, new Literal(Source.EMPTY, Duration.ofHours(24), DataType.TIME_DURATION), timestamp),
                    layout(timestamp)
                ).get(driverContext);
            }
            case "equal_to_const" -> {
                FieldAttribute longField = longField();
                yield EvalMapper.toEvaluator(
                    new Equals(Source.EMPTY, longField, new Literal(Source.EMPTY, 100_000L, DataType.LONG)),
                    layout(longField)
                ).get(driverContext);
            }
            case "long_equal_to_long" -> {
                FieldAttribute lhs = longField();
                FieldAttribute rhs = longField();
                yield EvalMapper.toEvaluator(new Equals(Source.EMPTY, lhs, rhs), layout(lhs, rhs)).get(driverContext);
            }
            case "long_equal_to_int" -> {
                FieldAttribute lhs = longField();
                FieldAttribute rhs = intField();
                yield EvalMapper.toEvaluator(new Equals(Source.EMPTY, lhs, rhs), layout(lhs, rhs)).get(driverContext);
            }
            case "mv_min", "mv_min_ascending" -> {
                FieldAttribute longField = longField();
                yield EvalMapper.toEvaluator(new MvMin(Source.EMPTY, longField), layout(longField)).get(driverContext);
            }
            case "rlike" -> {
                FieldAttribute keywordField = keywordField();
                RLike rlike = new RLike(Source.EMPTY, keywordField, new RLikePattern(".ar"));
                yield EvalMapper.toEvaluator(rlike, layout(keywordField)).get(driverContext);
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    private static FieldAttribute longField() {
        return new FieldAttribute(Source.EMPTY, "long", new EsField("long", DataType.LONG, Map.of(), true));
    }

    private static FieldAttribute doubleField() {
        return new FieldAttribute(Source.EMPTY, "double", new EsField("double", DataType.DOUBLE, Map.of(), true));
    }

    private static FieldAttribute intField() {
        return new FieldAttribute(Source.EMPTY, "int", new EsField("int", DataType.INTEGER, Map.of(), true));
    }

    private static FieldAttribute keywordField() {
        return new FieldAttribute(Source.EMPTY, "keyword", new EsField("keyword", DataType.KEYWORD, Map.of(), true));
    }

    private static Layout layout(FieldAttribute... fields) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(Arrays.asList(fields));
        return layout.build();
    }

    private static void checkExpected(String operation, Page actual) {
        switch (operation) {
            case "abs" -> {
                LongVector v = actual.<LongBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (v.getLong(i) != i * 100_000) {
                        throw new AssertionError("[" + operation + "] expected [" + (i * 100_000) + "] but was [" + v.getLong(i) + "]");
                    }
                }
            }
            case "add" -> {
                LongVector v = actual.<LongBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (v.getLong(i) != i * 100_000 + 1) {
                        throw new AssertionError("[" + operation + "] expected [" + (i * 100_000 + 1) + "] but was [" + v.getLong(i) + "]");
                    }
                }
            }
            case "add_double" -> {
                DoubleVector v = actual.<DoubleBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (v.getDouble(i) != i * 100_000 + 1D) {
                        throw new AssertionError(
                            "[" + operation + "] expected [" + (i * 100_000 + 1D) + "] but was [" + v.getDouble(i) + "]"
                        );
                    }
                }
            }
            case "case_1_eager" -> {
                LongVector f1 = actual.<LongBlock>getBlock(0).asVector();
                LongVector f2 = actual.<LongBlock>getBlock(1).asVector();
                LongVector result = actual.<LongBlock>getBlock(2).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = f1.getLong(i) == 1 ? f1.getLong(i) : f2.getLong(i);
                    if (result.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
                    }
                }
            }
            case "case_1_lazy" -> {
                LongVector f1 = actual.<LongBlock>getBlock(0).asVector();
                LongVector f2 = actual.<LongBlock>getBlock(1).asVector();
                LongVector result = actual.<LongBlock>getBlock(2).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = 1 + (f1.getLong(i) == 1 ? f1.getLong(i) : f2.getLong(i));
                    if (result.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
                    }
                }
            }
            case "date_trunc" -> {
                LongVector v = actual.<LongBlock>getBlock(1).asVector();
                long oneDay = TimeValue.timeValueHours(24).millis();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = i * 100_000;
                    expected -= expected % oneDay;
                    if (v.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getLong(i) + "]");
                    }
                }
            }
            case "equal_to_const" -> {
                BooleanVector v = actual.<BooleanBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (v.getBoolean(i) != (i == 1)) {
                        throw new AssertionError("[" + operation + "] expected [" + (i == 1) + "] but was [" + v.getBoolean(i) + "]");
                    }
                }
            }
            case "long_equal_to_long", "long_equal_to_int" -> {
                BooleanVector v = actual.<BooleanBlock>getBlock(2).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (v.getBoolean(i) != true) {
                        throw new AssertionError("[" + operation + "] expected [" + (i == 1) + "] but was [" + v.getBoolean(i) + "]");
                    }
                }
            }
            case "mv_min", "mv_min_ascending" -> {
                LongVector v = actual.<LongBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (v.getLong(i) != i) {
                        throw new AssertionError("[" + operation + "] expected [" + i + "] but was [" + v.getLong(i) + "]");
                    }
                }
            }
            case "rlike" -> {
                BooleanVector v = actual.<BooleanBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    boolean expected = i % 2 == 1;
                    if (v.getBoolean(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getBoolean(i) + "]");
                    }
                }
            }
            default -> throw new UnsupportedOperationException();
        }
    }

    private static Page page(String operation) {
        return switch (operation) {
            case "abs", "add", "date_trunc", "equal_to_const" -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendLong(i * 100_000);
                }
                yield new Page(builder.build());
            }
            case "add_double" -> {
                var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendDouble(i * 100_000D);
                }
                yield new Page(builder.build());
            }
            case "case_1_eager", "case_1_lazy" -> {
                var f1 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var f2 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    f1.appendLong(i);
                    f2.appendLong(-i);
                }
                yield new Page(f1.build(), f2.build());
            }
            case "long_equal_to_long" -> {
                var lhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var rhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    lhs.appendLong(i * 100_000);
                    rhs.appendLong(i * 100_000);
                }
                yield new Page(lhs.build(), rhs.build());
            }
            case "long_equal_to_int" -> {
                var lhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var rhs = blockFactory.newIntBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    lhs.appendLong(i * 100_000);
                    rhs.appendInt(i * 100_000);
                }
                yield new Page(lhs.build(), rhs.build());
            }
            case "mv_min", "mv_min_ascending" -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                if (operation.endsWith("ascending")) {
                    builder.mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
                }
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.beginPositionEntry();
                    builder.appendLong(i);
                    builder.appendLong(i + 1);
                    builder.appendLong(i + 2);
                    builder.endPositionEntry();
                }
                yield new Page(builder.build());
            }
            case "rlike" -> {
                var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                BytesRef[] values = new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") };
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendBytesRef(values[i % 2]);
                }
                yield new Page(builder.build().asBlock());
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void run() {
        run(operation);
    }

    private static void run(String operation) {
        try (Operator operator = operator(operation)) {
            Page page = page(operation);
            Page output = null;
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page);
                output = operator.getOutput();
            }
            // We only check the last one
            checkExpected(operation, output);
        }
    }
}
