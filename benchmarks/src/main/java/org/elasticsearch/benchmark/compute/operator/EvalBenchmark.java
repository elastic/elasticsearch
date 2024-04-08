/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
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
        { "abs", "add", "date_trunc", "equal_to_const", "long_equal_to_long", "long_equal_to_int", "mv_min", "mv_min_ascending", "rlike" }
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
                    new Add(Source.EMPTY, longField, new Literal(Source.EMPTY, 1L, DataTypes.LONG)),
                    layout(longField)
                ).get(driverContext);
            }
            case "date_trunc" -> {
                FieldAttribute timestamp = new FieldAttribute(
                    Source.EMPTY,
                    "timestamp",
                    new EsField("timestamp", DataTypes.DATETIME, Map.of(), true)
                );
                yield EvalMapper.toEvaluator(
                    new DateTrunc(Source.EMPTY, new Literal(Source.EMPTY, Duration.ofHours(24), EsqlDataTypes.TIME_DURATION), timestamp),
                    layout(timestamp)
                ).get(driverContext);
            }
            case "equal_to_const" -> {
                FieldAttribute longField = longField();
                yield EvalMapper.toEvaluator(
                    new Equals(Source.EMPTY, longField, new Literal(Source.EMPTY, 100_000L, DataTypes.LONG)),
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
        return new FieldAttribute(Source.EMPTY, "long", new EsField("long", DataTypes.LONG, Map.of(), true));
    }

    private static FieldAttribute intField() {
        return new FieldAttribute(Source.EMPTY, "int", new EsField("int", DataTypes.INTEGER, Map.of(), true));
    }

    private static FieldAttribute keywordField() {
        return new FieldAttribute(Source.EMPTY, "keyword", new EsField("keyword", DataTypes.KEYWORD, Map.of(), true));
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
