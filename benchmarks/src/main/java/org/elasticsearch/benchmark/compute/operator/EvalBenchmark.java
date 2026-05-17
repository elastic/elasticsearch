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
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.JsonExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
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

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark for ES|QL expression evaluators.
 *
 * <pre>{@code
 * # All benchmarks:
 * ./gradlew -p benchmarks run --args 'EvalBenchmark'
 *
 * # Specific operations:
 * ./gradlew -p benchmarks run --args 'EvalBenchmark -p operation=round_to_floor_10,round_to_ceil_10'
 *
 * # Quick test:
 * ./gradlew -p benchmarks run --args 'EvalBenchmark -p operation=abs -wi 1 -i 2 -f 1'
 * }</pre>
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class EvalBenchmark {
    static {
        Utils.configureBenchmarkLogging();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("bench"))
        .build();

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();
    private static final int BLOCK_LENGTH = 8 * 1024;
    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    static void selfTest() {
        Logger log = LogManager.getLogger(EvalBenchmark.class);
        for (String op : Utils.possibleValues(EvalBenchmark.class, "operation")) {
            log.info("self testing {}", op);
            run(op);
        }
    }

    @Param(
        {
            "abs",
            "add",
            "add_double",
            "case_eager",
            "case_lazy",
            "coalesce_noop",
            "coalesce_eager",
            "coalesce_lazy",
            "date_trunc",
            "equal_to_const",
            "json_extract",
            "json_extract_object",
            "long_equal_to_long",
            "long_equal_to_int",
            "mv_min",
            "mv_min_ascending",
            "rlike",
            "to_lower",
            "to_lower_ords",
            "to_upper",
            "to_upper_ords",
            "round_to_floor_2",
            "round_to_floor_5",
            "round_to_floor_10",
            "round_to_floor_interval",
            "round_to_floor_search",
            "round_to_ceil_2",
            "round_to_ceil_5",
            "round_to_ceil_10",
            "round_to_ceil_interval",
            "round_to_ceil_search",
            "round_to_via_case" }
    )
    public String operation;

    private static Operator operator(String operation) {
        return new EvalOperator(driverContext, evaluator(operation));
    }

    private static ExpressionEvaluator evaluator(String operation) {
        return switch (operation) {
            case "abs" -> {
                FieldAttribute f = longField();
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new Abs(Source.EMPTY, f), layout(f)).get(driverContext);
            }
            case "add" -> {
                FieldAttribute f = longField();
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Add(Source.EMPTY, f, new Literal(Source.EMPTY, 1L, DataType.LONG), configuration()),
                    layout(f)
                ).get(driverContext);
            }
            case "add_double" -> {
                FieldAttribute f = doubleField();
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Add(Source.EMPTY, f, new Literal(Source.EMPTY, 1D, DataType.DOUBLE), configuration()),
                    layout(f)
                ).get(driverContext);
            }
            case "case_eager", "case_lazy" -> {
                FieldAttribute f1 = longField();
                FieldAttribute f2 = longField();
                Expression condition = new Equals(Source.EMPTY, f1, new Literal(Source.EMPTY, 1L, DataType.LONG));
                Expression lhs = f1;
                Expression rhs = f2;
                if (operation.endsWith("lazy")) {
                    lhs = new Add(Source.EMPTY, lhs, new Literal(Source.EMPTY, 1L, DataType.LONG), configuration());
                    rhs = new Add(Source.EMPTY, rhs, new Literal(Source.EMPTY, 1L, DataType.LONG), configuration());
                }
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Case(Source.EMPTY, condition, List.of(lhs, rhs)),
                    layout(f1, f2)
                ).get(driverContext);
                String expected = operation.endsWith("lazy") ? "CaseLazyEvaluator" : "CaseEagerEvaluator";
                assertEvaluator(evaluator, expected);
                yield evaluator;
            }
            case "coalesce_noop", "coalesce_eager", "coalesce_lazy" -> {
                FieldAttribute f1 = longField();
                FieldAttribute f2 = longField();
                Expression lhs = f1;
                if (operation.endsWith("lazy")) {
                    lhs = new Add(Source.EMPTY, lhs, new Literal(Source.EMPTY, 1L, DataType.LONG), configuration());
                }
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Coalesce(Source.EMPTY, lhs, List.of(f2)),
                    layout(f1, f2)
                ).get(driverContext);
                String expected = operation.endsWith("lazy") ? "CoalesceLongLazyEvaluator" : "CoalesceLongEagerEvaluator";
                assertEvaluator(evaluator, expected);
                yield evaluator;
            }
            case "date_trunc" -> {
                FieldAttribute f = new FieldAttribute(
                    Source.EMPTY,
                    "timestamp",
                    new EsField("timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
                );
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new DateTrunc(
                        Source.EMPTY,
                        new Literal(Source.EMPTY, Duration.ofHours(24), DataType.TIME_DURATION),
                        f,
                        configuration()
                    ),
                    layout(f)
                ).get(driverContext);
            }
            case "equal_to_const" -> {
                FieldAttribute f = longField();
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Equals(Source.EMPTY, f, new Literal(Source.EMPTY, 100_000L, DataType.LONG)),
                    layout(f)
                ).get(driverContext);
            }
            case "json_extract" -> {
                FieldAttribute f = keywordField();
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new JsonExtract(Source.EMPTY, f, new Literal(Source.EMPTY, new BytesRef("user.name"), DataType.KEYWORD)),
                    layout(f)
                ).get(driverContext);
            }
            case "json_extract_object" -> {
                FieldAttribute f = keywordField();
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new JsonExtract(Source.EMPTY, f, new Literal(Source.EMPTY, new BytesRef("user"), DataType.KEYWORD)),
                    layout(f)
                ).get(driverContext);
            }
            case "long_equal_to_long" -> {
                FieldAttribute lhs = longField();
                FieldAttribute rhs = longField();
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new Equals(Source.EMPTY, lhs, rhs), layout(lhs, rhs)).get(driverContext);
            }
            case "long_equal_to_int" -> {
                FieldAttribute lhs = longField();
                FieldAttribute rhs = intField();
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new Equals(Source.EMPTY, lhs, rhs), layout(lhs, rhs)).get(driverContext);
            }
            case "mv_min", "mv_min_ascending" -> {
                FieldAttribute f = longField();
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new MvMin(Source.EMPTY, f), layout(f)).get(driverContext);
            }
            case "rlike" -> {
                FieldAttribute f = keywordField();
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new RLike(Source.EMPTY, f, new RLikePattern(".ar")), layout(f))
                    .get(driverContext);
            }
            case "to_lower", "to_lower_ords" -> {
                FieldAttribute f = keywordField();
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new ToLower(Source.EMPTY, f, configuration()), layout(f)).get(driverContext);
            }
            case "to_upper", "to_upper_ords" -> {
                FieldAttribute f = keywordField();
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new ToUpper(Source.EMPTY, f, configuration()), layout(f)).get(driverContext);
            }
            case "round_to_floor_2" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, List.of(lit(1L), lit(ByteSizeUnit.KB.toBytes(1))), Rounding.RoundingConvention.DOWN),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongFloor2");
                yield evaluator;
            }
            case "round_to_floor_5" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, byteSizePoints(5), Rounding.RoundingConvention.DOWN),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongFloor5");
                yield evaluator;
            }
            case "round_to_floor_10" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, uniformPoints(10), Rounding.RoundingConvention.DOWN),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongFloor10");
                yield evaluator;
            }
            case "round_to_floor_interval" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, uniformPoints(15), Rounding.RoundingConvention.DOWN),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongFloorInterval");
                yield evaluator;
            }
            case "round_to_floor_search" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, nonUniformPoints(15), Rounding.RoundingConvention.DOWN),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongFloorSearch");
                yield evaluator;
            }
            case "round_to_ceil_2" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, List.of(lit(1L), lit(ByteSizeUnit.KB.toBytes(1))), Rounding.RoundingConvention.UP),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongCeiling2");
                yield evaluator;
            }
            case "round_to_ceil_5" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, byteSizePoints(5), Rounding.RoundingConvention.UP),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongCeiling5");
                yield evaluator;
            }
            case "round_to_ceil_10" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, uniformPoints(10), Rounding.RoundingConvention.UP),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongCeiling10");
                yield evaluator;
            }
            case "round_to_ceil_interval" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, uniformPoints(15), Rounding.RoundingConvention.UP),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongCeilingInterval");
                yield evaluator;
            }
            case "round_to_ceil_search" -> {
                FieldAttribute f = longField();
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, nonUniformPoints(15), Rounding.RoundingConvention.UP),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "RoundToLongCeilingSearch");
                yield evaluator;
            }
            case "round_to_via_case" -> {
                FieldAttribute f = longField();
                Literal b = lit(1L);
                Literal kb = lit(ByteSizeUnit.KB.toBytes(1));
                Literal mb = lit(ByteSizeUnit.MB.toBytes(1));
                Literal gb = lit(ByteSizeUnit.GB.toBytes(1));
                Expression ltkb = new LessThan(Source.EMPTY, f, kb);
                Expression ltmb = new LessThan(Source.EMPTY, f, mb);
                Expression ltgb = new LessThan(Source.EMPTY, f, gb);
                ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Case(Source.EMPTY, ltkb, List.of(b, ltmb, kb, ltgb, mb, gb)),
                    layout(f)
                ).get(driverContext);
                assertEvaluator(evaluator, "CaseLazyEvaluator");
                yield evaluator;
            }
            default -> throw new IllegalArgumentException("Unknown operation: " + operation);
        };
    }

    private static void checkExpected(String operation, Page actual) {
        switch (operation) {
            case "abs" -> {
                LongVector v = actual.<LongBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = i * 100_000L;
                    if (v.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getLong(i) + "]");
                    }
                }
            }
            case "add" -> {
                LongVector v = actual.<LongBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = i * 100_000L + 1;
                    if (v.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getLong(i) + "]");
                    }
                }
            }
            case "add_double" -> {
                DoubleVector v = actual.<DoubleBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    double expected = i * 100_000D + 1D;
                    if (v.getDouble(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getDouble(i) + "]");
                    }
                }
            }
            case "case_eager" -> {
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
            case "case_lazy" -> {
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
            case "coalesce_noop" -> {
                LongVector f1 = actual.<LongBlock>getBlock(0).asVector();
                LongVector result = actual.<LongBlock>getBlock(2).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (result.getLong(i) != f1.getLong(i)) {
                        throw new AssertionError("[" + operation + "] mismatch at " + i);
                    }
                }
            }
            case "coalesce_eager" -> {
                LongBlock f1 = actual.<LongBlock>getBlock(0);
                LongVector f2 = actual.<LongBlock>getBlock(1).asVector();
                LongVector result = actual.<LongBlock>getBlock(2).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = i % 5 == 0 ? f2.getLong(i) : f1.getLong(f1.getFirstValueIndex(i));
                    if (result.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
                    }
                }
            }
            case "coalesce_lazy" -> {
                LongBlock f1 = actual.<LongBlock>getBlock(0);
                LongVector f2 = actual.<LongBlock>getBlock(1).asVector();
                LongVector result = actual.<LongBlock>getBlock(2).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = i % 5 == 0 ? f2.getLong(i) : f1.getLong(f1.getFirstValueIndex(i)) + 1;
                    if (result.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
                    }
                }
            }
            case "date_trunc" -> {
                LongVector v = actual.<LongBlock>getBlock(1).asVector();
                long oneDay = TimeValue.timeValueHours(24).millis();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long input = i * 100_000L;
                    long expected = input - input % oneDay;
                    if (v.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getLong(i) + "]");
                    }
                }
            }
            case "equal_to_const" -> {
                BooleanVector v = actual.<BooleanBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    boolean expected = i == 1;
                    if (v.getBoolean(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + v.getBoolean(i) + "]");
                    }
                }
            }
            case "long_equal_to_long", "long_equal_to_int" -> {
                BooleanVector v = actual.<BooleanBlock>getBlock(2).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (v.getBoolean(i) != true) {
                        throw new AssertionError("[" + operation + "] expected true but was false at " + i);
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
            case "json_extract" -> checkBytesRef(operation, actual, false, new BytesRef("John"), new BytesRef("John"));
            case "json_extract_object" -> {
                BytesRef expected = new BytesRef("{\"name\":\"John\",\"age\":30}");
                checkBytesRef(operation, actual, false, expected, expected);
            }
            case "to_lower" -> checkBytesRef(operation, actual, false, new BytesRef("foo"), new BytesRef("bar"));
            case "to_lower_ords" -> checkBytesRef(operation, actual, true, new BytesRef("foo"), new BytesRef("bar"));
            case "to_upper" -> checkBytesRef(operation, actual, false, new BytesRef("FOO"), new BytesRef("BAR"));
            case "to_upper_ords" -> checkBytesRef(operation, actual, true, new BytesRef("FOO"), new BytesRef("BAR"));
            case "round_to_floor_2", "round_to_floor_5", "round_to_floor_10", "round_to_floor_interval", "round_to_floor_search",
                "round_to_ceil_2", "round_to_ceil_5", "round_to_ceil_10", "round_to_ceil_interval", "round_to_ceil_search",
                "round_to_via_case" -> {
                LongVector result = actual.<LongBlock>getBlock(1).asVector();
                if (result.getPositionCount() != BLOCK_LENGTH) {
                    throw new AssertionError("[" + operation + "] expected " + BLOCK_LENGTH + " positions");
                }
            }
            default -> throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

    private static void checkBytesRef(String operation, Page actual, boolean expectOrds, BytesRef even, BytesRef odd) {
        BytesRef scratch = new BytesRef();
        BytesRefVector v = actual.<BytesRefBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            BytesRef expected = i % 2 == 0 ? even : odd;
            if (v.getBytesRef(i, scratch).equals(expected) == false) {
                throw new AssertionError("[" + operation + "] mismatch at " + i);
            }
        }
        if (expectOrds && v.asOrdinals() == null) {
            throw new AssertionError("[" + operation + "] expected ordinals");
        }
        if (expectOrds == false && v.asOrdinals() != null) {
            throw new AssertionError("[" + operation + "] did not expect ordinals");
        }
    }

    private static Page page(String operation) {
        return switch (operation) {
            case "abs", "add", "date_trunc", "equal_to_const", "round_to_floor_2", "round_to_floor_5", "round_to_floor_10",
                "round_to_floor_interval", "round_to_floor_search", "round_to_ceil_2", "round_to_ceil_5", "round_to_ceil_10",
                "round_to_ceil_interval", "round_to_ceil_search", "round_to_via_case" -> {
                var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendLong(i * 100_000L);
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
            case "case_eager", "case_lazy", "coalesce_noop" -> {
                var f1 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var f2 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    f1.appendLong(i);
                    f2.appendLong(-i);
                }
                yield new Page(f1.build(), f2.build());
            }
            case "coalesce_eager", "coalesce_lazy" -> {
                var f1 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var f2 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    if (i % 5 == 0) {
                        f1.appendNull();
                    } else {
                        f1.appendLong(i);
                    }
                    f2.appendLong(-i);
                }
                yield new Page(f1.build(), f2.build());
            }
            case "json_extract", "json_extract_object" -> {
                var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                BytesRef json = new BytesRef("{\"user\":{\"name\":\"John\",\"age\":30},\"active\":true}");
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendBytesRef(json);
                }
                yield new Page(builder.build().asBlock());
            }
            case "long_equal_to_long" -> {
                var lhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var rhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    lhs.appendLong(i * 100_000L);
                    rhs.appendLong(i * 100_000L);
                }
                yield new Page(lhs.build(), rhs.build());
            }
            case "long_equal_to_int" -> {
                var lhs = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var rhs = blockFactory.newIntBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    lhs.appendLong(i * 100_000L);
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
            case "rlike", "to_lower", "to_upper" -> {
                var builder = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                BytesRef foo = new BytesRef("foo");
                BytesRef bar = new BytesRef("bar");
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    builder.appendBytesRef(i % 2 == 0 ? foo : bar);
                }
                yield new Page(builder.build().asBlock());
            }
            case "to_lower_ords", "to_upper_ords" -> {
                var bytes = blockFactory.newBytesRefVectorBuilder(2);
                bytes.appendBytesRef(new BytesRef("foo"));
                bytes.appendBytesRef(new BytesRef("bar"));
                var ordinals = blockFactory.newIntVectorFixedBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    ordinals.appendInt(i % 2);
                }
                yield new Page(new OrdinalBytesRefVector(ordinals.build(), bytes.build()).asBlock());
            }
            default -> throw new IllegalArgumentException("Unknown operation: " + operation);
        };
    }

    private static FieldAttribute longField() {
        return new FieldAttribute(Source.EMPTY, "f", new EsField("f", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static FieldAttribute doubleField() {
        return new FieldAttribute(Source.EMPTY, "f", new EsField("f", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static FieldAttribute intField() {
        return new FieldAttribute(Source.EMPTY, "f", new EsField("f", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static FieldAttribute keywordField() {
        return new FieldAttribute(Source.EMPTY, "f", new EsField("f", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static Layout layout(FieldAttribute... fields) {
        Layout.Builder builder = new Layout.Builder();
        builder.append(Arrays.asList(fields));
        return builder.build();
    }

    private static Literal lit(long v) {
        return new Literal(Source.EMPTY, v, DataType.LONG);
    }

    private static List<Expression> byteSizePoints(int count) {
        List<Expression> points = new ArrayList<>(count);
        points.add(lit(1L));
        if (count > 1) points.add(lit(ByteSizeUnit.KB.toBytes(1)));
        if (count > 2) points.add(lit(ByteSizeUnit.MB.toBytes(1)));
        if (count > 3) points.add(lit(ByteSizeUnit.GB.toBytes(1)));
        if (count > 4) points.add(lit(ByteSizeUnit.TB.toBytes(1)));
        return points;
    }

    private static List<Expression> uniformPoints(int count) {
        List<Expression> points = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            points.add(lit(i * 100_000L));
        }
        return points;
    }

    private static List<Expression> nonUniformPoints(int count) {
        List<Expression> points = new ArrayList<>(count);
        long v = 1;
        for (int i = 0; i < count; i++) {
            points.add(lit(v));
            v = v * 2 + 1;
        }
        return points;
    }

    private static void assertEvaluator(ExpressionEvaluator evaluator, String expected) {
        if (evaluator.toString().contains(expected) == false) {
            throw new IllegalArgumentException("Expected [" + expected + "] but got [" + evaluator + "]");
        }
    }

    private static Configuration configuration() {
        return new Configuration(
            ZoneOffset.UTC,
            Instant.now(),
            Locale.ROOT,
            null,
            null,
            QueryPragmas.EMPTY,
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.get(Settings.EMPTY),
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.get(Settings.EMPTY),
            null,
            false,
            Map.of(),
            0,
            false,
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            null,
            null,
            Map.of()
        );
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void eval(Blackhole bh) {
        run(operation, bh);
    }

    private static void run(String operation) {
        run(operation, null);
    }

    private static void run(String operation, Blackhole bh) {
        try (Operator operator = operator(operation)) {
            Page page = page(operation);
            Page output = null;
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page);
                output = operator.getOutput();
                if (bh != null) {
                    bh.consume(output);
                }
            }
            checkExpected(operation, output);
        }
    }
}
