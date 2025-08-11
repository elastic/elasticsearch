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
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.TimeValue;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
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

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    private static final int BLOCK_LENGTH = 8 * 1024;

    static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory);

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
            "coalesce_2_noop",
            "coalesce_2_eager",
            "coalesce_2_lazy",
            "date_trunc",
            "equal_to_const",
            "long_equal_to_long",
            "long_equal_to_int",
            "mv_min",
            "mv_min_ascending",
            "round_to_4_via_case",
            "round_to_2",
            "round_to_3",
            "round_to_4",
            "rlike",
            "to_lower",
            "to_lower_ords",
            "to_upper",
            "to_upper_ords" }
    )
    public String operation;

    private static Operator operator(String operation) {
        return new EvalOperator(driverContext.blockFactory(), evaluator(operation));
    }

    private static EvalOperator.ExpressionEvaluator evaluator(String operation) {
        return switch (operation) {
            case "abs" -> {
                FieldAttribute longField = longField();
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new Abs(Source.EMPTY, longField), layout(longField)).get(driverContext);
            }
            case "add" -> {
                FieldAttribute longField = longField();
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Add(Source.EMPTY, longField, new Literal(Source.EMPTY, 1L, DataType.LONG)),
                    layout(longField)
                ).get(driverContext);
            }
            case "add_double" -> {
                FieldAttribute doubleField = doubleField();
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
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
                EvalOperator.ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Case(Source.EMPTY, condition, List.of(lhs, rhs)),
                    layout(f1, f2)
                ).get(driverContext);
                String desc = operation.endsWith("lazy") ? "CaseLazyEvaluator" : "CaseEagerEvaluator";
                if (evaluator.toString().contains(desc) == false) {
                    throw new IllegalArgumentException("Evaluator was [" + evaluator + "] but expected one containing [" + desc + "]");
                }
                yield evaluator;
            }
            case "coalesce_2_noop", "coalesce_2_eager", "coalesce_2_lazy" -> {
                FieldAttribute f1 = longField();
                FieldAttribute f2 = longField();
                Expression lhs = f1;
                if (operation.endsWith("lazy")) {
                    lhs = new Add(Source.EMPTY, lhs, new Literal(Source.EMPTY, 1L, DataType.LONG));
                }
                EvalOperator.ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Coalesce(Source.EMPTY, lhs, List.of(f2)),
                    layout(f1, f2)
                ).get(driverContext);
                String desc = operation.endsWith("lazy") ? "CoalesceLazyEvaluator" : "CoalesceEagerEvaluator";
                if (evaluator.toString().contains(desc) == false) {
                    throw new IllegalArgumentException("Evaluator was [" + evaluator + "] but expected one containing [" + desc + "]");
                }
                yield evaluator;
            }
            case "date_trunc" -> {
                FieldAttribute timestamp = new FieldAttribute(
                    Source.EMPTY,
                    "timestamp",
                    new EsField("timestamp", DataType.DATETIME, Map.of(), true)
                );
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new DateTrunc(Source.EMPTY, new Literal(Source.EMPTY, Duration.ofHours(24), DataType.TIME_DURATION), timestamp),
                    layout(timestamp)
                ).get(driverContext);
            }
            case "equal_to_const" -> {
                FieldAttribute longField = longField();
                yield EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Equals(Source.EMPTY, longField, new Literal(Source.EMPTY, 100_000L, DataType.LONG)),
                    layout(longField)
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
                FieldAttribute longField = longField();
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, new MvMin(Source.EMPTY, longField), layout(longField)).get(driverContext);
            }
            case "rlike" -> {
                FieldAttribute keywordField = keywordField();
                RLike rlike = new RLike(Source.EMPTY, keywordField, new RLikePattern(".ar"));
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, rlike, layout(keywordField)).get(driverContext);
            }
            case "round_to_4_via_case" -> {
                FieldAttribute f = longField();

                Expression ltkb = new LessThan(Source.EMPTY, f, kb());
                Expression ltmb = new LessThan(Source.EMPTY, f, mb());
                Expression ltgb = new LessThan(Source.EMPTY, f, gb());
                EvalOperator.ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new Case(Source.EMPTY, ltkb, List.of(b(), ltmb, kb(), ltgb, mb(), gb())),
                    layout(f)
                ).get(driverContext);
                String desc = "CaseLazyEvaluator";
                if (evaluator.toString().contains(desc) == false) {
                    throw new IllegalArgumentException("Evaluator was [" + evaluator + "] but expected one containing [" + desc + "]");
                }
                yield evaluator;
            }
            case "round_to_2" -> {
                FieldAttribute f = longField();

                EvalOperator.ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, List.of(b(), kb())),
                    layout(f)
                ).get(driverContext);
                String desc = "RoundToLong2";
                if (evaluator.toString().contains(desc) == false) {
                    throw new IllegalArgumentException("Evaluator was [" + evaluator + "] but expected one containing [" + desc + "]");
                }
                yield evaluator;
            }
            case "round_to_3" -> {
                FieldAttribute f = longField();

                EvalOperator.ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, List.of(b(), kb(), mb())),
                    layout(f)
                ).get(driverContext);
                String desc = "RoundToLong3";
                if (evaluator.toString().contains(desc) == false) {
                    throw new IllegalArgumentException("Evaluator was [" + evaluator + "] but expected one containing [" + desc + "]");
                }
                yield evaluator;
            }
            case "round_to_4" -> {
                FieldAttribute f = longField();

                EvalOperator.ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
                    FOLD_CONTEXT,
                    new RoundTo(Source.EMPTY, f, List.of(b(), kb(), mb(), gb())),
                    layout(f)
                ).get(driverContext);
                String desc = "RoundToLong4";
                if (evaluator.toString().contains(desc) == false) {
                    throw new IllegalArgumentException("Evaluator was [" + evaluator + "] but expected one containing [" + desc + "]");
                }
                yield evaluator;
            }
            case "to_lower", "to_lower_ords" -> {
                FieldAttribute keywordField = keywordField();
                ToLower toLower = new ToLower(Source.EMPTY, keywordField, configuration());
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, toLower, layout(keywordField)).get(driverContext);
            }
            case "to_upper", "to_upper_ords" -> {
                FieldAttribute keywordField = keywordField();
                ToUpper toUpper = new ToUpper(Source.EMPTY, keywordField, configuration());
                yield EvalMapper.toEvaluator(FOLD_CONTEXT, toUpper, layout(keywordField)).get(driverContext);
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
            case "coalesce_2_noop" -> {
                LongVector f1 = actual.<LongBlock>getBlock(0).asVector();
                LongVector result = actual.<LongBlock>getBlock(2).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = f1.getLong(i);
                    if (result.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
                    }
                }
            }
            case "coalesce_2_eager" -> {
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
            case "coalesce_2_lazy" -> {
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
            case "round_to_4_via_case", "round_to_4" -> {
                long b = 1;
                long kb = ByteSizeUnit.KB.toBytes(1);
                long mb = ByteSizeUnit.MB.toBytes(1);
                long gb = ByteSizeUnit.GB.toBytes(1);

                LongVector f = actual.<LongBlock>getBlock(0).asVector();
                LongVector result = actual.<LongBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = f.getLong(i);
                    if (expected < kb) {
                        expected = b;
                    } else if (expected < mb) {
                        expected = kb;
                    } else if (expected < gb) {
                        expected = mb;
                    } else {
                        expected = gb;
                    }
                    if (result.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
                    }
                }
            }
            case "round_to_3" -> {
                long b = 1;
                long kb = ByteSizeUnit.KB.toBytes(1);
                long mb = ByteSizeUnit.MB.toBytes(1);

                LongVector f = actual.<LongBlock>getBlock(0).asVector();
                LongVector result = actual.<LongBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = f.getLong(i);
                    if (expected < kb) {
                        expected = b;
                    } else if (expected < mb) {
                        expected = kb;
                    } else {
                        expected = mb;
                    }
                    if (result.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
                    }
                }
            }
            case "round_to_2" -> {
                long b = 1;
                long kb = ByteSizeUnit.KB.toBytes(1);

                LongVector f = actual.<LongBlock>getBlock(0).asVector();
                LongVector result = actual.<LongBlock>getBlock(1).asVector();
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    long expected = f.getLong(i);
                    if (expected < kb) {
                        expected = b;
                    } else {
                        expected = kb;
                    }
                    if (result.getLong(i) != expected) {
                        throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + result.getLong(i) + "]");
                    }
                }
            }
            case "to_lower" -> checkBytes(operation, actual, false, new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") });
            case "to_lower_ords" -> checkBytes(operation, actual, true, new BytesRef[] { new BytesRef("foo"), new BytesRef("bar") });
            case "to_upper" -> checkBytes(operation, actual, false, new BytesRef[] { new BytesRef("FOO"), new BytesRef("BAR") });
            case "to_upper_ords" -> checkBytes(operation, actual, true, new BytesRef[] { new BytesRef("FOO"), new BytesRef("BAR") });
            default -> throw new UnsupportedOperationException(operation);
        }
    }

    private static void checkBytes(String operation, Page actual, boolean expectOrds, BytesRef[] expectedVals) {
        BytesRef scratch = new BytesRef();
        BytesRefVector v = actual.<BytesRefBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            BytesRef expected = expectedVals[i % 2];
            BytesRef b = v.getBytesRef(i, scratch);
            if (b.equals(expected) == false) {
                throw new AssertionError("[" + operation + "] expected [" + expected + "] but was [" + b + "]");
            }
        }
        if (expectOrds) {
            if (v.asOrdinals() == null) {
                throw new IllegalArgumentException("expected ords but got " + v);
            }
        } else {
            if (v.asOrdinals() != null) {
                throw new IllegalArgumentException("expected non-ords but got " + v);
            }
        }
    }

    private static Page page(String operation) {
        return switch (operation) {
            case "abs", "add", "date_trunc", "equal_to_const", "round_to_4_via_case", "round_to_2", "round_to_3", "round_to_4" -> {
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
            case "case_1_eager", "case_1_lazy", "coalesce_2_noop" -> {
                var f1 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                var f2 = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    f1.appendLong(i);
                    f2.appendLong(-i);
                }
                yield new Page(f1.build(), f2.build());
            }
            case "coalesce_2_eager", "coalesce_2_lazy" -> {
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
            case "to_lower_ords", "to_upper_ords" -> {
                var bytes = blockFactory.newBytesRefVectorBuilder(BLOCK_LENGTH);
                bytes.appendBytesRef(new BytesRef("foo"));
                bytes.appendBytesRef(new BytesRef("bar"));
                var ordinals = blockFactory.newIntVectorFixedBuilder(BLOCK_LENGTH);
                for (int i = 0; i < BLOCK_LENGTH; i++) {
                    ordinals.appendInt(i % 2);
                }
                yield new Page(new OrdinalBytesRefVector(ordinals.build(), bytes.build()).asBlock());
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    private static Literal b() {
        return lit(1L);
    }

    private static Literal kb() {
        return lit(ByteSizeUnit.KB.toBytes(1));
    }

    private static Literal mb() {
        return lit(ByteSizeUnit.MB.toBytes(1));
    }

    private static Literal gb() {
        return lit(ByteSizeUnit.GB.toBytes(1));
    }

    private static Literal lit(long v) {
        return new Literal(Source.EMPTY, v, DataType.LONG);
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

    private static Configuration configuration() {
        return new Configuration(
            ZoneOffset.UTC,
            Locale.ROOT,
            null,
            null,
            null,
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.get(Settings.EMPTY),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.get(Settings.EMPTY),
            null,
            false,
            Map.of(),
            0,
            false
        );
    }
}
