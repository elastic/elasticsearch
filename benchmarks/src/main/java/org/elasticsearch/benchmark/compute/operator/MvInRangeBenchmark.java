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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.ConstantMethodResultSpecializer;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Compares engine-eval execution strategies for {@code MV_IN_RANGE(field, lower, upper)} when the bounds are constant:
 * <ul>
 *   <li><b>block</b> — the shipped path. All three arguments are read as blocks, one {@code Block} fetch per bound per
 *       row (constructed through {@link EvalMapper}, exactly as production routes today).</li>
 *   <li><b>fixed</b> — the bounds are plain {@code @Fixed} constructor params, baked into the evaluator once.</li>
 *   <li><b>jit</b> — the bounds are JIT-folded via {@code @Fixed(jitConstant = true)}, carried as a composite record
 *       ({@code LongBounds} / {@code BytesRefBounds}) so a single jitConstant param covers both bounds. The materialized
 *       class name is asserted to contain {@code ConstantSpecialized}, proving specialization fired rather than the
 *       {@code Standard} (non-folded) fallback.</li>
 * </ul>
 *
 * <p>The matrix crosses strategy × element type (long / keyword — int/double share the long regime) × multivalue count
 * × where in the value list the match lands ({@code hit_first} exits on the first value, {@code hit_last} walks the whole
 * list, {@code miss} finds nothing and also walks the whole list). The folding win, if any, is one saved block fetch per
 * bound per row; it should be largest at {@code mvCount = 1} and shrink as the per-row loop lengthens.
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class MvInRangeBenchmark {

    // Initialize logging before any BlockFactory / DriverContext field touches LogManager (mirrors EvalBenchmark), then
    // force the admission threshold to 1 so the jit strategy specializes on the first evaluator built — the bench must
    // measure the JIT-folded steady state, not the cold Standard fallback that a fresh admission cycle would route to.
    static {
        Utils.configureBenchmarkLogging();
        ConstantMethodResultSpecializer.SHARED.setAdmissionThreshold(1);
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    private static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    private static final int BLOCK_LENGTH = 8 * 1024;

    // Inclusive range the evaluators test against, one per element type. Values are generated to fall clearly inside or
    // clearly above the range so membership is deterministic regardless of the per-row seed.
    private static final long LOWER_LONG = 1000L;
    private static final long UPPER_LONG = 2000L;
    private static final BytesRef LOWER_KEYWORD = new BytesRef("m");
    private static final BytesRef UPPER_KEYWORD = new BytesRef("s");

    private static final String STRATEGY_BLOCK = "block";
    private static final String STRATEGY_FIXED = "fixed";
    private static final String STRATEGY_JIT = "jit";
    private static final String TYPE_LONG = "long";
    private static final String TYPE_KEYWORD = "keyword";
    private static final String MATCH_HIT_FIRST = "hit_first";
    private static final String MATCH_HIT_LAST = "hit_last";
    private static final String MATCH_MISS = "miss";

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            // Smoke every cell: build its strategy evaluator, run it, and verify the boolean result — and, for jit,
            // that specialization actually fired. This also forces the subclasses to load more like production.
            selfTest();
        }
    }

    static void selfTest() {
        Logger log = LogManager.getLogger(MvInRangeBenchmark.class);
        for (String strategy : List.of(STRATEGY_BLOCK, STRATEGY_FIXED, STRATEGY_JIT)) {
            for (String type : List.of(TYPE_LONG, TYPE_KEYWORD)) {
                for (int mvCount : new int[] { 1, 3, 10 }) {
                    for (String match : List.of(MATCH_HIT_FIRST, MATCH_HIT_LAST, MATCH_MISS)) {
                        log.info("self testing strategy={} type={} mvCount={} match={}", strategy, type, mvCount, match);
                        ExpressionEvaluator.Factory factory = evaluatorFactory(strategy, type);
                        assertStrategy(strategy, type, factory);
                        Page output = run(factory, type, mvCount, match);
                        checkExpected(strategy, type, mvCount, match, output);
                    }
                }
            }
        }
    }

    @Param({ "block", "fixed", "jit" })
    public String strategy;

    @Param({ "long", "keyword" })
    public String type;

    @Param({ "1", "3", "10" })
    public int mvCount;

    @Param({ "hit_first", "hit_last", "miss" })
    public String match;

    private ExpressionEvaluator.Factory factory;

    @Setup(Level.Trial)
    public void setup() {
        this.factory = evaluatorFactory(strategy, type);
        assertStrategy(strategy, type, factory);
    }

    private static ExpressionEvaluator.Factory evaluatorFactory(String strategy, String type) {
        FieldAttribute field = fieldAttribute(type);
        return switch (strategy) {
            case STRATEGY_BLOCK -> blockFactory(field, type);
            case STRATEGY_FIXED -> fixedFactory(field, type);
            case STRATEGY_JIT -> jitFactory(field, type);
            default -> throw new IllegalArgumentException("unknown strategy [" + strategy + "]");
        };
    }

    /** Shipped path: MV_IN_RANGE mapped through EvalMapper, which selects the block-based per-type evaluator. */
    private static ExpressionEvaluator.Factory blockFactory(FieldAttribute field, String type) {
        MvInRange expression = new MvInRange(Source.EMPTY, field, lowerLiteral(type), upperLiteral(type));
        return EvalMapper.toEvaluator(FOLD_CONTEXT, expression, layout(field));
    }

    /** Bounds baked as plain @Fixed constructor params. Field read from the page via its own field-attribute evaluator. */
    private static ExpressionEvaluator.Factory fixedFactory(FieldAttribute field, String type) {
        ExpressionEvaluator.Factory fieldEval = EvalMapper.toEvaluator(FOLD_CONTEXT, field, layout(field));
        return switch (type) {
            case TYPE_LONG -> MvInRange.constantLongFactory(Source.EMPTY, fieldEval, LOWER_LONG, UPPER_LONG);
            case TYPE_KEYWORD -> MvInRange.constantBytesRefFactory(Source.EMPTY, fieldEval, LOWER_KEYWORD, UPPER_KEYWORD);
            default -> throw new IllegalArgumentException("unknown type [" + type + "]");
        };
    }

    /** Bounds JIT-folded via @Fixed(jitConstant = true), carried as a composite record. */
    private static ExpressionEvaluator.Factory jitFactory(FieldAttribute field, String type) {
        ExpressionEvaluator.Factory fieldEval = EvalMapper.toEvaluator(FOLD_CONTEXT, field, layout(field));
        return switch (type) {
            case TYPE_LONG -> MvInRange.foldedLongFactory(Source.EMPTY, fieldEval, LOWER_LONG, UPPER_LONG);
            case TYPE_KEYWORD -> MvInRange.foldedBytesRefFactory(Source.EMPTY, fieldEval, LOWER_KEYWORD, UPPER_KEYWORD);
            default -> throw new IllegalArgumentException("unknown type [" + type + "]");
        };
    }

    private static void assertStrategy(String strategy, String type, ExpressionEvaluator.Factory factory) {
        ExpressionEvaluator evaluator = factory.get(driverContext);
        String rendered = evaluator.toString();
        switch (strategy) {
            case STRATEGY_BLOCK -> {
                String expected = type.equals(TYPE_LONG) ? "MvInRangeLongEvaluator" : "MvInRangeBytesRefEvaluator";
                assertContains(rendered, expected);
                assertNotContains(rendered, "Constant");
                assertNotContains(rendered, "Folded");
            }
            case STRATEGY_FIXED -> assertContains(
                rendered,
                type.equals(TYPE_LONG) ? "MvInRangeConstantLongEvaluator" : "MvInRangeConstantBytesRefEvaluator"
            );
            case STRATEGY_JIT -> {
                assertContains(rendered, type.equals(TYPE_LONG) ? "MvInRangeFoldedLongEvaluator" : "MvInRangeFoldedBytesRefEvaluator");
                // The materialized class must be the constant-specialized subclass, not the Standard fallback: that is
                // the whole point of the jit strategy, and the admission threshold is forced to 1 so it fires.
                assertContains(evaluator.getClass().getName(), "ConstantSpecialized");
            }
            default -> throw new IllegalArgumentException("unknown strategy [" + strategy + "]");
        }
    }

    private static FieldAttribute fieldAttribute(String type) {
        DataType dataType = type.equals(TYPE_LONG) ? DataType.LONG : DataType.KEYWORD;
        return new FieldAttribute(Source.EMPTY, type, new EsField(type, dataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal lowerLiteral(String type) {
        return type.equals(TYPE_LONG)
            ? new Literal(Source.EMPTY, LOWER_LONG, DataType.LONG)
            : new Literal(Source.EMPTY, LOWER_KEYWORD, DataType.KEYWORD);
    }

    private static Literal upperLiteral(String type) {
        return type.equals(TYPE_LONG)
            ? new Literal(Source.EMPTY, UPPER_LONG, DataType.LONG)
            : new Literal(Source.EMPTY, UPPER_KEYWORD, DataType.KEYWORD);
    }

    private static Layout layout(FieldAttribute field) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(List.of(field));
        return layout.build();
    }

    private static Page page(String type, int mvCount, String match) {
        return type.equals(TYPE_LONG) ? longPage(mvCount, match) : keywordPage(mvCount, match);
    }

    private static Page longPage(int mvCount, String match) {
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH * mvCount)) {
            for (int p = 0; p < BLOCK_LENGTH; p++) {
                if (mvCount == 1) {
                    builder.appendLong(match.equals(MATCH_MISS) ? outLong(p) : inLong(p));
                    continue;
                }
                builder.beginPositionEntry();
                int hitIndex = hitIndex(mvCount, match); // -1 for miss
                for (int v = 0; v < mvCount; v++) {
                    builder.appendLong(v == hitIndex ? inLong(p * mvCount + v) : outLong(p * mvCount + v));
                }
                builder.endPositionEntry();
            }
            return new Page(builder.build());
        }
    }

    private static Page keywordPage(int mvCount, String match) {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(BLOCK_LENGTH * mvCount)) {
            for (int p = 0; p < BLOCK_LENGTH; p++) {
                if (mvCount == 1) {
                    builder.appendBytesRef(match.equals(MATCH_MISS) ? outKeyword(p) : inKeyword(p));
                    continue;
                }
                builder.beginPositionEntry();
                int hitIndex = hitIndex(mvCount, match); // -1 for miss
                for (int v = 0; v < mvCount; v++) {
                    builder.appendBytesRef(v == hitIndex ? inKeyword(p * mvCount + v) : outKeyword(p * mvCount + v));
                }
                builder.endPositionEntry();
            }
            return new Page(builder.build());
        }
    }

    /** Which value index carries the in-range match, or -1 if none (miss). hit_first exits early; hit_last walks all. */
    private static int hitIndex(int mvCount, String match) {
        return switch (match) {
            case MATCH_HIT_FIRST -> 0;
            case MATCH_HIT_LAST -> mvCount - 1;
            case MATCH_MISS -> -1;
            default -> throw new IllegalArgumentException("unknown match [" + match + "]");
        };
    }

    private static long inLong(int seed) {
        return LOWER_LONG + (seed % (UPPER_LONG - LOWER_LONG + 1)); // always within [LOWER_LONG, UPPER_LONG]
    }

    private static long outLong(int seed) {
        return UPPER_LONG + 1000L + (seed % 1000L); // always above UPPER_LONG
    }

    private static BytesRef inKeyword(int seed) {
        return new BytesRef("p" + (seed % 1000)); // "p..." sorts inside ["m", "s"]
    }

    private static BytesRef outKeyword(int seed) {
        return new BytesRef("z" + (seed % 1000)); // "z..." sorts above "s"
    }

    private static void checkExpected(String strategy, String type, int mvCount, String match, Page output) {
        boolean expected = match.equals(MATCH_MISS) == false;
        BooleanBlock resultBlock = output.getBlock(1);
        BooleanVector result = resultBlock.asVector();
        if (result == null) {
            throw new AssertionError(
                "[" + strategy + "/" + type + "/mv" + mvCount + "/" + match + "] expected a dense boolean result, got nulls"
            );
        }
        for (int p = 0; p < BLOCK_LENGTH; p++) {
            if (result.getBoolean(p) != expected) {
                throw new AssertionError(
                    "[" + strategy + "/" + type + "/mv" + mvCount + "/" + match + "] row " + p + " expected [" + expected + "]"
                );
            }
        }
    }

    private static Page run(ExpressionEvaluator.Factory factory, String type, int mvCount, String match) {
        try (Operator operator = new EvalOperator(driverContext, factory.get(driverContext))) {
            Page page = page(type, mvCount, match);
            Page output = null;
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page);
                output = operator.getOutput();
            }
            return output;
        }
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void run(Blackhole bh) {
        bh.consume(run(factory, type, mvCount, match));
    }

    private static void assertContains(String actual, String required) {
        if (actual.contains(required) == false) {
            throw new IllegalArgumentException("expected [" + actual + "] to contain [" + required + "]");
        }
    }

    private static void assertNotContains(String actual, String forbidden) {
        if (actual.contains(forbidden)) {
            throw new IllegalArgumentException("expected [" + actual + "] to NOT contain [" + forbidden + "]");
        }
    }
}
