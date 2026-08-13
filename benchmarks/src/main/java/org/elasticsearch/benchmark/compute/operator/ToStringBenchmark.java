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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the {@code ToString} evaluators that convert numeric and boolean
 * values to {@link BytesRef}.
 *
 * <p>Covered evaluators:
 * <ul>
 *   <li>{@link #run} — {@code ToStringFromLongEvaluator}
 *   <li>{@link #fromInt} — {@code ToStringFromIntEvaluator}
 *   <li>{@link #fromDouble} — {@code ToStringFromDoubleEvaluator}
 *   <li>{@link #fromBoolean} — {@code ToStringFromBooleanEvaluator}
 *   <li>{@link #fromUnsignedLong} — {@code ToStringFromUnsignedLongEvaluator}
 * </ul>
 *
 * <p>All benchmarks use {@link #BLOCK_LENGTH} = 4096 rows per page and process
 * 1024 pages per JMH invocation; {@code @OperationsPerInvocation} normalises
 * results to nanoseconds per element.
 */
@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(1)
public class ToStringBenchmark {
    /**
     * ES|QL default max_concurrent_rows page size; matches the operating
     * regime observed in the esql-grouping-explore highcard macro profile.
     */
    static final int BLOCK_LENGTH = 4096;

    // LCG step shared by all Fisher-Yates shuffles (seed 0xCAFEBABE).
    private static long lcgStep(long seed) {
        return seed * 6364136223846793005L + 1442695040888963407L;
    }

    // -----------------------------------------------------------------------
    // Input arrays — one per evaluator type, each shuffled with a fixed seed
    // -----------------------------------------------------------------------

    /**
     * Input longs: {@code i * 100_000L} for i in [0, BLOCK_LENGTH), shuffled.
     * Gives a realistic spread of digit counts (1–9 digits).
     */
    private static final long[] LONG_VALUES = buildShuffledLongValues();

    /**
     * Input ints: {@code i * 10_000} for i in [0, BLOCK_LENGTH), shuffled.
     * Gives 1–8 decimal digits.
     */
    private static final int[] INT_VALUES = buildShuffledIntValues();

    /**
     * Input doubles: {@code i * 1.5} for i in [0, BLOCK_LENGTH), shuffled.
     * Mix of integer-valued and fractional representations exercises the Ryu
     * algorithm's fast and slow paths.
     */
    private static final double[] DOUBLE_VALUES = buildShuffledDoubleValues();

    /**
     * Input booleans: alternating true/false for even/odd positions.
     */
    private static final boolean[] BOOL_VALUES = buildBoolValues();

    /**
     * Input unsigned longs encoded as ES|QL stores them
     * ({@code unsigned_value ^ Long.MIN_VALUE}): unsigned values are
     * {@code (long) i * 1_000_000_000L} for i in [0, BLOCK_LENGTH), shuffled.
     * All unsigned values fit in a positive signed long, so the stored values
     * are all negative.
     */
    private static final long[] UNSIGNED_LONG_VALUES = buildShuffledUnsignedLongValues();

    private static long[] buildShuffledLongValues() {
        long[] v = new long[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            v[i] = i * 100_000L;
        }
        long seed = 0xCAFEBABEL;
        for (int i = BLOCK_LENGTH - 1; i > 0; i--) {
            seed = lcgStep(seed);
            int j = (int) (((seed >>> 33) * (i + 1L)) >>> 31);
            long tmp = v[i];
            v[i] = v[j];
            v[j] = tmp;
        }
        return v;
    }

    private static int[] buildShuffledIntValues() {
        int[] v = new int[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            v[i] = i * 10_000;
        }
        long seed = 0xCAFEBABEL;
        for (int i = BLOCK_LENGTH - 1; i > 0; i--) {
            seed = lcgStep(seed);
            int j = (int) (((seed >>> 33) * (i + 1L)) >>> 31);
            int tmp = v[i];
            v[i] = v[j];
            v[j] = tmp;
        }
        return v;
    }

    private static double[] buildShuffledDoubleValues() {
        double[] v = new double[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            v[i] = i * 1.5;
        }
        long seed = 0xCAFEBABEL;
        for (int i = BLOCK_LENGTH - 1; i > 0; i--) {
            seed = lcgStep(seed);
            int j = (int) (((seed >>> 33) * (i + 1L)) >>> 31);
            double tmp = v[i];
            v[i] = v[j];
            v[j] = tmp;
        }
        return v;
    }

    private static boolean[] buildBoolValues() {
        boolean[] v = new boolean[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            v[i] = (i & 1) == 0;
        }
        return v;
    }

    private static long[] buildShuffledUnsignedLongValues() {
        long[] v = new long[BLOCK_LENGTH];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            // ES|QL stores unsigned longs as (unsigned_value ^ Long.MIN_VALUE).
            v[i] = ((long) i * 1_000_000_000L) ^ Long.MIN_VALUE;
        }
        long seed = 0xCAFEBABEL;
        for (int i = BLOCK_LENGTH - 1; i > 0; i--) {
            seed = lcgStep(seed);
            int j = (int) (((seed >>> 33) * (i + 1L)) >>> 31);
            long tmp = v[i];
            v[i] = v[j];
            v[j] = tmp;
        }
        return v;
    }

    // -----------------------------------------------------------------------
    // Shared infrastructure
    // -----------------------------------------------------------------------

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    private static final FoldContext FOLD_CONTEXT = FoldContext.small();

    static final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    static {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    static void selfTest() {
        run();
        runInt();
        runDouble();
        runBoolean();
        runUnsignedLong();
    }

    private static Configuration configuration() {
        return new Configuration(
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
            ResolvedSettings.EMPTY,
            Map.of()
        );
    }

    private static Layout layout(FieldAttribute... fields) {
        Layout.Builder layout = new Layout.Builder();
        layout.append(Arrays.asList(fields));
        return layout.build();
    }

    // -----------------------------------------------------------------------
    // FromLong — ToStringFromLongEvaluator
    // -----------------------------------------------------------------------

    private static ExpressionEvaluator toStringFromLongEvaluator() {
        FieldAttribute longField = new FieldAttribute(
            Source.EMPTY,
            "lng",
            new EsField("lng", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new ToString(Source.EMPTY, longField, configuration()),
            layout(longField)
        ).get(driverContext);
        if (evaluator.toString().contains("ToStringFromLongEvaluator") == false) {
            throw new IllegalArgumentException("Expected ToStringFromLongEvaluator but got [" + evaluator + "]");
        }
        return evaluator;
    }

    private static Operator operator() {
        return new EvalOperator(driverContext, toStringFromLongEvaluator());
    }

    private static Page longPage() {
        var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            builder.appendLong(LONG_VALUES[i]);
        }
        return new Page(builder.build());
    }

    private static void checkLong(Page actual) {
        BytesRef scratch = new BytesRef();
        BytesRefVector v = actual.<BytesRefBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            BytesRef expected = new BytesRef(String.valueOf(LONG_VALUES[i]));
            BytesRef got = v.getBytesRef(i, scratch);
            if (got.equals(expected) == false) {
                throw new AssertionError(
                    "position [" + i + "] expected [" + expected.utf8ToString() + "] but was [" + got.utf8ToString() + "]"
                );
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void run(Blackhole bh) {
        bh.consume(run());
    }

    private static Object run() {
        try (var op = operator()) {
            Page page = longPage();
            Page output = null;
            for (int i = 0; i < 1024; i++) {
                op.addInput(page);
                output = op.getOutput();
            }
            checkLong(output);
            return output;
        }
    }

    // -----------------------------------------------------------------------
    // FromInt — ToStringFromIntEvaluator
    // -----------------------------------------------------------------------

    private static ExpressionEvaluator toStringFromIntEvaluator() {
        FieldAttribute intField = new FieldAttribute(
            Source.EMPTY,
            "i",
            new EsField("i", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new ToString(Source.EMPTY, intField, configuration()),
            layout(intField)
        ).get(driverContext);
        if (evaluator.toString().contains("ToStringFromIntEvaluator") == false) {
            throw new IllegalArgumentException("Expected ToStringFromIntEvaluator but got [" + evaluator + "]");
        }
        return evaluator;
    }

    private static Page intPage() {
        var builder = blockFactory.newIntBlockBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            builder.appendInt(INT_VALUES[i]);
        }
        return new Page(builder.build());
    }

    private static void checkInt(Page actual) {
        BytesRef scratch = new BytesRef();
        BytesRefVector v = actual.<BytesRefBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            BytesRef expected = new BytesRef(String.valueOf(INT_VALUES[i]));
            BytesRef got = v.getBytesRef(i, scratch);
            if (got.equals(expected) == false) {
                throw new AssertionError(
                    "fromInt position [" + i + "] expected [" + expected.utf8ToString() + "] but was [" + got.utf8ToString() + "]"
                );
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void fromInt(Blackhole bh) {
        bh.consume(runInt());
    }

    private static Object runInt() {
        try (var operator = new EvalOperator(driverContext, toStringFromIntEvaluator())) {
            Page page = intPage();
            Page output = null;
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page);
                output = operator.getOutput();
            }
            checkInt(output);
            return output;
        }
    }

    // -----------------------------------------------------------------------
    // FromDouble — ToStringFromDoubleEvaluator
    // -----------------------------------------------------------------------

    private static ExpressionEvaluator toStringFromDoubleEvaluator() {
        FieldAttribute doubleField = new FieldAttribute(
            Source.EMPTY,
            "d",
            new EsField("d", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new ToString(Source.EMPTY, doubleField, configuration()),
            layout(doubleField)
        ).get(driverContext);
        if (evaluator.toString().contains("ToStringFromDoubleEvaluator") == false) {
            throw new IllegalArgumentException("Expected ToStringFromDoubleEvaluator but got [" + evaluator + "]");
        }
        return evaluator;
    }

    private static Page doublePage() {
        var builder = blockFactory.newDoubleBlockBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            builder.appendDouble(DOUBLE_VALUES[i]);
        }
        return new Page(builder.build());
    }

    private static void checkDouble(Page actual) {
        BytesRef scratch = new BytesRef();
        BytesRefVector v = actual.<BytesRefBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            BytesRef expected = new BytesRef(String.valueOf(DOUBLE_VALUES[i]));
            BytesRef got = v.getBytesRef(i, scratch);
            if (got.equals(expected) == false) {
                throw new AssertionError(
                    "fromDouble position [" + i + "] expected [" + expected.utf8ToString() + "] but was [" + got.utf8ToString() + "]"
                );
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void fromDouble(Blackhole bh) {
        bh.consume(runDouble());
    }

    private static Object runDouble() {
        try (var operator = new EvalOperator(driverContext, toStringFromDoubleEvaluator())) {
            Page page = doublePage();
            Page output = null;
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page);
                output = operator.getOutput();
            }
            checkDouble(output);
            return output;
        }
    }

    // -----------------------------------------------------------------------
    // FromBoolean — ToStringFromBooleanEvaluator
    // -----------------------------------------------------------------------

    private static ExpressionEvaluator toStringFromBooleanEvaluator() {
        FieldAttribute boolField = new FieldAttribute(
            Source.EMPTY,
            "b",
            new EsField("b", DataType.BOOLEAN, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new ToString(Source.EMPTY, boolField, configuration()),
            layout(boolField)
        ).get(driverContext);
        if (evaluator.toString().contains("ToStringFromBooleanEvaluator") == false) {
            throw new IllegalArgumentException("Expected ToStringFromBooleanEvaluator but got [" + evaluator + "]");
        }
        return evaluator;
    }

    private static Page boolPage() {
        var builder = blockFactory.newBooleanBlockBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            builder.appendBoolean(BOOL_VALUES[i]);
        }
        return new Page(builder.build());
    }

    private static void checkBoolean(Page actual) {
        BytesRef scratch = new BytesRef();
        BytesRefVector v = actual.<BytesRefBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            BytesRef expected = new BytesRef(String.valueOf(BOOL_VALUES[i]));
            BytesRef got = v.getBytesRef(i, scratch);
            if (got.equals(expected) == false) {
                throw new AssertionError(
                    "fromBoolean position [" + i + "] expected [" + expected.utf8ToString() + "] but was [" + got.utf8ToString() + "]"
                );
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void fromBoolean(Blackhole bh) {
        bh.consume(runBoolean());
    }

    private static Object runBoolean() {
        try (var operator = new EvalOperator(driverContext, toStringFromBooleanEvaluator())) {
            Page page = boolPage();
            Page output = null;
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page);
                output = operator.getOutput();
            }
            checkBoolean(output);
            return output;
        }
    }

    // -----------------------------------------------------------------------
    // FromUnsignedLong — ToStringFromUnsignedLongEvaluator
    // -----------------------------------------------------------------------

    private static ExpressionEvaluator toStringFromUnsignedLongEvaluator() {
        FieldAttribute ulField = new FieldAttribute(
            Source.EMPTY,
            "ul",
            new EsField("ul", DataType.UNSIGNED_LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        ExpressionEvaluator evaluator = EvalMapper.toEvaluator(
            FOLD_CONTEXT,
            new ToString(Source.EMPTY, ulField, configuration()),
            layout(ulField)
        ).get(driverContext);
        if (evaluator.toString().contains("ToStringFromUnsignedLongEvaluator") == false) {
            throw new IllegalArgumentException("Expected ToStringFromUnsignedLongEvaluator but got [" + evaluator + "]");
        }
        return evaluator;
    }

    private static Page unsignedLongPage() {
        var builder = blockFactory.newLongBlockBuilder(BLOCK_LENGTH);
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            builder.appendLong(UNSIGNED_LONG_VALUES[i]);
        }
        return new Page(builder.build());
    }

    private static void checkUnsignedLong(Page actual) {
        BytesRef scratch = new BytesRef();
        BytesRefVector v = actual.<BytesRefBlock>getBlock(1).asVector();
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            // UNSIGNED_LONG_VALUES are stored biased (^ Long.MIN_VALUE); XOR recovers the
            // original unsigned value for comparison. All test values fit in a positive long.
            BytesRef expected = new BytesRef(Long.toUnsignedString(UNSIGNED_LONG_VALUES[i] ^ Long.MIN_VALUE));
            BytesRef got = v.getBytesRef(i, scratch);
            if (got.equals(expected) == false) {
                throw new AssertionError(
                    "fromUnsignedLong position [" + i + "] expected [" + expected.utf8ToString() + "] but was [" + got.utf8ToString() + "]"
                );
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(1024 * BLOCK_LENGTH)
    public void fromUnsignedLong(Blackhole bh) {
        bh.consume(runUnsignedLong());
    }

    private static Object runUnsignedLong() {
        try (var operator = new EvalOperator(driverContext, toStringFromUnsignedLongEvaluator())) {
            Page page = unsignedLongPage();
            Page output = null;
            for (int i = 0; i < 1024; i++) {
                operator.addInput(page);
                output = operator.getOutput();
            }
            checkUnsignedLong(output);
            return output;
        }
    }
}
