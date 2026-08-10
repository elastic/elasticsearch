/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.columnar.numeric.AlpDoubleTransform;
import org.elasticsearch.columnar.numeric.BlockTransform;
import org.elasticsearch.columnar.numeric.DeltaTransform;
import org.elasticsearch.columnar.numeric.ForTerminal;
import org.elasticsearch.columnar.numeric.GcdTransform;
import org.elasticsearch.columnar.numeric.NumericBlockEncoder;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.OffsetTransform;
import org.elasticsearch.columnar.numeric.SplitDeltaTransform;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * Shared state and configuration for {@link EncodeBlockTransformBenchmark} and
 * {@link DecodeBlockTransformBenchmark}.
 *
 * <p>Each stage entry in {@link #PIPELINE_FACTORIES} is the single source of truth for the
 * available stages. The {@code stage} {@link Param} must list the same keys; {@link #encoderFor}
 * enforces this at setup time by throwing for any unknown value.
 *
 * <h2>Stages</h2>
 *
 * <p>Each single-stage entry pairs the named transform with a {@link RawTerminal} (raw 8-byte
 * longs, no bit-packing), so the throughput score reflects only that stage's work. {@code for}
 * runs the FOR bit-packer alone with no preceding transform. The composed pipeline cost is
 * covered by the end-to-end benchmarks ({@code ColumnarNumericIngestBenchmark}).
 *
 * <ul>
 *   <li>{@code delta} - DeltaTransform + raw terminal</li>
 *   <li>{@code offset} - OffsetTransform + raw terminal</li>
 *   <li>{@code gcd} - GcdTransform + raw terminal</li>
 *   <li>{@code splitDelta} - SplitDeltaTransform + raw terminal</li>
 *   <li>{@code alp} - AlpDoubleTransform + raw terminal</li>
 *   <li>{@code for} - FOR bit-packing alone</li>
 * </ul>
 *
 * <h2>Patterns</h2>
 * <ul>
 *   <li>{@code MONOTONIC_TIMESTAMPS} - strictly increasing timestamps (delta applies)</li>
 *   <li>{@code COUNTER_STEADY} - linear counter (delta + gcd apply)</li>
 *   <li>{@code GAUGE} - non-monotonic oscillation around a centre (delta skips, offset applies)</li>
 *   <li>{@code TSDB_SPLIT} - four descending runs with upward jumps (splitDelta applies)</li>
 *   <li>{@code SENSOR_DOUBLES} - sortable longs for one-decimal IEEE doubles (alp applies)</li>
 *   <li>{@code RANDOM_FULL} - full-width random longs (most stages skip)</li>
 *   <li>{@code CONSTANT} - all values identical (delta→0, gcd→max, FOR needs 1 bit; collapse baseline)</li>
 *   <li>{@code DECREASING} - strictly descending values (delta produces all-negative deltas)</li>
 *   <li>{@code GCD_FRIENDLY} - random multiples of 1 000 000 (gcd applies strongly)</li>
 *   <li>{@code NEAR_CONSTANT_OUTLIERS} - base value with ~5% wide outliers (offset applies)</li>
 * </ul>
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public abstract class BlockTransformBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    // NOTE: ALP metadata is at most e/f (2B) + vint count (5B) + exceptions * 10B. ALP fires only
    // when bestExceptions <= bitsSaved * blockSize / 160, so the theoretical maximum is well below
    // blockSize * 10. For the current workloads SENSOR_DOUBLES produces 0 ALP exceptions and every
    // other pattern causes ALP to decline, making 1024 B sufficient across all supported block sizes.
    static final int EXTRA_METADATA_SIZE = 1024;

    /**
     * Maps each stage name to a factory that builds the corresponding pipeline given a block size.
     * This is the single source of truth for available stages; the {@code stage} {@link Param}
     * must list the same keys.
     */
    private static final Map<String, IntFunction<NumericPipeline>> PIPELINE_FACTORIES;
    static {
        PIPELINE_FACTORIES = new LinkedHashMap<>();
        PIPELINE_FACTORIES.put(
            "delta",
            bs -> new NumericPipeline(new BlockTransform[] { DeltaTransform.INSTANCE }, RawTerminal.INSTANCE, bs)
        );
        PIPELINE_FACTORIES.put(
            "offset",
            bs -> new NumericPipeline(new BlockTransform[] { OffsetTransform.INSTANCE }, RawTerminal.INSTANCE, bs)
        );
        PIPELINE_FACTORIES.put("gcd", bs -> new NumericPipeline(new BlockTransform[] { GcdTransform.INSTANCE }, RawTerminal.INSTANCE, bs));
        PIPELINE_FACTORIES.put(
            "splitDelta",
            bs -> new NumericPipeline(new BlockTransform[] { new SplitDeltaTransform() }, RawTerminal.INSTANCE, bs)
        );
        PIPELINE_FACTORIES.put(
            "alp",
            bs -> new NumericPipeline(new BlockTransform[] { new AlpDoubleTransform(bs) }, RawTerminal.INSTANCE, bs)
        );
        PIPELINE_FACTORIES.put("for", bs -> new NumericPipeline(new BlockTransform[] {}, new ForTerminal(bs), bs));
    }

    // keep in sync with PIPELINE_FACTORIES.keySet()
    @Param({ "delta", "offset", "gcd", "splitDelta", "alp", "for" })
    protected String stage;

    @Param(
        {
            "MONOTONIC_TIMESTAMPS",
            "COUNTER_STEADY",
            "GAUGE",
            "LOW_CARDINALITY",
            "SMALL_INTS",
            "TSDB_SPLIT",
            "SENSOR_DOUBLES",
            "RANDOM_FULL",
            "CONSTANT",
            "DECREASING",
            "GCD_FRIENDLY",
            "NEAR_CONSTANT_OUTLIERS" }
    )
    protected String pattern;

    @Param({ "512", "8192" })
    protected int blockSize;

    /**
     * Number of blocks encoded or decoded per JMH invocation.
     *
     * <p>The value is a deliberate balance between two opposing forces. Too few blocks and the
     * per-invocation overhead (the JMH measurement loop, {@code Blackhole} consumption, the
     * pre-invocation copy and cursor-reset) becomes a significant fraction of measured time,
     * inflating noise and making small regressions invisible. Too many blocks and the combined
     * input and output arrays exceed cache capacity, causing the score to reflect
     * memory-bandwidth pressure rather than the transform's compute cost.
     *
     * <p>At 100 blocks and the default block size of 512 longs, the combined input and output
     * working set is approximately 900 KB, which targets L3 on typical benchmark hardware. This
     * matches the value used in the TSDB per-stage benchmarks. A smaller value (e.g. {@code 8})
     * keeps data in L2 and isolates pure compute throughput at the cost of less stable scores.
     */
    @Param({ "100" })
    protected int blocksPerInvocation;

    protected NumericBlockEncoder blockEncoder;

    static NumericBlockEncoder encoderFor(String stage, int blockSize) {
        IntFunction<NumericPipeline> factory = PIPELINE_FACTORIES.get(stage);
        if (factory == null) {
            throw new IllegalArgumentException("Unknown stage '" + stage + "'; valid stages: " + PIPELINE_FACTORIES.keySet());
        }
        return new NumericBlockEncoder(factory.apply(blockSize), blockSize);
    }
}
