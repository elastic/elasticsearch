/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.BoundaryBlockSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.ConstantIntegerSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.CounterWithResetsSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecreasingIntegerSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.GaugeLikeSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.GcdFriendlySupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.IncreasingIntegerSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.LowCardinalitySupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.NearConstantWithOutliersSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.NonSortedIntegerSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.TimestampLikeSupplier;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Per-stage encode benchmark for the ES95 pipeline.
 *
 * <p>Each {@code stage} parameter builds a minimal pipeline that contains only the named
 * transform plus the {@code bitpack} payload, so the throughput score isolates that stage's
 * contribution. The {@code pattern} parameter feeds inputs that exercise both the apply
 * path and the skip path of every transform, so a regression in any stage shows up as a
 * slowdown on the stage's row for the relevant pattern.
 *
 * <p>{@code full} runs the production {@code delta>offset>gcd>bitpack} pipeline so the
 * per-pattern numbers can be compared against the realistic refresh workload.
 *
 * <h2>Stages</h2>
 * <ul>
 *   <li>{@code delta} - just {@code delta>bitpack}</li>
 *   <li>{@code offset} - just {@code offset>bitpack}</li>
 *   <li>{@code gcd} - just {@code gcd>bitpack}</li>
 *   <li>{@code splitDelta} - just {@code splitDelta>bitpack}</li>
 *   <li>{@code bitpackOnly} - just {@code bitpack}</li>
 *   <li>{@code full} - {@code delta>offset>gcd>bitpack} (production)</li>
 * </ul>
 *
 * <h2>Patterns</h2>
 * <ul>
 *   <li>{@code monotonic} - sorted increasing integers (delta applies, gcd usually skips)</li>
 *   <li>{@code decreasing} - sorted decreasing integers (delta applies in reverse direction)</li>
 *   <li>{@code random} - random unsorted integers (delta skips, gcd scans to 1 quickly)</li>
 *   <li>{@code gcdFriendly} - random multiples of 100 (gcd applies)</li>
 *   <li>{@code constant} - all values identical (most stages collapse)</li>
 *   <li>{@code gauge} - non-monotonic oscillation around a center (delta skips, offset applies)</li>
 *   <li>{@code lowCardinality} - small palette of values (bitpack uses few bits)</li>
 *   <li>{@code counterWithResets} - monotonic counter with occasional drops</li>
 *   <li>{@code nearConstant} - mostly the same value with rare outliers (offset case)</li>
 *   <li>{@code timestampLike} - timestamps with small jitter around a fixed delta</li>
 *   <li>{@code tsdbBoundary} - TSDB descending block with one upward boundary jump (k=1, the SplitDelta common case)</li>
 *   <li>{@code tsdbMultiBoundary} - TSDB block with four boundary jumps (k=4)</li>
 * </ul>
 *
 * <h2>Ready to run commands</h2>
 *
 * <pre>{@code
 * # Full stage x pattern matrix (5 x 10 = 50 rows, ~10 minutes)
 * ./gradlew :benchmarks:run --args="EncodePipelineStageBenchmark"
 *
 * # Just the production pipeline across all patterns
 * ./gradlew :benchmarks:run --args="EncodePipelineStageBenchmark -p stage=full"
 *
 * # Drill into a single stage (e.g. spot regressions in gcd)
 * ./gradlew :benchmarks:run --args="EncodePipelineStageBenchmark -p stage=gcd"
 *
 * # Compare gcd against a known-friendly input vs no common factor
 * ./gradlew :benchmarks:run --args="EncodePipelineStageBenchmark -p stage=gcd -p pattern=random,gcdFriendly"
 *
 * # Allocation rate per pattern (uses JMH built in GC profiler)
 * ./gradlew :benchmarks:run --args="EncodePipelineStageBenchmark -p stage=full -prof gc"
 *
 * # Quick smoke (1 warmup, 1 measurement iteration, 1 fork)
 * ./gradlew :benchmarks:run --args="EncodePipelineStageBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1"
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class EncodePipelineStageBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final int SEED = 17;
    private static final int EXTRA_METADATA_SIZE = 512;
    private static final int RANDOM_INTEGER_BITS = 32;

    @Param({ "delta", "offset", "gcd", "splitDelta", "bitpackOnly", "full" })
    private String stage;

    @Param(
        {
            "monotonic",
            "decreasing",
            "random",
            "gcdFriendly",
            "constant",
            "gauge",
            "lowCardinality",
            "counterWithResets",
            "nearConstant",
            "timestampLike",
            "tsdbBoundary",
            "tsdbMultiBoundary" }
    )
    private String pattern;

    @Param({ "100" })
    private int blocksPerInvocation;

    private int blockSize;
    private long[][] inputs;
    private long[] template;
    private byte[][] outputBuffers;
    private ByteArrayDataOutput[] outputs;
    private NumericBlockEncoder blockEncoder;

    @Setup(Level.Trial)
    public void setupTrial() {
        blockSize = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;
        template = supplierFor(pattern, blockSize).get();

        inputs = new long[blocksPerInvocation][blockSize];
        outputBuffers = new byte[blocksPerInvocation][Long.BYTES * blockSize + EXTRA_METADATA_SIZE];
        outputs = new ByteArrayDataOutput[blocksPerInvocation];
        for (int i = 0; i < blocksPerInvocation; i++) {
            outputs[i] = new ByteArrayDataOutput(outputBuffers[i]);
        }

        final PipelineConfig config = configFor(stage, blockSize);
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        blockEncoder = encoder.newBlockEncoder();
    }

    @Benchmark
    public void encode(final Blackhole bh) throws IOException {
        for (int i = 0; i < blocksPerInvocation; i++) {
            System.arraycopy(template, 0, inputs[i], 0, blockSize);
            outputs[i].reset(outputBuffers[i]);
            blockEncoder.encode(inputs[i], blockSize, outputs[i]);
            bh.consume(outputs[i].getPosition());
        }
    }

    private static PipelineConfig configFor(final String stage, final int blockSize) {
        return switch (stage) {
            case "delta" -> PipelineConfig.forLongs(blockSize).delta().bitPack();
            case "offset" -> PipelineConfig.forLongs(blockSize).offset().bitPack();
            case "gcd" -> PipelineConfig.forLongs(blockSize).gcd().bitPack();
            case "splitDelta" -> PipelineConfig.forLongs(blockSize).splitDelta().bitPack();
            case "bitpackOnly" -> PipelineConfig.forLongs(blockSize).bitPack();
            case "full" -> PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
            default -> throw new IllegalArgumentException("Unknown stage: " + stage);
        };
    }

    private static Supplier<long[]> supplierFor(final String pattern, final int size) {
        return switch (pattern) {
            case "monotonic" -> new IncreasingIntegerSupplier(SEED, RANDOM_INTEGER_BITS, size);
            case "decreasing" -> new DecreasingIntegerSupplier(SEED, RANDOM_INTEGER_BITS, size);
            case "random" -> new NonSortedIntegerSupplier(SEED, RANDOM_INTEGER_BITS, size);
            case "gcdFriendly" -> GcdFriendlySupplier.builder(SEED, size).withGcd(100L).build();
            case "constant" -> new ConstantIntegerSupplier(SEED, RANDOM_INTEGER_BITS, size);
            case "gauge" -> GaugeLikeSupplier.builder(SEED, size).build();
            case "lowCardinality" -> LowCardinalitySupplier.builder(SEED, size).build();
            case "counterWithResets" -> CounterWithResetsSupplier.builder(SEED, size).build();
            case "nearConstant" -> NearConstantWithOutliersSupplier.builder(SEED, size).build();
            case "timestampLike" -> TimestampLikeSupplier.builder(SEED, size).build();
            case "tsdbBoundary" -> BoundaryBlockSupplier.builder(SEED, size).withFlips(1).build();
            case "tsdbMultiBoundary" -> BoundaryBlockSupplier.builder(SEED, size).withFlips(4).build();
            default -> throw new IllegalArgumentException("Unknown pattern: " + pattern);
        };
    }
}
