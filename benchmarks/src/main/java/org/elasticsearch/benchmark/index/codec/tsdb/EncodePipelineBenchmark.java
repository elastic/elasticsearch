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
import org.elasticsearch.benchmark.index.codec.tsdb.internal.ConstantIntegerSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.ConstantIntervalSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.ConstantRateOfChangeSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.CounterWithResetsSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.DecreasingIntegerSupplier;
import org.elasticsearch.benchmark.index.codec.tsdb.internal.EventDrivenSupplier;
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
import org.openjdk.jmh.annotations.AuxCounters;
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
 * Multi-stage pipeline encode benchmark for the ES95 pipeline.
 *
 * <p>Sibling of {@link EncodePipelineStageBenchmark}, which isolates single transforms.
 * This class runs full production-shaped pipelines so the per-pattern numbers can be
 * compared against the realistic refresh workload and against design alternatives.
 *
 * <p>Reports average time per invocation in microseconds. Each invocation encodes
 * {@code blocksPerInvocation} blocks, so multiply the score by
 * {@code 1000 / blocksPerInvocation} to get nanoseconds per 128 value block. The
 * latency framing matches how the codec is consumed (cost per encoded block) and
 * lines up with the {@code bytesPerBlock} aux counter that reports compressed size
 * on the same axis.
 *
 * <p>{@code full} is the baseline production pipeline for non {@code @timestamp} fields.
 * {@code fullTimestamp} is the production pipeline routed for the {@code @timestamp} field.
 * {@code fullCascade} is the hypothetical alternative that would result from removing
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaOfDeltaCodecStage}
 * and applying {@link org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages.DeltaCodecStage}
 * twice instead. The pair {@code fullTimestamp} vs {@code fullCascade} measures the cost of
 * running two sequential delta stages vs one dedicated stage, plus the downstream offset and
 * gcd work.
 *
 * <h2>Pipelines</h2>
 * <ul>
 *   <li>{@code full} - {@code delta>offset>gcd>bitpack} (baseline production)</li>
 *   <li>{@code fullTimestamp} - {@code deltaOfDelta>offset>bitpack} ({@code @timestamp} production)</li>
 *   <li>{@code fullCascade} - {@code delta>delta>offset>gcd>bitpack} (two sequential {@code delta} stages instead of one dedicated {@code deltaOfDelta})</li>
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
 *   <li>{@code timestampLike} - timestamps with small jitter around a fixed delta
 *       (the second {@code delta} stage of {@code fullCascade} gates off on jitter and skips
 *       on this pattern)</li>
 *   <li>{@code constantInterval} - timestamps spaced by a perfectly constant interval, no jitter
 *       (idealized scrape input; {@code delta>offset} collapses to zeros, {@code deltaOfDelta}
 *       reaches the same result in one stage)</li>
 *   <li>{@code eventDriven} - irregular bursty timestamps, modeling event ingest (logs, traces)
 *       where short bursts are separated by occasional longer gaps</li>
 *   <li>{@code constantRateOfChange} - timestamps whose interval grows by a fixed amount per
 *       step (jitter free; both {@code delta} stages of {@code fullCascade} gate on, exercising
 *       the cost of running two sequential transforms vs one dedicated {@code deltaOfDelta})</li>
 * </ul>
 *
 * <h2>Ready to run commands</h2>
 *
 * <pre>{@code
 * # Full pipeline x pattern matrix (3 x 13 = 39 rows)
 * ./gradlew :benchmarks:run --args="EncodePipelineBenchmark"
 *
 * # Realistic @timestamp shape (the second delta in fullCascade skips on jitter)
 * ./gradlew :benchmarks:run --args="EncodePipelineBenchmark -p pattern=timestampLike"
 *
 * # Compare fullTimestamp vs fullCascade when both delta stages actually apply
 * ./gradlew :benchmarks:run --args="EncodePipelineBenchmark -p pipeline=fullTimestamp,fullCascade -p pattern=constantRateOfChange"
 *
 * # Allocation rate per pattern (uses JMH built in GC profiler)
 * ./gradlew :benchmarks:run --args="EncodePipelineBenchmark -prof gc"
 *
 * # Quick smoke (1 warmup, 1 measurement iteration, 1 fork)
 * ./gradlew :benchmarks:run --args="EncodePipelineBenchmark -wi 1 -i 1 -f 1 -w 1 -r 1"
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class EncodePipelineBenchmark {

    private static final int SEED = 17;
    private static final int EXTRA_METADATA_SIZE = 64;
    private static final int RANDOM_INTEGER_BITS = 32;

    @Param({ "full", "fullTimestamp", "fullCascade" })
    private String pipeline;

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
            "constantInterval",
            "eventDriven",
            "constantRateOfChange" }
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

        final PipelineConfig config = configFor(pipeline, blockSize);
        final NumericEncoder encoder = NumericCodecFactory.DEFAULT.createEncoder(config);
        blockEncoder = encoder.newBlockEncoder();
    }

    @Benchmark
    public void encode(final Blackhole bh, final Counters counters) throws IOException {
        for (int i = 0; i < blocksPerInvocation; i++) {
            System.arraycopy(template, 0, inputs[i], 0, blockSize);
            outputs[i].reset(outputBuffers[i]);
            blockEncoder.encode(inputs[i], blockSize, outputs[i]);
            counters.bytesPerBlock = outputs[i].getPosition();
            bh.consume(outputs[i].getPosition());
        }
    }

    /**
     * Reports the encoded size per 128 value block alongside the average time score. The size is
     * deterministic given the pipeline and pattern, so every invocation overwrites the same value
     * and JMH reports it as the final per iteration reading.
     */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Counters {
        public int bytesPerBlock;
    }

    private static PipelineConfig configFor(final String pipeline, final int blockSize) {
        return switch (pipeline) {
            case "full" -> PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
            case "fullTimestamp" -> PipelineConfig.forLongs(blockSize).deltaOfDelta().offset().bitPack();
            case "fullCascade" -> PipelineConfig.forLongs(blockSize).delta().delta().offset().gcd().bitPack();
            default -> throw new IllegalArgumentException("Unknown pipeline: " + pipeline);
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
            case "constantInterval" -> ConstantIntervalSupplier.builder(SEED, size).build();
            case "eventDriven" -> EventDrivenSupplier.builder(SEED, size).build();
            case "constantRateOfChange" -> ConstantRateOfChangeSupplier.builder(SEED, size).build();
            default -> throw new IllegalArgumentException("Unknown pattern: " + pattern);
        };
    }
}
