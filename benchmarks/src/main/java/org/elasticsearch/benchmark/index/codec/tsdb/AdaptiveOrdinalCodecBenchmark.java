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
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.es95.AdaptiveOrdinalCodec;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Four-cell benchmark matrix comparing the legacy {@link TSDBDocValuesEncoder}
 * ordinal encoder (used by {@code TSDBOrdinalBlockCodec}) and the new
 * {@link AdaptiveOrdinalCodec} (used by {@code AdaptiveOrdinalBlockCodec})
 * under two cardinality regimes:
 *
 * <ul>
 *   <li>{@code LOW} models a segment with ~16 unique ordinals
 *       ({@code bitsPerOrd = 4}); the headline goal here is "no regression".</li>
 *   <li>{@code HIGH} models a segment with ~65536 unique ordinals
 *       ({@code bitsPerOrd = 16}); the headline goal here is "measurable
 *       improvement" thanks to {@code BITPACK_LOCAL} on tsid-run blocks whose
 *       local value range is far narrower than the segment-global one.</li>
 * </ul>
 *
 * <p>For both regimes the per-block input is a {@code NARROW_RANGE} pattern:
 * 128 ordinals drawn from a 64-wide window. This shape mimics a single tsid
 * run inside a TSDB segment, where consecutive documents carry the same
 * keyword dimension value across long stretches but the segment as a whole
 * holds many more unique values. The local 6-bit window plus the
 * segment-global {@code bitsPerOrd} gap is exactly what {@code BITPACK_LOCAL}
 * is designed to exploit.
 *
 * <p>JMH primary score: encode throughput. Secondary aux counter:
 * {@code encodedBytes} captures total bytes the encoder writes; dividing it
 * by the primary score yields per-block bytes.
 *
 * <h2>Ready to run command</h2>
 *
 * <pre>{@code
 * ./gradlew :benchmarks:run --args="AdaptiveOrdinalCodecBenchmark \
 *   -wi 5 -i 5 -f 3"
 * }</pre>
 */
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class AdaptiveOrdinalCodecBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    public enum Cardinality {
        LOW(4),
        HIGH(16);

        final int bitsPerOrd;

        Cardinality(int bitsPerOrd) {
            this.bitsPerOrd = bitsPerOrd;
        }
    }

    static final int BLOCK_SIZE = 128;
    static final int BLOCKS_PER_INVOCATION = 10;
    static final int OUTPUT_BUFFER_BYTES = BLOCK_SIZE * Long.BYTES + 64;
    private static final int SEED = 17;

    @Param({ "LOW", "HIGH" })
    private Cardinality cardinality;

    // NOTE: number of distinct tsids represented in the block, modeled as the number of
    // contiguous equal-ordinal runs. 1 is the typical mid-tsid block (all docs share one
    // tsid -> one ordinal -> CONST mode candidate); 2 is a single boundary block; 4 and 6
    // are heavier boundary blocks that exercise RLE / BITPACK_LOCAL more aggressively.
    @Param({ "1", "2", "4", "6" })
    private int runsPerBlock;

    private int bitsPerOrd;
    private long[] inputBlock;
    private long[] scratchBlock;
    private byte[] outputBuffer;
    private ByteArrayDataOutput dataOutput;

    private TSDBDocValuesEncoder tsdbOrdinalCodec;
    private AdaptiveOrdinalCodec adaptiveOrdinalCodec;

    @Setup(Level.Trial)
    public void setupTrial() {
        bitsPerOrd = cardinality.bitsPerOrd;
        inputBlock = generateTsidRunBlock(bitsPerOrd, runsPerBlock);
        scratchBlock = new long[BLOCK_SIZE];
        outputBuffer = new byte[OUTPUT_BUFFER_BYTES];
        dataOutput = new ByteArrayDataOutput(outputBuffer);

        tsdbOrdinalCodec = new TSDBDocValuesEncoder(BLOCK_SIZE);
        adaptiveOrdinalCodec = new AdaptiveOrdinalCodec(BLOCK_SIZE);
    }

    /**
     * Secondary metric reported in the JMH JSON: total bytes the encoder under
     * test writes. With {@link AuxCounters.Type#OPERATIONS} in Throughput
     * mode the reported value is {@code totalBytes / elapsedTime}, so dividing
     * by the primary throughput gives the per-block byte count.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    public static class StorageMetric {

        public long encodedBytes;

        @Setup(Level.Iteration)
        public void reset() {
            encodedBytes = 0;
        }
    }

    @Benchmark
    @OperationsPerInvocation(BLOCKS_PER_INVOCATION)
    public void tsdbOrdinalCodecEncode(StorageMetric metric, Blackhole bh) throws IOException {
        long total = 0;
        for (int i = 0; i < BLOCKS_PER_INVOCATION; i++) {
            System.arraycopy(inputBlock, 0, scratchBlock, 0, BLOCK_SIZE);
            dataOutput.reset(outputBuffer);
            tsdbOrdinalCodec.encodeOrdinals(scratchBlock, dataOutput, bitsPerOrd);
            total += dataOutput.getPosition();
        }
        metric.encodedBytes += total;
        bh.consume(total);
    }

    @Benchmark
    @OperationsPerInvocation(BLOCKS_PER_INVOCATION)
    public void adaptiveOrdinalCodecEncode(StorageMetric metric, Blackhole bh) throws IOException {
        long total = 0;
        for (int i = 0; i < BLOCKS_PER_INVOCATION; i++) {
            System.arraycopy(inputBlock, 0, scratchBlock, 0, BLOCK_SIZE);
            dataOutput.reset(outputBuffer);
            adaptiveOrdinalCodec.encodeOrdinals(scratchBlock, dataOutput, bitsPerOrd);
            total += dataOutput.getPosition();
        }
        metric.encodedBytes += total;
        bh.consume(total);
    }

    @TearDown(Level.Trial)
    public void reportEncodedSizes() throws IOException {
        // NOTE: emit a deterministic [storage] line per cardinality combo, capturing exact
        // per-block byte counts for both encoders. Grep-friendly summary; captured by the
        // runner script via JMH's `-o` stdout file.
        System.arraycopy(inputBlock, 0, scratchBlock, 0, BLOCK_SIZE);
        dataOutput.reset(outputBuffer);
        tsdbOrdinalCodec.encodeOrdinals(scratchBlock, dataOutput, bitsPerOrd);
        final int tsdbBytes = dataOutput.getPosition();

        System.arraycopy(inputBlock, 0, scratchBlock, 0, BLOCK_SIZE);
        dataOutput.reset(outputBuffer);
        adaptiveOrdinalCodec.encodeOrdinals(scratchBlock, dataOutput, bitsPerOrd);
        final int adaptiveBytes = dataOutput.getPosition();

        final double ratio = tsdbBytes == 0 ? Double.NaN : ((double) adaptiveBytes) / tsdbBytes;
        System.out.printf(
            Locale.ROOT,
            "[storage] cardinality=%s bitsPerOrd=%d runsPerBlock=%d tsdbBytes=%d adaptiveBytes=%d adaptive/tsdb=%.4f%n",
            cardinality,
            bitsPerOrd,
            runsPerBlock,
            tsdbBytes,
            adaptiveBytes,
            ratio
        );
    }

    // NOTE: TSDB index-sort groups docs by `_tsid`, so within a doc-values block consecutive
    // docs that belong to the same tsid share the same ordinal. A block at a tsid run boundary
    // therefore looks like a small number of CONTIGUOUS RUNS of identical ordinals, not random
    // values. We model a block that crosses `runsPerBlock` tsid runs, each contributing one
    // ordinal drawn from a 64-wide window inside the segment-global bit range. runsPerBlock = 1
    // models the dominant mid-tsid case; larger runsPerBlock values exercise the boundary cases.
    private static long[] generateTsidRunBlock(int bitsPerOrd, int runsPerBlock) {
        final Random random = new Random(SEED);
        final long mask = bitsPerOrd >= 63 ? Long.MAX_VALUE : (1L << bitsPerOrd) - 1L;
        final long base = Math.floorMod(random.nextLong(), Math.max(1L, mask - 64L));
        final int runLength = BLOCK_SIZE / runsPerBlock;
        final long[] out = new long[BLOCK_SIZE];
        for (int r = 0; r < runsPerBlock; r++) {
            final long ordForRun = base + (random.nextLong() & 0x3F);
            final int start = r * runLength;
            final int end = (r == runsPerBlock - 1) ? BLOCK_SIZE : start + runLength;
            Arrays.fill(out, start, end, ordForRun);
        }
        return out;
    }
}
