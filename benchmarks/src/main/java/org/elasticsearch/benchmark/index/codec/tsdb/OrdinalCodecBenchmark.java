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
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.es95.SortedOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.es95.SortedSetOrdinalCodec;
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
 * Benchmark matrix comparing the legacy {@link TSDBDocValuesEncoder} ordinal
 * encoder against the per-field-type adaptive encoders: {@link SortedOrdinalCodec}
 * for the {@code TSID_RUNS} shape (SORTED fields) and {@link SortedSetOrdinalCodec}
 * for the {@code MULTIVALUE_CYCLE} shape (SORTED_SET fields with the K-cycle
 * pattern). Two cardinality regimes:
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
 * ./gradlew :benchmarks:run --args="OrdinalCodecBenchmark \
 *   -wi 5 -i 5 -f 3"
 * }</pre>
 */
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class OrdinalCodecBenchmark {

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

    /**
     * Block content shape:
     *
     * <ul>
     *   <li>{@code TSID_RUNS}: {@code runsPerBlock} contiguous equal-ordinal runs.
     *       Models a single-valued field (sorted) across one or a few tsid boundaries.</li>
     *   <li>{@code MULTIVALUE_CYCLE}: a cycle of period {@code runsPerBlock} repeating
     *       across the 128-element block. Models a multi-valued sorted-set field
     *       (host.ip, host.mac) where each doc emits {@code runsPerBlock} ordinals
     *       drawn from a fixed set and consecutive docs in the same tsid share the
     *       set, producing a perfect cycle in the flat ord stream.</li>
     * </ul>
     */
    public enum BlockShape {
        TSID_RUNS,
        MULTIVALUE_CYCLE
    }

    static final int BLOCK_SIZE = 128;
    static final int BLOCKS_PER_INVOCATION = 10;
    static final int OUTPUT_BUFFER_BYTES = BLOCK_SIZE * Long.BYTES + 64;
    private static final int SEED = 17;

    @Param({ "LOW", "HIGH" })
    private Cardinality cardinality;

    @Param({ "TSID_RUNS", "MULTIVALUE_CYCLE" })
    private BlockShape blockShape;

    // NOTE: For TSID_RUNS, this is the number of distinct tsids represented in the block,
    // modeled as the number of contiguous equal-ordinal runs. 1 is the typical mid-tsid
    // block (CONST candidate); 2 is a single boundary; 4 and 6 are heavier boundary blocks.
    // For MULTIVALUE_CYCLE, this is the cycle period K (i.e., the number of ordinals each
    // doc emits, repeating across consecutive docs of the same tsid). 1 reduces to CONST;
    // 2, 3, 5 are typical for multi-valued fields like host.ip (~3 values per doc).
    @Param({ "1", "2", "4", "6" })
    private int runsPerBlock;

    private int bitsPerOrd;
    private long[] inputBlock;
    private long[] scratchBlock;
    private byte[] outputBuffer;
    private ByteArrayDataOutput dataOutput;

    private TSDBDocValuesEncoder tsdbOrdinalCodec;
    private BlockEncoder adaptiveOrdinalCodec;

    /**
     * Encoder shape shared by {@link SortedOrdinalCodec#encodeOrdinals} and
     * {@link SortedSetOrdinalCodec#encodeOrdinals}, so the benchmark methods can
     * stay shape-agnostic and the right codec is wired up once in
     * {@link #setupTrial} based on the {@link BlockShape} param.
     */
    @FunctionalInterface
    private interface BlockEncoder {
        void encode(long[] in, DataOutput out, int bitsPerOrd) throws IOException;
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        bitsPerOrd = cardinality.bitsPerOrd;
        inputBlock = switch (blockShape) {
            case TSID_RUNS -> generateTsidRunBlock(bitsPerOrd, runsPerBlock);
            case MULTIVALUE_CYCLE -> generateMultivalueCycleBlock(bitsPerOrd, runsPerBlock);
        };
        scratchBlock = new long[BLOCK_SIZE];
        outputBuffer = new byte[OUTPUT_BUFFER_BYTES];
        dataOutput = new ByteArrayDataOutput(outputBuffer);

        tsdbOrdinalCodec = new TSDBDocValuesEncoder(BLOCK_SIZE);
        adaptiveOrdinalCodec = switch (blockShape) {
            case TSID_RUNS -> new SortedOrdinalCodec(BLOCK_SIZE)::encodeOrdinals;
            case MULTIVALUE_CYCLE -> new SortedSetOrdinalCodec(BLOCK_SIZE)::encodeOrdinals;
        };
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
            adaptiveOrdinalCodec.encode(scratchBlock, dataOutput, bitsPerOrd);
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
        adaptiveOrdinalCodec.encode(scratchBlock, dataOutput, bitsPerOrd);
        final int adaptiveBytes = dataOutput.getPosition();

        final double ratio = tsdbBytes == 0 ? Double.NaN : ((double) adaptiveBytes) / tsdbBytes;
        System.out.printf(
            Locale.ROOT,
            "[storage] cardinality=%s bitsPerOrd=%d blockShape=%s runsPerBlock=%d tsdbBytes=%d adaptiveBytes=%d adaptive/tsdb=%.4f%n",
            cardinality,
            bitsPerOrd,
            blockShape,
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

    // NOTE: Multi-valued SortedSetDocValuesField shape. Each doc emits `cyclePeriod` distinct
    // ordinals; consecutive docs in the same tsid share the same K-value set, so the flat ord
    // stream is `[v0, v1, ..., v(K-1), v0, v1, ...]` with period K. Cycle ordinals are spread
    // evenly across the full segment-global bit range to model the realistic case where the K
    // values come from terms that sort far apart in the term dictionary: e.g. the three IPs
    // emitted by hostmetricsreceiver for a single host (loopback `127.0.0.1`, a private
    // `10.x.x.x`, a docker `172.17.x.x`) all sort into completely different positions in the
    // dictionary, so `max - min` is essentially the full bit range and BITPACK_LOCAL cannot
    // absorb the cost. This is the scenario where legacy's CYCLE detection wins big.
    private static long[] generateMultivalueCycleBlock(int bitsPerOrd, int cyclePeriod) {
        final long mask = bitsPerOrd >= 63 ? Long.MAX_VALUE : (1L << bitsPerOrd) - 1L;
        final long[] cycleValues = new long[cyclePeriod];
        for (int i = 0; i < cyclePeriod; i++) {
            cycleValues[i] = (mask / Math.max(1, cyclePeriod)) * i;
        }
        final long[] out = new long[BLOCK_SIZE];
        for (int i = 0; i < out.length; i++) {
            out[i] = cycleValues[i % cyclePeriod];
        }
        return out;
    }
}
