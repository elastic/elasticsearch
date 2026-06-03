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
 * for the multi-valued shapes (SORTED_SET fields). Two cardinality regimes:
 *
 * <ul>
 *   <li>{@code LOW} ({@code bitsPerOrd = 4}): the headline goal here is "no regression".</li>
 *   <li>{@code HIGH} ({@code bitsPerOrd = 16}): the headline goal is "measurable
 *       improvement" thanks to {@code BITPACK_LOCAL} on tsid-run blocks and
 *       {@code TupleRunCodec} on multi-valued cycle blocks where the K cycle ords
 *       are scattered across the segment-global bit range.</li>
 * </ul>
 *
 * <p>Block shapes:
 *
 * <ul>
 *   <li>{@code TSID_RUNS}: single-valued ords, {@code runsPerBlock} contiguous equal-ord
 *       runs (SORTED field with one or a few tsid transitions).</li>
 *   <li>{@code MULTIVALUE_CYCLE}: multi-valued ords, one tuple-run of period
 *       {@code runsPerBlock} filling the block (SORTED_SET field, pure mid-tsid block).</li>
 *   <li>{@code MULTIVALUE_BOUNDARY}: multi-valued ords with two tuple-runs of the same K,
 *       modeling a block that crosses a tsid boundary where both tsids share K.</li>
 *   <li>{@code MULTIVALUE_VARYING_K}: multi-valued ords with two tuple-runs whose K
 *       differs across the boundary, modeling docs where the second tsid emits a
 *       different number of values (e.g. one host has 3 IPs, the next has 5).</li>
 * </ul>
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

    public enum BlockShape {
        TSID_RUNS,
        MULTIVALUE_CYCLE,
        MULTIVALUE_BOUNDARY,
        MULTIVALUE_VARYING_K
    }

    static final int BLOCK_SIZE = 128;
    static final int BLOCKS_PER_INVOCATION = 10;
    static final int OUTPUT_BUFFER_BYTES = BLOCK_SIZE * Long.BYTES + 64;
    private static final int SEED = 17;

    @Param({ "LOW", "HIGH" })
    private Cardinality cardinality;

    @Param({ "TSID_RUNS", "MULTIVALUE_CYCLE", "MULTIVALUE_BOUNDARY", "MULTIVALUE_VARYING_K" })
    private BlockShape blockShape;

    // NOTE: TSID_RUNS uses this as the number of tsid runs in the block. MULTIVALUE shapes
    // use it as the K of the (first) tsid run; boundary/varying-K shapes derive the second
    // tuple deterministically from K.
    @Param({ "1", "2", "3", "4", "5", "6", "8" })
    private int runsPerBlock;

    private int bitsPerOrd;
    private long[] inputBlock;
    private int[] perDocK;
    private int numDocs;
    private int headOffset;
    private int tailMissing;
    private long[] scratchBlock;
    private byte[] outputBuffer;
    private ByteArrayDataOutput dataOutput;

    private TSDBDocValuesEncoder tsdbOrdinalCodec;
    private SortedOrdinalCodec sortedOrdinalCodec;
    private SortedSetOrdinalCodec sortedSetOrdinalCodec;

    @Setup(Level.Trial)
    public void setupTrial() {
        bitsPerOrd = cardinality.bitsPerOrd;
        final BlockInputs in = switch (blockShape) {
            case TSID_RUNS -> generateTsidRunBlock(bitsPerOrd, runsPerBlock);
            case MULTIVALUE_CYCLE -> generateMultivalueCycleBlock(bitsPerOrd, runsPerBlock);
            case MULTIVALUE_BOUNDARY -> generateMultivalueBoundaryBlock(bitsPerOrd, runsPerBlock);
            case MULTIVALUE_VARYING_K -> generateMultivalueVaryingKBlock(bitsPerOrd, runsPerBlock);
        };
        inputBlock = in.ords;
        perDocK = in.perDocK;
        numDocs = in.numDocs;
        headOffset = in.headOffset;
        tailMissing = in.tailMissing;

        scratchBlock = new long[BLOCK_SIZE];
        outputBuffer = new byte[OUTPUT_BUFFER_BYTES];
        dataOutput = new ByteArrayDataOutput(outputBuffer);

        tsdbOrdinalCodec = new TSDBDocValuesEncoder(BLOCK_SIZE);
        sortedOrdinalCodec = new SortedOrdinalCodec(BLOCK_SIZE);
        sortedSetOrdinalCodec = new SortedSetOrdinalCodec(BLOCK_SIZE);
    }

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
            encodeAdaptive();
            total += dataOutput.getPosition();
        }
        metric.encodedBytes += total;
        bh.consume(total);
    }

    private void encodeAdaptive() throws IOException {
        if (blockShape == BlockShape.TSID_RUNS) {
            sortedOrdinalCodec.encodeOrdinals(scratchBlock, dataOutput, bitsPerOrd);
        } else {
            sortedSetOrdinalCodec.encodeOrdinals(scratchBlock, perDocK, numDocs, headOffset, tailMissing, dataOutput, bitsPerOrd);
        }
    }

    @TearDown(Level.Trial)
    public void reportEncodedSizes() throws IOException {
        System.arraycopy(inputBlock, 0, scratchBlock, 0, BLOCK_SIZE);
        dataOutput.reset(outputBuffer);
        tsdbOrdinalCodec.encodeOrdinals(scratchBlock, dataOutput, bitsPerOrd);
        final int tsdbBytes = dataOutput.getPosition();

        System.arraycopy(inputBlock, 0, scratchBlock, 0, BLOCK_SIZE);
        dataOutput.reset(outputBuffer);
        encodeAdaptive();
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

    private record BlockInputs(long[] ords, int[] perDocK, int numDocs, int headOffset, int tailMissing) {}

    private static BlockInputs singleValuedBlock(long[] ords) {
        final int[] perDocK = new int[BLOCK_SIZE + 1];
        Arrays.fill(perDocK, 0, BLOCK_SIZE, 1);
        return new BlockInputs(ords, perDocK, BLOCK_SIZE, 0, 0);
    }

    private static BlockInputs generateTsidRunBlock(int bitsPerOrd, int runsPerBlock) {
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
        return singleValuedBlock(out);
    }

    private static BlockInputs generateMultivalueCycleBlock(int bitsPerOrd, int cyclePeriod) {
        final long[] tuple = scatteredTuple(bitsPerOrd, cyclePeriod, 0);
        return tupleRunBlock(tuple);
    }

    // NOTE: two tuple-runs of the same K across a tsid boundary.
    private static BlockInputs generateMultivalueBoundaryBlock(int bitsPerOrd, int K) {
        if (K < 2) {
            return generateMultivalueCycleBlock(bitsPerOrd, K);
        }
        final long[] tupleA = scatteredTuple(bitsPerOrd, K, 0);
        final long[] tupleB = scatteredTuple(bitsPerOrd, K, 1);
        return concatTupleRuns(K, tupleA, K, tupleB);
    }

    // NOTE: two tuple-runs with different K. First run K, second run K+1 (or K-1 when K==6).
    private static BlockInputs generateMultivalueVaryingKBlock(int bitsPerOrd, int K) {
        if (K < 2) {
            return generateMultivalueCycleBlock(bitsPerOrd, K);
        }
        final int K2 = K >= 6 ? K - 1 : K + 1;
        final long[] tupleA = scatteredTuple(bitsPerOrd, K, 0);
        final long[] tupleB = scatteredTuple(bitsPerOrd, K2, 1);
        return concatTupleRuns(K, tupleA, K2, tupleB);
    }

    private static long[] scatteredTuple(int bitsPerOrd, int K, int offset) {
        final long mask = bitsPerOrd >= 63 ? Long.MAX_VALUE : (1L << bitsPerOrd) - 1L;
        final long step = mask / Math.max(1, K + 2);
        final long[] tuple = new long[K];
        long cursor = step / 2 + offset * (step / 4);
        for (int i = 0; i < K; i++) {
            tuple[i] = cursor;
            cursor += step;
        }
        return tuple;
    }

    private static BlockInputs tupleRunBlock(long[] tuple) {
        final int K = tuple.length;
        final long[] out = new long[BLOCK_SIZE];
        final int[] perDocK = new int[BLOCK_SIZE + 1];
        int pos = 0;
        int doc = 0;
        while (pos + K <= BLOCK_SIZE) {
            perDocK[doc++] = K;
            for (int k = 0; k < K; k++) {
                out[pos++] = tuple[k];
            }
        }
        final int tailLeft = BLOCK_SIZE - pos;
        int tailMissing = 0;
        if (tailLeft > 0) {
            perDocK[doc++] = K;
            for (int k = 0; k < tailLeft; k++) {
                out[pos++] = tuple[k];
            }
            tailMissing = K - tailLeft;
        }
        return new BlockInputs(out, perDocK, doc, 0, tailMissing);
    }

    private static BlockInputs concatTupleRuns(int Ka, long[] tupleA, int Kb, long[] tupleB) {
        final long[] out = new long[BLOCK_SIZE];
        final int[] perDocK = new int[BLOCK_SIZE + 1];
        final int halfDocs = BLOCK_SIZE / (2 * Ka);
        int pos = 0;
        int doc = 0;
        for (int d = 0; d < halfDocs; d++) {
            perDocK[doc++] = Ka;
            for (int k = 0; k < Ka; k++) {
                out[pos++] = tupleA[k];
            }
        }
        while (pos + Kb <= BLOCK_SIZE) {
            perDocK[doc++] = Kb;
            for (int k = 0; k < Kb; k++) {
                out[pos++] = tupleB[k];
            }
        }
        final int tailLeft = BLOCK_SIZE - pos;
        int tailMissing = 0;
        if (tailLeft > 0) {
            perDocK[doc++] = Kb;
            for (int k = 0; k < tailLeft; k++) {
                out[pos++] = tupleB[k];
            }
            tailMissing = Kb - tailLeft;
        }
        return new BlockInputs(out, perDocK, doc, 0, tailMissing);
    }
}
