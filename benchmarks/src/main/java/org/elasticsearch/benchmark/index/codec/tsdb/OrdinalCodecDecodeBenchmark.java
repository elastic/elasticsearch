/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.es95.SortedOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.es95.SortedSetOrdinalCodec;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Decode-side companion to {@link OrdinalCodecBenchmark}. For each (cardinality, shape, K)
 * cell the trial setup encodes the block once with both the legacy encoder and the
 * adaptive encoder, then the @Benchmark methods rewind the cursor and decode repeatedly.
 * Decode is the read-path hot loop in production (every range query, histogram, and
 * downsampling pass over a SORTED_SET field calls it per block) so the throughput numbers
 * matter more for the rally outcomes than encode does.
 *
 * <h2>Ready to run command</h2>
 *
 * <pre>{@code
 * ./gradlew :benchmarks:run --args="OrdinalCodecDecodeBenchmark \
 *   -wi 5 -i 5 -f 3"
 * }</pre>
 */
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class OrdinalCodecDecodeBenchmark {

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

    @Param({ "1", "2", "3", "4", "5", "6", "8" })
    private int runsPerBlock;

    private int bitsPerOrd;
    private long[] decodeScratch;
    private byte[] tsdbEncoded;
    private byte[] adaptiveEncoded;

    private TSDBDocValuesEncoder tsdbOrdinalCodec;
    private SortedOrdinalCodec sortedOrdinalCodec;
    private SortedSetOrdinalCodec sortedSetOrdinalCodec;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        bitsPerOrd = cardinality.bitsPerOrd;
        final BlockInputs in = switch (blockShape) {
            case TSID_RUNS -> generateTsidRunBlock(bitsPerOrd, runsPerBlock);
            case MULTIVALUE_CYCLE -> generateMultivalueCycleBlock(bitsPerOrd, runsPerBlock);
            case MULTIVALUE_BOUNDARY -> generateMultivalueBoundaryBlock(bitsPerOrd, runsPerBlock);
            case MULTIVALUE_VARYING_K -> generateMultivalueVaryingKBlock(bitsPerOrd, runsPerBlock);
        };

        decodeScratch = new long[BLOCK_SIZE];
        tsdbOrdinalCodec = new TSDBDocValuesEncoder(BLOCK_SIZE);
        sortedOrdinalCodec = new SortedOrdinalCodec(BLOCK_SIZE);
        sortedSetOrdinalCodec = new SortedSetOrdinalCodec(BLOCK_SIZE);

        final byte[] tsdbBuf = new byte[OUTPUT_BUFFER_BYTES];
        final ByteArrayDataOutput tsdbOut = new ByteArrayDataOutput(tsdbBuf);
        final long[] tsdbBlock = Arrays.copyOf(in.ords, BLOCK_SIZE);
        tsdbOrdinalCodec.encodeOrdinals(tsdbBlock, tsdbOut, bitsPerOrd);
        tsdbEncoded = Arrays.copyOf(tsdbBuf, tsdbOut.getPosition());

        final byte[] adaptiveBuf = new byte[OUTPUT_BUFFER_BYTES];
        final ByteArrayDataOutput adaptiveOut = new ByteArrayDataOutput(adaptiveBuf);
        final long[] adaptiveBlock = Arrays.copyOf(in.ords, BLOCK_SIZE);
        if (blockShape == BlockShape.TSID_RUNS) {
            sortedOrdinalCodec.encodeOrdinals(adaptiveBlock, adaptiveOut, bitsPerOrd);
        } else {
            sortedSetOrdinalCodec.encodeOrdinals(
                adaptiveBlock,
                in.perDocK,
                in.numDocs,
                in.headOffset,
                in.tailMissing,
                adaptiveOut,
                bitsPerOrd
            );
        }
        adaptiveEncoded = Arrays.copyOf(adaptiveBuf, adaptiveOut.getPosition());
    }

    @Benchmark
    @OperationsPerInvocation(BLOCKS_PER_INVOCATION)
    public void tsdbOrdinalCodecDecode(Blackhole bh) throws IOException {
        for (int i = 0; i < BLOCKS_PER_INVOCATION; i++) {
            tsdbOrdinalCodec.decodeOrdinals(new ByteArrayDataInput(tsdbEncoded), decodeScratch, bitsPerOrd);
            bh.consume(decodeScratch);
        }
    }

    @Benchmark
    @OperationsPerInvocation(BLOCKS_PER_INVOCATION)
    public void adaptiveOrdinalCodecDecode(Blackhole bh) throws IOException {
        for (int i = 0; i < BLOCKS_PER_INVOCATION; i++) {
            if (blockShape == BlockShape.TSID_RUNS) {
                sortedOrdinalCodec.decodeOrdinals(new ByteArrayDataInput(adaptiveEncoded), decodeScratch, bitsPerOrd);
            } else {
                sortedSetOrdinalCodec.decodeOrdinals(new ByteArrayDataInput(adaptiveEncoded), decodeScratch, bitsPerOrd);
            }
            bh.consume(decodeScratch);
        }
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
        return tupleRunBlock(scatteredTuple(bitsPerOrd, cyclePeriod, 0));
    }

    private static BlockInputs generateMultivalueBoundaryBlock(int bitsPerOrd, int K) {
        if (K < 2) {
            return generateMultivalueCycleBlock(bitsPerOrd, K);
        }
        return concatTupleRuns(K, scatteredTuple(bitsPerOrd, K, 0), K, scatteredTuple(bitsPerOrd, K, 1));
    }

    private static BlockInputs generateMultivalueVaryingKBlock(int bitsPerOrd, int K) {
        if (K < 2) {
            return generateMultivalueCycleBlock(bitsPerOrd, K);
        }
        final int K2 = K >= 6 ? K - 1 : K + 1;
        return concatTupleRuns(K, scatteredTuple(bitsPerOrd, K, 0), K2, scatteredTuple(bitsPerOrd, K2, 1));
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
