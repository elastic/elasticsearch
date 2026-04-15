/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.vector.cluster;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.vectors.cluster.BulkNeighborQueue;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;
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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
public class NeighborQueueInsertWithOverflowBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    public enum HeapImplementation {
        BINARY,
        PIVOT_AUTO
    }

    public enum WorkloadModel {
        PREFILTERED_DECAY,
        EXTRAPOLATED_REAL_WORLD
    }

    @Param({ "1", "5", "10", "20", "30", "100", "300", "500" })
    public int heapSize;

    @Param({ "32" })
    public int bulkSize;

    @Param({ "1.0" })
    public double acceptHighRate;

    @Param({ "0.1" })
    public double acceptLowRate;

    @Param({ "0.9" })
    public double decayPerK;

    @Param({ "1000", "10000", "100000", "500000" })
    public int totalVectors;

    @Param({ "EXTRAPOLATED_REAL_WORLD", "PREFILTERED_DECAY" })
    public WorkloadModel workloadModel;

    @Param({ "false" })
    public boolean verifyTopK;

    @Param({ "BINARY", "PIVOT_AUTO" })
    public HeapImplementation heapImplementation;

    private QueueAdapter queueAdapter;
    private int[] docIdsScratch;
    private float[] scores;
    private NeighborQueue verifyQueue;

    @Setup(Level.Trial)
    public void setupTrial() {
        docIdsScratch = new int[bulkSize];
        scores = new float[bulkSize];
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        queueAdapter = createQueueAdapter();
        verifyQueue = verifyTopK ? new NeighborQueue(heapSize, false) : null;
    }

    @Benchmark
    public int insertWithOverflow(Blackhole bh, QueueStats queueStats) {
        int accepted = 0;
        int docBase = 0;
        int safeHeapSize = Math.max(heapSize, 1);
        while (docBase < totalVectors) {
            int blockSize = Math.min(bulkSize, totalVectors - docBase);
            float baseScore = docBase;
            for (int i = 0; i < blockSize; i++) {
                docIdsScratch[i] = docBase + i;
                scores[i] = baseScore + i;
            }

            int candidateCount;
            if (workloadModel == WorkloadModel.PREFILTERED_DECAY) {
                candidateCount = prefilteredCandidateCount(blockSize, docBase, safeHeapSize);
            } else {
                candidateCount = extrapolatedCandidateCount(blockSize, docBase, safeHeapSize);
            }
            if (candidateCount < blockSize) {
                int start = blockSize - candidateCount;
                for (int i = 0; i < candidateCount; i++) {
                    docIdsScratch[i] = docIdsScratch[start + i];
                    scores[i] = scores[start + i];
                }
            }

            float bestScore = scores[candidateCount - 1];
            int acceptedThisBatch = queueAdapter.insertWithOverflowBulk(docIdsScratch, scores, candidateCount, bestScore);
            accepted += acceptedThisBatch;
            queueStats.generated += blockSize;
            queueStats.candidates += candidateCount;
            queueStats.accepted += acceptedThisBatch;
            queueStats.batchCalls++;

            if (verifyQueue != null) {
                for (int i = 0; i < candidateCount; i++) {
                    verifyQueue.insertWithOverflow(docIdsScratch[i], scores[i]);
                }
            }

            docBase += blockSize;
        }

        if (verifyQueue != null) {
            assertTopKMatchesVerificationQueue();
        }
        bh.consume(accepted);
        return accepted;
    }

    private int prefilteredCandidateCount(int blockSize, int docBase, int safeHeapSize) {
        int decaySteps = docBase / safeHeapSize;
        double acceptanceRate = Math.max(acceptLowRate, acceptHighRate * Math.pow(decayPerK, decaySteps));
        int accepted = Math.max(1, (int) Math.rint(blockSize * acceptanceRate));
        return Math.min(blockSize, accepted);
    }

    /**
     * Closed-form acceptance profile extrapolated from real checkVec runs.
     * This avoids external replay files while preserving realistic decline and burst patterns.
     *
     * Coefficients were tuned empirically from recorded checkVec acceptance rates:
     * k=2 -> ~0.10, k=10 -> ~0.13, k=200 -> ~0.20, k=1000 -> ~0.29, k=2000 -> ~0.32.
     * The model intentionally uses smooth monotonic components (exp/log decay + bounded sinusoidal
     * burst term) so we can keep benchmark inputs deterministic and compact without requiring
     * a persisted replay dataset.
     */
    private int extrapolatedCandidateCount(int blockSize, int docBase, int safeHeapSize) {
        double steps = (double) docBase / safeHeapSize;
        double highRate = extrapolatedHighRate(heapSize);
        double lowRate = extrapolatedLowRate(heapSize);
        double decay = extrapolatedDecay(heapSize);
        double burst = 1.0d + 0.18d * Math.sin(steps * 0.11d);
        double acceptanceRate = Math.max(lowRate, Math.min(1.0d, highRate * Math.pow(decay, steps) * burst));
        int accepted = Math.max(1, (int) Math.rint(blockSize * acceptanceRate));
        return Math.min(blockSize, accepted);
    }

    private static double extrapolatedHighRate(int k) {
        double kShape = 1.0d - Math.exp(-Math.pow(k / 250.0d, 0.45d));
        double high = 0.16d + (0.22d * kShape);
        return clamp(high, 0.08d, 0.62d);
    }

    private static double extrapolatedLowRate(int k) {
        double low = 0.13d - (0.07d * (1.0d - Math.exp(-(double) k / 300.0d)));
        return clamp(low, 0.005d, 0.22d);
    }

    private static double extrapolatedDecay(int k) {
        double decay = 0.992d - (0.035d * (1.0d - Math.exp(-(double) k / 500.0d)));
        return clamp(decay, 0.94d, 0.99d);
    }

    private static double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class QueueStats {
        public long generated;
        public long candidates;
        public long accepted;
        public long batchCalls;

        @Setup(Level.Invocation)
        public void reset() {
            generated = 0L;
            candidates = 0L;
            accepted = 0L;
            batchCalls = 0L;
        }
    }

    private QueueAdapter createQueueAdapter() {
        return switch (heapImplementation) {
            case BINARY -> new BinaryQueueAdapter(heapSize);
            case PIVOT_AUTO -> new BulkNeighborQueueAdapter(new BulkNeighborQueue(heapSize));
        };
    }

    private void assertTopKMatchesVerificationQueue() {
        long[] expected = snapshotNeighborQueue(verifyQueue);
        long[] actual = queueAdapter.snapshotSorted();
        if (Arrays.equals(expected, actual) == false) {
            throw new IllegalStateException(
                "TopK mismatch for " + heapImplementation + " expected=" + Arrays.toString(expected) + " actual=" + Arrays.toString(actual)
            );
        }
    }

    private static long[] snapshotNeighborQueue(NeighborQueue queue) {
        int size = queue.size();
        long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = queue.popRaw();
        }
        Arrays.sort(values);
        return values;
    }

    private static long[] snapshotBulkQueue(BulkNeighborQueue queue) {
        long[] values = new long[queue.size()];
        final int[] index = new int[1];
        queue.drain(encoded -> values[index[0]++] = encoded);
        Arrays.sort(values);
        return values;
    }

    private interface QueueAdapter {
        int insertWithOverflowBulk(int[] docs, float[] scores, int count, float bestScore);

        long[] snapshotSorted();
    }

    private static class BinaryQueueAdapter implements QueueAdapter {
        private final NeighborQueue queue;

        private BinaryQueueAdapter(int heapSize) {
            this.queue = new NeighborQueue(heapSize, false);
        }

        @Override
        public int insertWithOverflowBulk(int[] docs, float[] scores, int count, float bestScore) {
            int accepted = 0;
            for (int i = 0; i < count; i++) {
                if (queue.insertWithOverflow(docs[i], scores[i])) {
                    accepted++;
                }
            }
            return accepted;
        }

        @Override
        public long[] snapshotSorted() {
            return snapshotNeighborQueue(queue);
        }
    }

    private static class BulkNeighborQueueAdapter implements QueueAdapter {
        private final BulkNeighborQueue queue;

        private BulkNeighborQueueAdapter(BulkNeighborQueue queue) {
            this.queue = queue;
        }

        @Override
        public int insertWithOverflowBulk(int[] docs, float[] scores, int count, float bestScore) {
            return queue.insertWithOverflowBulk(docs, scores, count, bestScore);
        }

        @Override
        public long[] snapshotSorted() {
            return snapshotBulkQueue(queue);
        }
    }
}
