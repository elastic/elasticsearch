package org.elasticsearch.index.seqno;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * This benchmark measures throughput of <code>LocalCheckpointTracker</code> if it were used by a primary shard (get sequence number +
 * mark it as completed) under contention. The benchmark also backs off in-between operations to avoid "hammering" counters in an
 * unrealistic fashion (see parameter <code>backoff</code>).
 *
 * An additional optimization (in <code>LocalCheckpointTracker</code>) would be to implement a backoff in the atomic counters themselves
 * as described in the paper
 * <a href="https://arxiv.org/abs/1305.5800">Lightweight Contention Management for Efficient Compare-and-Swap Operations</a>.
 */
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") //invoked by benchmarking framework
public class LocalCheckpointTrackerBenchmark {
    private LocalCheckpointTracker tracker;

    @Param({"0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024", "2048", "4096", "8192"})
    int backoff;

    @Setup
    public void setUp() {
        tracker = new LocalCheckpointTracker(SequenceNumbersService.NO_OPS_PERFORMED, SequenceNumbersService.NO_OPS_PERFORMED);
    }

    @Benchmark
    @Group("seq_1")
    @GroupThreads()
    public long measureAdvance_1() {
        Blackhole.consumeCPU(backoff);
        long seqNo = tracker.generateSeqNo();
        Blackhole.consumeCPU(backoff);
        tracker.markSeqNoAsCompleted(seqNo);
        return seqNo;
    }


    @Benchmark
    @Group("seq_2")
    @GroupThreads(2)
    public long measureAdvance_2() {
        Blackhole.consumeCPU(backoff);
        long seqNo = tracker.generateSeqNo();
        Blackhole.consumeCPU(backoff);
        tracker.markSeqNoAsCompleted(seqNo);
        return seqNo;
    }


    @Benchmark
    @Group("seq_4")
    @GroupThreads(4)
    public long measureAdvance_4() {
        Blackhole.consumeCPU(backoff);
        long seqNo = tracker.generateSeqNo();
        Blackhole.consumeCPU(backoff);
        tracker.markSeqNoAsCompleted(seqNo);
        return seqNo;
    }


    @Benchmark
    @Group("seq_8")
    @GroupThreads(8)
    public long measureAdvance_8() {
        Blackhole.consumeCPU(backoff);
        long seqNo = tracker.generateSeqNo();
        Blackhole.consumeCPU(backoff);
        tracker.markSeqNoAsCompleted(seqNo);
        return seqNo;
    }


    @Benchmark
    @Group("seq_16")
    @GroupThreads(16)
    public long measureAdvance_16() {
        Blackhole.consumeCPU(backoff);
        long seqNo = tracker.generateSeqNo();
        Blackhole.consumeCPU(backoff);
        tracker.markSeqNoAsCompleted(seqNo);
        return seqNo;
    }


    @Benchmark
    @Group("seq_32")
    @GroupThreads(32)
    public long measureAdvance_32() {
        Blackhole.consumeCPU(backoff);
        long seqNo = tracker.generateSeqNo();
        Blackhole.consumeCPU(backoff);
        tracker.markSeqNoAsCompleted(seqNo);
        return seqNo;
    }
}
