/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.data;

import org.elasticsearch.compute.data.AbstractBlockRefCounted;
import org.elasticsearch.compute.data.AbstractSynchronizedBlockRefCounted;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 2, jvmArgsAppend = { "-da" })
@State(Scope.Thread)
public class RefCountBenchmark {

    @Param({ "1", "100", "1000" })
    int blockCount;

    @Param({ "false", "true" })
    boolean promoted;

    static final class ConcreteSync extends AbstractSynchronizedBlockRefCounted {
        @Override
        protected void closeInternal() {}
    }

    static final class ConcreteAtomic extends AbstractBlockRefCounted {
        @Override
        protected void closeInternal() {}
    }

    private ConcreteSync[] syncBlocks;
    private ConcreteSync[] syncNext;
    private ConcreteAtomic[] atomicBlocks;
    private ConcreteAtomic[] atomicNext;

    @Setup
    public void setup() {
        syncBlocks = new ConcreteSync[blockCount];
        syncNext = new ConcreteSync[blockCount];
        atomicBlocks = new ConcreteAtomic[blockCount];
        atomicNext = new ConcreteAtomic[blockCount];
        for (int i = 0; i < blockCount; i++) {
            syncBlocks[i] = new ConcreteSync();
            if (promoted) syncBlocks[i].makeRefCountsThreadSafe();
            syncNext[i] = new ConcreteSync();
            if (promoted) syncNext[i].makeRefCountsThreadSafe();

            atomicBlocks[i] = new ConcreteAtomic();
            if (promoted) atomicBlocks[i].makeRefCountsThreadSafe();
            atomicNext[i] = new ConcreteAtomic();
            if (promoted) atomicNext[i].makeRefCountsThreadSafe();
        }
    }

    /**
     * {@link AbstractSynchronizedBlockRefCounted}: plain {@code int} before promotion, {@code synchronized} after.
     */
    @Benchmark
    public int synchronizedRefCount() {
        int closed = 0;
        for (int i = 0; i < blockCount; i++) {
            ConcreteSync b = syncBlocks[i];
            b.incRef();
            b.decRef();
            if (b.decRef()) closed++;
            syncBlocks[i] = syncNext[i];
            ConcreteSync fresh = new ConcreteSync();
            if (promoted) fresh.makeRefCountsThreadSafe();
            syncNext[i] = fresh;
        }
        return closed;
    }

    /**
     * {@link AbstractBlockRefCounted}: plain {@code int} before promotion, VarHandle CAS after.
     */
    @Benchmark
    public int atomicIntRefCount() {
        int closed = 0;
        for (int i = 0; i < blockCount; i++) {
            ConcreteAtomic b = atomicBlocks[i];
            b.incRef();
            b.decRef();
            if (b.decRef()) closed++;
            atomicBlocks[i] = atomicNext[i];
            ConcreteAtomic fresh = new ConcreteAtomic();
            if (promoted) fresh.makeRefCountsThreadSafe();
            atomicNext[i] = fresh;
        }
        return closed;
    }
}
