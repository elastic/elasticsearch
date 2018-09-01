/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.benchmark.indices.breaker;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") //invoked by benchmarking framework
public class MemoryStatsBenchmark {
    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

    @Param({"0", "16", "256", "4096"})
    private int tokens;

    @Benchmark
    public void baseline() {
        Blackhole.consumeCPU(tokens);
    }

    @Benchmark
    @Threads(1)
    public long getMemoryStats_01() {
        Blackhole.consumeCPU(tokens);
        return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
    }

    @Benchmark
    @Threads(2)
    public long getMemoryStats_02() {
        Blackhole.consumeCPU(tokens);
        return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
    }

    @Benchmark
    @Threads(4)
    public long getMemoryStats_04() {
        Blackhole.consumeCPU(tokens);
        return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
    }

    @Benchmark
    @Threads(8)
    public long getMemoryStats_08() {
        Blackhole.consumeCPU(tokens);
        return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
    }

    @Benchmark
    @Threads(16)
    public long getMemoryStats_16() {
        Blackhole.consumeCPU(tokens);
        return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
    }

    @Benchmark
    @Threads(32)
    public long getMemoryStats_32() {
        Blackhole.consumeCPU(tokens);
        return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
    }

    @Benchmark
    @Threads(64)
    public long getMemoryStats_64() {
        Blackhole.consumeCPU(tokens);
        return MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed();
    }
}

