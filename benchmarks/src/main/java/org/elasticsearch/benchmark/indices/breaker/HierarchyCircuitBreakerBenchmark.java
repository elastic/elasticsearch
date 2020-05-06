/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.benchmark.indices.breaker;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class HierarchyCircuitBreakerBenchmark {

    @Param({ "request", "request,inflight_requests,accounting" })
    private String breakerNames;

    private static HierarchyCircuitBreakerService service;
    private static String[] breakers;

    @Setup(Level.Iteration)
    public void setup() {
        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "500mb")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100mb")
            .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100mb")
            .put(HierarchyCircuitBreakerService.ACCOUNTING_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100mb")
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100mb")
            .build();
        service = new HierarchyCircuitBreakerService(
            clusterSettings,
            new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        breakers = Strings.splitStringByCommaToArray(breakerNames);
    }

    @Benchmark
    @Threads(1)
    public void circuitBreak_1(Blackhole bh) {
        for (int i = 0; i < 100; ++i) {
            for (String breaker : breakers) {
                try {
                    bh.consume(service.getBreaker(breaker).addEstimateBytesAndMaybeBreak(2048, "foo"));
                    bh.consume(service.getBreaker(breaker).addWithoutBreaking(-2048));
                } catch (CircuitBreakingException ex) {
                    bh.consume(ex);
                }
            }
        }
    }

    @Benchmark
    @Threads(4)
    public void circuitBreak_4(Blackhole bh) {
        for (int i = 0; i < 100; ++i) {
            for (String breaker : breakers) {
                try {
                    bh.consume(service.getBreaker(breaker).addEstimateBytesAndMaybeBreak(2048, "foo"));
                    bh.consume(service.getBreaker(breaker).addWithoutBreaking(-2048));
                } catch (CircuitBreakingException ex) {
                    bh.consume(ex);
                }
            }
        }
    }

    @Benchmark
    @Threads(64)
    public void circuitBreak_64(Blackhole bh) {
        for (int i = 0; i < 100; ++i) {
            for (String breaker : breakers) {
                try {
                    bh.consume(service.getBreaker(breaker).addEstimateBytesAndMaybeBreak(2048, "foo"));
                    bh.consume(service.getBreaker(breaker).addWithoutBreaking(-2048));
                } catch (CircuitBreakingException ex) {
                    bh.consume(ex);
                }
            }
        }
    }
}
