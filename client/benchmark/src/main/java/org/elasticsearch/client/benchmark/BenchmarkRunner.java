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
package org.elasticsearch.client.benchmark;

import org.elasticsearch.client.benchmark.metrics.Metrics;
import org.elasticsearch.client.benchmark.metrics.MetricsCalculator;
import org.elasticsearch.client.benchmark.metrics.Sample;
import org.elasticsearch.client.benchmark.metrics.SampleRecorder;
import org.elasticsearch.common.SuppressForbidden;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public final class BenchmarkRunner {
    private final int warmupIterations;
    private final int iterations;
    private final BenchmarkTask task;

    public BenchmarkRunner(int warmupIterations, int iterations, BenchmarkTask task) {
        this.warmupIterations = warmupIterations;
        this.iterations = iterations;
        this.task = task;
    }

    @SuppressForbidden(reason = "system out is ok for a command line tool")
    public void run() throws Exception {
        SampleRecorder recorder = new SampleRecorder(warmupIterations, iterations);
        System.out.printf("Running %s with %d warmup iterations and %d iterations.%n",
            task.getClass().getSimpleName(), warmupIterations, iterations);

        try {
            task.setUp(recorder);
            task.run();
            task.tearDown();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return;
        }

        List<Sample> samples = recorder.getSamples();
        final List<Metrics> summaryMetrics = MetricsCalculator.calculate(samples);

        if (summaryMetrics.isEmpty()) {
            System.out.println("No results.");
        }

        for (Metrics metrics : summaryMetrics) {
            System.out.printf(Locale.ROOT, "Operation: %s%n", metrics.operation);
            String stats = String.format(Locale.ROOT,
                "Throughput = %f ops/s, p90 = %f ms, p95 = %f ms, p99 = %f ms, p99.9 = %f ms, p99.99 = %f ms",
                metrics.throughput,
                metrics.serviceTimeP90, metrics.serviceTimeP95,
                metrics.serviceTimeP99, metrics.serviceTimeP999,
                metrics.serviceTimeP9999);
            System.out.println(repeat(stats.length(), '-'));
            System.out.println(stats);
            System.out.printf("success count = %d, error count = %d%n", metrics.successCount, metrics.errorCount);
            System.out.println(repeat(stats.length(), '-'));
        }
    }

    private String repeat(int times, char character) {
        char[] characters = new char[times];
        Arrays.fill(characters, character);
        return new String(characters);
    }
}
