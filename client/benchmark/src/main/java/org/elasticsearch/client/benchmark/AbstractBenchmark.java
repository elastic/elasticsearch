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

import org.elasticsearch.client.benchmark.ops.bulk.BulkBenchmarkTask;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;
import org.elasticsearch.client.benchmark.ops.search.SearchBenchmarkTask;
import org.elasticsearch.client.benchmark.ops.search.SearchRequestExecutor;
import org.elasticsearch.common.SuppressForbidden;

import java.io.Closeable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractBenchmark<T extends Closeable> {
    private static final int SEARCH_BENCHMARK_ITERATIONS = 10_000;

    protected abstract T client(String benchmarkTargetHost) throws Exception;

    protected abstract BulkRequestExecutor bulkRequestExecutor(T client, String indexName, String typeName);

    protected abstract SearchRequestExecutor searchRequestExecutor(T client, String indexName);

    @SuppressForbidden(reason = "system out is ok for a command line tool")
    public final void run(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: [search|bulk]");
            System.exit(1);
        }
        switch (args[0]) {
            case "search":
                runSearchBenchmark(args);
                break;
            case "bulk":
                runBulkIndexBenchmark(args);
                break;
            default:
                System.err.println("Unknown benchmark type [" + args[0] + "]");
                System.exit(1);

        }

    }

    @SuppressForbidden(reason = "system out is ok for a command line tool")
    private void runBulkIndexBenchmark(String[] args) throws Exception {
        if (args.length != 7) {
            System.err.println(
                "usage: 'bulk' benchmarkTargetHostIp indexFilePath indexName typeName numberOfDocuments bulkSize");
            System.exit(1);
        }
        String benchmarkTargetHost = args[1];
        String indexFilePath = args[2];
        String indexName = args[3];
        String typeName = args[4];
        int totalDocs = Integer.valueOf(args[5]);
        int bulkSize = Integer.valueOf(args[6]);

        int totalIterationCount = (int) Math.floor(totalDocs / bulkSize);
        // consider 40% of all iterations as warmup iterations
        int warmupIterations = (int) (0.4d * totalIterationCount);
        int iterations = totalIterationCount - warmupIterations;

        T client = client(benchmarkTargetHost);

        BenchmarkRunner benchmark = new BenchmarkRunner(warmupIterations, iterations,
            new BulkBenchmarkTask(
                bulkRequestExecutor(client, indexName, typeName), indexFilePath, warmupIterations, iterations, bulkSize));

        try {
            runTrials(() -> {
                runGc();
                benchmark.run();
            });
        } finally {
            client.close();
        }

    }

    @SuppressForbidden(reason = "system out is ok for a command line tool")
    private void runSearchBenchmark(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println(
                "usage: 'search' benchmarkTargetHostIp indexName searchRequestBody throughputRates");
            System.exit(1);
        }
        String benchmarkTargetHost = args[1];
        String indexName = args[2];
        String searchBody = args[3];
        List<Integer> throughputRates = Arrays.asList(args[4].split(",")).stream().map(Integer::valueOf).collect(Collectors.toList());

        T client = client(benchmarkTargetHost);

        try {
            runTrials(() -> {
                for (int throughput : throughputRates) {
                    //GC between trials to reduce the likelihood of a GC occurring in the middle of a trial.
                    runGc();
                    BenchmarkRunner benchmark = new BenchmarkRunner(SEARCH_BENCHMARK_ITERATIONS, SEARCH_BENCHMARK_ITERATIONS,
                        new SearchBenchmarkTask(
                            searchRequestExecutor(client, indexName), searchBody, SEARCH_BENCHMARK_ITERATIONS,
                            SEARCH_BENCHMARK_ITERATIONS, throughput));
                    System.out.printf("Target throughput = %d ops / s%n", throughput);
                    benchmark.run();
                }
            });
        } finally {
            client.close();
        }
    }

    @SuppressForbidden(reason = "system out is ok for a command line tool")
    private void runTrials(Runnable runner) {
        int totalWarmupTrialRuns = 1;
        for (int run = 1; run <= totalWarmupTrialRuns; run++) {
            System.out.println("======================");
            System.out.println(" Warmup trial run " + run + "/" + totalWarmupTrialRuns);
            System.out.println("======================");
            runner.run();
        }

        int totalTrialRuns = 5;
        for (int run = 1; run <= totalTrialRuns; run++) {
            System.out.println("================");
            System.out.println(" Trial run " + run + "/" + totalTrialRuns);
            System.out.println("================");

            runner.run();
        }
    }

    /**
     * Requests a full GC and checks whether the GC did actually run after a request. It retries up to 5 times in case the GC did not
     * run in time.
     */
    @SuppressForbidden(reason = "we need to request a system GC for the benchmark")
    private void runGc() {
        long previousCollections = getTotalGcCount();
        int attempts = 0;
        do {
            // request a full GC ...
            System.gc();
            // ... and give GC a chance to run
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            attempts++;
        } while (previousCollections == getTotalGcCount() || attempts < 5);
    }

    private long getTotalGcCount() {
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        return gcMxBeans.stream().mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
    }
}
