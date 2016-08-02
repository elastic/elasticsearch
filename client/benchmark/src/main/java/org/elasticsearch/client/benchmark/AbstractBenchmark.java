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
import java.util.List;

public abstract class AbstractBenchmark<T extends Closeable> {
    private static final int SEARCH_BENCHMARK_ITERATIONS = 10_000;

    protected abstract T client(String benchmarkTargetHost) throws Exception;

    protected abstract BulkRequestExecutor bulkRequestExecutor(T client, String indexName, String typeName);

    protected abstract SearchRequestExecutor searchRequestExecutor(T client, String indexName);

    @SuppressForbidden(reason = "system out is ok for a command line tool")
    public final void run(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println(
                "usage: benchmarkTargetHostIp indexFilePath indexName typeName numberOfDocuments bulkSize [search request body]");
            System.exit(1);
        }
        String benchmarkTargetHost = args[0];
        String indexFilePath = args[1];
        String indexName = args[2];
        String typeName = args[3];
        int totalDocs = Integer.valueOf(args[4]);
        int bulkSize = Integer.valueOf(args[5]);

        int totalIterationCount = (int) Math.floor(totalDocs / bulkSize);
        // consider 40% of all iterations as warmup iterations
        int warmupIterations = (int) (0.4d * totalIterationCount);
        int iterations = totalIterationCount - warmupIterations;
        String searchBody = (args.length == 7) ? args[6] : null;

        T client = client(benchmarkTargetHost);

        BenchmarkRunner benchmark = new BenchmarkRunner(warmupIterations, iterations,
            new BulkBenchmarkTask(
                bulkRequestExecutor(client, indexName, typeName), indexFilePath, warmupIterations + iterations, bulkSize));

        try {
            benchmark.run();
            if (searchBody != null) {
                for (int run = 1; run <= 5; run++) {
                    System.out.println("=============");
                    System.out.println(" Trial run " + run);
                    System.out.println("=============");

                    for (int throughput = 100; throughput <= 100_000; throughput *= 10) {
                        //GC between trials to reduce the likelihood of a GC occurring in the middle of a trial.
                        runGc();
                        BenchmarkRunner searchBenchmark = new BenchmarkRunner(SEARCH_BENCHMARK_ITERATIONS, SEARCH_BENCHMARK_ITERATIONS,
                            new SearchBenchmarkTask(
                                searchRequestExecutor(client, indexName), searchBody, 2 * SEARCH_BENCHMARK_ITERATIONS, throughput));
                        System.out.printf("Target throughput = %d ops / s%n", throughput);
                        searchBenchmark.run();
                    }
                }
            }
        } finally {
            client.close();
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
