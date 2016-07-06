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

import org.elasticsearch.client.benchmark.metrics.MetricsRecord;
import org.elasticsearch.client.benchmark.metrics.Statistics;
import org.elasticsearch.client.benchmark.metrics.StatisticsRecord;
import org.elasticsearch.client.benchmark.ops.bulk.BulkIndexRunner;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;
import org.elasticsearch.client.benchmark.ops.bulk.LoadGenerator;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class ClientBenchmark {
    protected abstract BulkRequestExecutor requestExecutor(String hostName, String indexName, String typeName) throws Exception;

    @SuppressForbidden(reason = "System.out is ok for a command line tool")
    public void run(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("usage: benchmarkTargetHostIp indexFilePath indexName typeName bulkSize");
            System.exit(1);
        }
        String benchmarkTargetHost = args[0];
        String indexFilePath = args[1];
        String indexName = args[2];
        String typeName = args[3];
        int bulkSize = Integer.valueOf(args[4]);

        System.out.printf(Locale.ROOT, "Starting %s%n%n", getClass().getSimpleName());

        System.out.printf(Locale.ROOT, "Targeting Elasticsearch at %s%n", benchmarkTargetHost);
        System.out.printf(Locale.ROOT, "Indexing %s with %d docs / bulk request...%n", indexFilePath, bulkSize);


        BlockingQueue<List<String>> bulkQueue = new ArrayBlockingQueue<>(512);

        BulkIndexRunner runner = new BulkIndexRunner(bulkQueue, requestExecutor(benchmarkTargetHost, indexName, typeName));

        ExecutorService executorService = Executors.newSingleThreadExecutor((r) -> new Thread(r, "bulk-index-runner"));
        executorService.submit(runner);

        LoadGenerator generator = new LoadGenerator(PathUtils.get(indexFilePath), bulkQueue, bulkSize);
        generator.execute();
        // when the generator is done, there are no more data -> shutdown client
        executorService.shutdownNow();
        System.out.println("All data read. Waiting for client to finish...");
        //We need to wait until the queue is drained
        executorService.awaitTermination(20, TimeUnit.SECONDS);
        // put after thread pool termination as we wait until the queue is drained

        List<MetricsRecord> metrics = runner.getMetrics();
        final List<StatisticsRecord> summaryStatistics = Statistics.calculate(metrics);

        for (StatisticsRecord statisticsRecord : summaryStatistics) {
            System.out.printf(Locale.ROOT, "Operation: %s%n", statisticsRecord.operation);
            String stats = String.format(Locale.ROOT,
                "Throughput = %f ops/s (%d docs bulk size), p90 = %f ms, p95 = %f ms, p99 = %f ms, p99.9 = %f ms%n",
                statisticsRecord.throughput,
                bulkSize,
                statisticsRecord.serviceTimeP90, statisticsRecord.serviceTimeP95,
                statisticsRecord.serviceTimeP99, statisticsRecord.serviceTimeP999);
            System.out.println(repeat(stats.length(), '-'));
            System.out.println(stats);
            System.out.println(repeat(stats.length(), '-'));

        }
    }

    private String repeat(int times, char character) {
        char[] characters = new char[times];
        Arrays.fill(characters, character);
        return new String(characters);
    }
}
