/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.client.benchmark.ops.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.client.benchmark.BenchmarkTask;
import org.elasticsearch.client.benchmark.metrics.Sample;
import org.elasticsearch.client.benchmark.metrics.SampleRecorder;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BulkBenchmarkTask implements BenchmarkTask {
    private final BulkRequestExecutor requestExecutor;
    private final String indexFilePath;
    private final int warmupIterations;
    private final int measurementIterations;
    private final int bulkSize;
    private LoadGenerator generator;
    private ExecutorService executorService;

    public BulkBenchmarkTask(
        BulkRequestExecutor requestExecutor,
        String indexFilePath,
        int warmupIterations,
        int measurementIterations,
        int bulkSize
    ) {
        this.requestExecutor = requestExecutor;
        this.indexFilePath = indexFilePath;
        this.warmupIterations = warmupIterations;
        this.measurementIterations = measurementIterations;
        this.bulkSize = bulkSize;
    }

    @Override
    @SuppressForbidden(reason = "PathUtils#get is fine - we don't have environment here")
    public void setUp(SampleRecorder sampleRecorder) {
        BlockingQueue<List<String>> bulkQueue = new ArrayBlockingQueue<>(256);

        BulkIndexer runner = new BulkIndexer(bulkQueue, warmupIterations, measurementIterations, sampleRecorder, requestExecutor);

        executorService = Executors.newSingleThreadExecutor((r) -> new Thread(r, "bulk-index-runner"));
        executorService.submit(runner);

        generator = new LoadGenerator(PathUtils.get(indexFilePath), bulkQueue, bulkSize);
    }

    @Override
    @SuppressForbidden(reason = "system out is ok for a command line tool")
    public void run() throws Exception {
        generator.execute();
        // when the generator is done, there are no more data -> shutdown client
        executorService.shutdown();
        // We need to wait until the queue is drained
        final boolean finishedNormally = executorService.awaitTermination(20, TimeUnit.MINUTES);
        if (finishedNormally == false) {
            System.err.println("Background tasks are still running after timeout on enclosing pool. Forcing pool shutdown.");
            executorService.shutdownNow();
        }
    }

    @Override
    public void tearDown() {
        // no op
    }

    private static final class LoadGenerator {
        private final Path bulkDataFile;
        private final BlockingQueue<List<String>> bulkQueue;
        private final int bulkSize;

        LoadGenerator(Path bulkDataFile, BlockingQueue<List<String>> bulkQueue, int bulkSize) {
            this.bulkDataFile = bulkDataFile;
            this.bulkQueue = bulkQueue;
            this.bulkSize = bulkSize;
        }

        @SuppressForbidden(reason = "Classic I/O is fine in non-production code")
        public void execute() {
            try (BufferedReader reader = Files.newBufferedReader(bulkDataFile, StandardCharsets.UTF_8)) {
                String line;
                int bulkIndex = 0;
                List<String> bulkData = new ArrayList<>(bulkSize);
                while ((line = reader.readLine()) != null) {
                    if (bulkIndex == bulkSize) {
                        sendBulk(bulkData);
                        // reset data structures
                        bulkData = new ArrayList<>(bulkSize);
                        bulkIndex = 0;
                    }
                    bulkData.add(line);
                    bulkIndex++;
                }
                // also send the last bulk:
                if (bulkIndex > 0) {
                    sendBulk(bulkData);
                }
            } catch (IOException e) {
                throw new ElasticsearchException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void sendBulk(List<String> bulkData) throws InterruptedException {
            bulkQueue.put(bulkData);
        }
    }

    private static final class BulkIndexer implements Runnable {
        private static final Logger logger = LogManager.getLogger(BulkIndexer.class);

        private final BlockingQueue<List<String>> bulkData;
        private final int warmupIterations;
        private final int measurementIterations;
        private final BulkRequestExecutor bulkRequestExecutor;
        private final SampleRecorder sampleRecorder;

        BulkIndexer(
            BlockingQueue<List<String>> bulkData,
            int warmupIterations,
            int measurementIterations,
            SampleRecorder sampleRecorder,
            BulkRequestExecutor bulkRequestExecutor
        ) {
            this.bulkData = bulkData;
            this.warmupIterations = warmupIterations;
            this.measurementIterations = measurementIterations;
            this.bulkRequestExecutor = bulkRequestExecutor;
            this.sampleRecorder = sampleRecorder;
        }

        @Override
        public void run() {
            for (int iteration = 0; iteration < warmupIterations + measurementIterations; iteration++) {
                boolean success = false;
                List<String> currentBulk;
                try {
                    currentBulk = bulkData.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                // measure only service time, latency is not that interesting for a throughput benchmark
                long start = System.nanoTime();
                try {
                    success = bulkRequestExecutor.bulkIndex(currentBulk);
                } catch (Exception ex) {
                    logger.warn("Error while executing bulk request", ex);
                }
                long stop = System.nanoTime();
                if (iteration >= warmupIterations) {
                    sampleRecorder.addSample(new Sample("bulk", start, start, stop, success));
                }
            }
        }
    }
}
