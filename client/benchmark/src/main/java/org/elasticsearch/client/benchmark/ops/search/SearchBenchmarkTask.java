/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.benchmark.ops.search;

import org.elasticsearch.client.benchmark.BenchmarkTask;
import org.elasticsearch.client.benchmark.metrics.Sample;
import org.elasticsearch.client.benchmark.metrics.SampleRecorder;

import java.util.concurrent.TimeUnit;

public class SearchBenchmarkTask implements BenchmarkTask {
    private final SearchRequestExecutor searchRequestExecutor;
    private final String searchRequestBody;
    private final int warmupIterations;
    private final int measurementIterations;
    private final int targetThroughput;

    private SampleRecorder sampleRecorder;

    public SearchBenchmarkTask(SearchRequestExecutor searchRequestExecutor, String body, int warmupIterations,
                               int measurementIterations, int targetThroughput) {
        this.searchRequestExecutor = searchRequestExecutor;
        this.searchRequestBody = body;
        this.warmupIterations = warmupIterations;
        this.measurementIterations = measurementIterations;
        this.targetThroughput = targetThroughput;
    }

    @Override
    public void setUp(SampleRecorder recorder) throws Exception {
        this.sampleRecorder = recorder;
    }

    @Override
    public void run() throws Exception {
        runIterations(warmupIterations, false);
        runIterations(measurementIterations, true);
    }

    private void runIterations(int iterations, boolean addSample) {
        long interval = TimeUnit.SECONDS.toNanos(1L) / targetThroughput;

        long totalStart = System.nanoTime();
        for (int iteration = 0; iteration < iterations; iteration++) {
            long expectedStart = totalStart + iteration * interval;
            while (System.nanoTime() < expectedStart) {
                // busy spin
            }
            long start = System.nanoTime();
            boolean success = searchRequestExecutor.search(searchRequestBody);
            long stop = System.nanoTime();
            if (addSample) {
                sampleRecorder.addSample(new Sample("search", expectedStart, start, stop, success));
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
        // no op
    }
}
