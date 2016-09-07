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
    public void setUp(SampleRecorder sampleRecorder) throws Exception {
        this.sampleRecorder = sampleRecorder;
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
