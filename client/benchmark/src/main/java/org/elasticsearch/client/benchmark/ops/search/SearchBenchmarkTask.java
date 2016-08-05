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
    private static final long MICROS_PER_SEC = TimeUnit.SECONDS.toMicros(1L);
    private static final long NANOS_PER_MICRO = TimeUnit.MICROSECONDS.toNanos(1L);

    private final SearchRequestExecutor searchRequestExecutor;
    private final String searchRequestBody;
    private final int iterations;
    private final int targetThroughput;

    private SampleRecorder sampleRecorder;

    public SearchBenchmarkTask(SearchRequestExecutor searchRequestExecutor, String body, int iterations, int targetThroughput) {
        this.searchRequestExecutor = searchRequestExecutor;
        this.searchRequestBody = body;
        this.iterations = iterations;
        this.targetThroughput = targetThroughput;
    }

    @Override
    public void setUp(SampleRecorder sampleRecorder) throws Exception {
        this.sampleRecorder = sampleRecorder;
    }

    @Override
    public void run() throws Exception {
        for (int iteration = 0; iteration < this.iterations; iteration++) {
            final long start = System.nanoTime();
            boolean success = searchRequestExecutor.search(searchRequestBody);
            final long stop = System.nanoTime();
            sampleRecorder.addSample(new Sample("search", start, stop, success));

            int waitTime = (int) Math.floor(MICROS_PER_SEC / targetThroughput - (stop - start) / NANOS_PER_MICRO);
            if (waitTime > 0) {
                waitMicros(waitTime);
            }
        }
    }

    private void waitMicros(int waitTime) throws InterruptedException {
        // Thread.sleep() time is not very accurate (it's most of the time around 1 - 2 ms off)
        // we busy spin all the time to avoid introducing additional measurement artifacts (noticed 100% skew on 99.9th percentile)
        // this approach is not suitable for low throughput rates (in the second range) though
        if (waitTime > 0) {
            long end = System.nanoTime() + 1000L * waitTime;
            while (end > System.nanoTime()) {
                // busy spin
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
        // no op
    }
}
