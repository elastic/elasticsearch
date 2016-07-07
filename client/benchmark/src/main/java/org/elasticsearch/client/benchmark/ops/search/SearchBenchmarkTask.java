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

public class SearchBenchmarkTask implements BenchmarkTask {
    private final SearchRequestExecutor searchRequestExecutor;
    private final String searchRequestBody;
    private final int iterations;

    private SampleRecorder sampleRecorder;

    public SearchBenchmarkTask(SearchRequestExecutor searchRequestExecutor, String searchRequestBody, int iterations) {
        this.searchRequestExecutor = searchRequestExecutor;
        this.searchRequestBody = searchRequestBody;
        this.iterations = iterations;
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
        }
    }

    @Override
    public void tearDown() throws Exception {
        // no op
    }
}
