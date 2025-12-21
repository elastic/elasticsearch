/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
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
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class FixedCapacityExponentialHistogramTests extends ExponentialHistogramTestCase {

    public void testConcurrentHashCode() throws ExecutionException, InterruptedException {
        List<ExponentialHistogram> originalHistograms = IntStream.range(0, 1000)
            .mapToObj(i -> ExponentialHistogramTestUtils.randomHistogram())
            .toList();

        List<? extends ExponentialHistogram> copies = originalHistograms.stream()
            .map(histo -> ExponentialHistogram.builder(histo, ExponentialHistogramCircuitBreaker.noop()).build())
            .toList();

        // Compute potentially lazy data correctly on the originals
        originalHistograms.forEach(Object::hashCode);
        concurrentTest(() -> {
            for (int i = 0; i < originalHistograms.size(); i++) {
                ExponentialHistogram original = originalHistograms.get(i);
                ExponentialHistogram copy = copies.get(i);
                assertThat(copy.hashCode(), equalTo(original.hashCode()));
            }
        });
    }

    public void testValueCountUpdatedCorrectly() {

        FixedCapacityExponentialHistogram histogram = FixedCapacityExponentialHistogram.create(100, breaker());
        autoReleaseOnTestEnd(histogram);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(0L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(0L));

        histogram.tryAddBucket(1, 10, false);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(10L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(0L));

        histogram.tryAddBucket(2, 3, false);
        histogram.tryAddBucket(3, 4, false);
        histogram.tryAddBucket(1, 5, true);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(17L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(5L));

        histogram.tryAddBucket(2, 3, true);
        histogram.tryAddBucket(3, 4, true);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(17L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(12L));

        histogram.resetBuckets(0);

        assertThat(histogram.negativeBuckets().valueCount(), equalTo(0L));
        assertThat(histogram.positiveBuckets().valueCount(), equalTo(0L));
    }

    public void testMemoryAccounting() {
        CircuitBreaker esBreaker = newLimitedBreaker(ByteSizeValue.ofMb(100));
        try (FixedCapacityExponentialHistogram histogram = FixedCapacityExponentialHistogram.create(100, breaker(esBreaker))) {
            assertThat(histogram.ramBytesUsed(), greaterThan(2 * RamEstimationUtil.estimateLongArray(100)));
            assertThat(esBreaker.getUsed(), equalTo(histogram.ramBytesUsed()));
        }
        assertThat(esBreaker.getUsed(), equalTo(0L));
    }

    protected void concurrentTest(Runnable r) throws InterruptedException, ExecutionException {
        int threads = 5;
        int tasks = threads * 2;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int t = 0; t < tasks; t++) {
                results.add(exec.submit(r));
            }
            for (Future<?> f : results) {
                f.get();
            }
        } finally {
            exec.shutdown();
        }
    }
}
