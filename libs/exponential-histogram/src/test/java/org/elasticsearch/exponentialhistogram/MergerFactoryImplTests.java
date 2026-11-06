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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.sameInstance;

public class MergerFactoryImplTests extends ExponentialHistogramTestCase {

    public void testCrankyBreaker() {
        CrankyCircuitBreakerService crankyBreakerService = new CrankyCircuitBreakerService();
        CircuitBreaker breaker = crankyBreakerService.getBreaker(CircuitBreaker.REQUEST);
        ExponentialHistogramCircuitBreaker ehBreaker = breaker(breaker);
        int bucketLimit = randomIntBetween(10, 100);

        ExponentialHistogramMerger.Factory factory = null;
        List<ExponentialHistogramMerger> mergers = new ArrayList<>();

        try {
            factory = ExponentialHistogramMerger.createFactory(bucketLimit, ehBreaker);
            int numMergers = randomIntBetween(1, 5);
            for (int i = 0; i < numMergers; i++) {
                mergers.add(factory.createMerger());
            }

            int numMerges = randomIntBetween(1, 3);
            for (int j = 0; j < numMerges; j++) {
                for (var merger : mergers) {
                    try (ReleasableExponentialHistogram hist = ExponentialHistogramTestUtils.randomHistogram(bucketLimit, ehBreaker)) {
                        merger.add(hist);
                    }
                }
            }

            assertThat(breaker.getUsed(), greaterThan(0L));
        } catch (CircuitBreakingException e) {
            // expected some of the time
        } finally {
            Releasables.close(mergers);
            Releasables.close(factory);
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testMultipleMergersInUse() {
        int numMergers = randomIntBetween(1, 50);
        int histogramsPerMerger = randomIntBetween(1, 10);
        int bucketLimit = randomIntBetween(10, 100);

        ExponentialHistogramMerger.Factory factory = null;
        List<List<ReleasableExponentialHistogram>> inputHistograms = new ArrayList<>();
        List<ExponentialHistogramMerger> mergers = new ArrayList<>();
        try {
            factory = ExponentialHistogramMerger.createFactory(bucketLimit, breaker());

            for (int i = 0; i < numMergers; i++) {
                List<ReleasableExponentialHistogram> histList = new ArrayList<>();
                inputHistograms.add(histList);
                mergers.add(factory.createMerger());
                for (int j = 0; j < histogramsPerMerger; j++) {
                    histList.add(ExponentialHistogramTestUtils.randomHistogram(bucketLimit, breaker()));
                }
            }

            for (int i = 0; i < histogramsPerMerger; i++) {
                for (int j = 0; j < numMergers; j++) {
                    mergers.get(j).add(inputHistograms.get(j).get(i));
                }
            }

            // check the results
            for (int j = 0; j < numMergers; j++) {
                try (var expected = ExponentialHistogram.merge(bucketLimit, breaker(), inputHistograms.get(j).iterator())) {
                    assertThat(mergers.get(j).get(), equalTo(expected));
                }
            }

        } finally {
            for (var histoList : inputHistograms) {
                Releasables.close(histoList);
            }
            Releasables.close(mergers);
            Releasables.close(factory);
        }
    }

    public void testBufferCapacities() {
        assertBufferCapacity(320, 0, 16);
        assertBufferCapacity(320, 1, 16);
        assertBufferCapacity(320, 16, 16);
        assertBufferCapacity(320, 17, 32);
        assertBufferCapacity(320, 256, 256);
        assertBufferCapacity(320, 257, 320);
        assertBufferCapacity(320, 1000, 320);
        assertBufferCapacity(256, 129, 256);
        assertBufferCapacity(256, 1000, 256);
        assertBufferCapacity(17, 16, 16);
        assertBufferCapacity(17, 17, 17);
        assertBufferCapacity(16, 5, 16);
        assertBufferCapacity(4, 2, 4);
        assertBufferCapacity(4, 100, 4);
    }

    private void assertBufferCapacity(int bucketLimit, int requiredBucketCount, int expectedCapacity) {
        try (MergerFactoryImpl factory = MergerFactoryImpl.create(bucketLimit, ExponentialHistogram.MAX_SCALE, breaker())) {
            FixedCapacityExponentialHistogram buffer = factory.acquireBuffer(requiredBucketCount);
            assertThat(buffer.getCapacity(), equalTo(expectedCapacity));
            factory.releaseBuffer(buffer);
            // the released buffer is pooled and handed out again for the same request
            FixedCapacityExponentialHistogram reacquired = factory.acquireBuffer(requiredBucketCount);
            assertThat(reacquired, sameInstance(buffer));
            factory.releaseBuffer(reacquired);
        }
    }

    /**
     * Verifies that the merger sizes its buffers for the upper bound of the merged bucket count,
     * only falling back to reducing the scale once the bucket limit is reached.
     */
    public void testBufferGrowth() {
        try (
            ExponentialHistogramMerger.Factory factory = ExponentialHistogramMerger.createFactory(320, breaker());
            ExponentialHistogramMerger merger = factory.createMerger()
        ) {
            // The upper bound of 10 buckets is based on the added histogram only, so the smallest pool is used
            merger.add(histogramWithPositiveBuckets(0, 10));
            assertThat(resultCapacity(merger), equalTo(16));
            assertThat(merger.get().scale(), equalTo(0));
            assertThat(merger.get().positiveBuckets().bucketCount(), equalTo(10));

            // The buckets overlap completely, but the buffer is sized for the upper bound of 20 buckets
            merger.add(histogramWithPositiveBuckets(0, 10));
            assertThat(resultCapacity(merger), equalTo(32));
            assertThat(merger.get().scale(), equalTo(0));
            assertThat(merger.get().positiveBuckets().bucketCount(), equalTo(10));

            merger.add(histogramWithPositiveBuckets(100, 10));
            assertThat(resultCapacity(merger), equalTo(32));
            assertThat(merger.get().scale(), equalTo(0));
            assertThat(merger.get().positiveBuckets().bucketCount(), equalTo(20));

            merger.add(histogramWithPositiveBuckets(1000, 100));
            assertThat(resultCapacity(merger), equalTo(128));
            assertThat(merger.get().scale(), equalTo(0));
            assertThat(merger.get().positiveBuckets().bucketCount(), equalTo(120));

            // 420 buckets exceed the bucket limit, so the full buffer is used and the scale is reduced by one
            merger.add(histogramWithPositiveBuckets(10000, 300));
            assertThat(resultCapacity(merger), equalTo(320));
            assertThat(merger.get().scale(), equalTo(-1));
            assertThat(merger.get().positiveBuckets().bucketCount(), equalTo(5 + 5 + 50 + 150));
        }
    }

    private static int resultCapacity(ExponentialHistogramMerger merger) {
        // the merger always stores its non-empty result in a buffer acquired from the factory
        return ((FixedCapacityExponentialHistogram) merger.get()).getCapacity();
    }

    private ExponentialHistogram histogramWithPositiveBuckets(long startIndex, int numBuckets) {
        try (ExponentialHistogramBuilder builder = ExponentialHistogram.builder(0, breaker())) {
            for (long i = startIndex; i < startIndex + numBuckets; i++) {
                builder.setPositiveBucket(i, 1);
            }
            ReleasableExponentialHistogram histogram = builder.build();
            autoReleaseOnTestEnd(histogram);
            return histogram;
        }
    }
}
