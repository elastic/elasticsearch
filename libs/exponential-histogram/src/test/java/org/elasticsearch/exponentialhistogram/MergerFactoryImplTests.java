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
}
