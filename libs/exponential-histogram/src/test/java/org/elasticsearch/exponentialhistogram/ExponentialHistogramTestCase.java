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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.function.Consumer;

public abstract class ExponentialHistogramTestCase extends ESTestCase {

    private final ArrayList<ReleasableExponentialHistogram> releaseBeforeEnd = new ArrayList<>();

    /**
     * Release all histograms registered via {@link #autoReleaseOnTestEnd(ReleasableExponentialHistogram)}
     * before {@link ESTestCase} checks for unreleased bytes.
     */
    @After
    public void releaseHistograms() {
        Releasables.close(releaseBeforeEnd);
        releaseBeforeEnd.clear();
    }

    ExponentialHistogramCircuitBreaker breaker(CircuitBreaker esBreaker) {
        return bytesAllocated -> {
            if (bytesAllocated > 0) {
                esBreaker.addEstimateBytesAndMaybeBreak(bytesAllocated, "exponential-histo-test-case");
            } else {
                esBreaker.addWithoutBreaking(bytesAllocated);
            }
        };
    }

    ExponentialHistogramCircuitBreaker breaker() {
        return breaker(newLimitedBreaker(ByteSizeValue.ofMb(100)));
    }

    void autoReleaseOnTestEnd(ReleasableExponentialHistogram toRelease) {
        releaseBeforeEnd.add(toRelease);
    }

    ExponentialHistogram createAutoReleasedHistogram(Consumer<ExponentialHistogramBuilder> customizer) {
        ExponentialHistogramBuilder resultBuilder = ExponentialHistogram.builder(ExponentialHistogram.MAX_SCALE, breaker());
        customizer.accept(resultBuilder);
        ReleasableExponentialHistogram result = resultBuilder.build();
        releaseBeforeEnd.add(result);
        return result;
    }

    protected ExponentialHistogram createAutoReleasedHistogram(int numBuckets, double... values) {
        ReleasableExponentialHistogram result = ExponentialHistogram.create(numBuckets, breaker(), values);
        releaseBeforeEnd.add(result);
        return result;
    }
}
