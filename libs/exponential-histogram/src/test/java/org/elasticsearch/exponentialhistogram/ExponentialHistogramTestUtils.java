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

import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public abstract class ExponentialHistogramTestUtils {

    public static ExponentialHistogram randomHistogram() {
        return randomHistogram(ExponentialHistogramCircuitBreaker.noop());
    }

    public static ReleasableExponentialHistogram randomHistogram(ExponentialHistogramCircuitBreaker breaker) {
        boolean hasNegativeValues = randomBoolean();
        boolean hasPositiveValues = randomBoolean();
        boolean hasZeroValues = randomBoolean();
        double[] rawValues = IntStream.concat(
            IntStream.concat(
                hasNegativeValues ? IntStream.range(0, randomIntBetween(1, 1000)).map(i1 -> -1) : IntStream.empty(),
                hasPositiveValues ? IntStream.range(0, randomIntBetween(1, 1000)).map(i1 -> 1) : IntStream.empty()
            ),
            hasZeroValues ? IntStream.range(0, randomIntBetween(1, 100)).map(i1 -> 0) : IntStream.empty()
        ).mapToDouble(sign -> sign * (Math.pow(1_000_000, randomDouble()))).toArray();

        int numBuckets = randomIntBetween(4, 300);
        ReleasableExponentialHistogram histo = ExponentialHistogram.create(numBuckets, breaker, rawValues);
        // Setup a proper zeroThreshold based on a random chance
        if (histo.zeroBucket().count() > 0 && randomBoolean()) {
            double smallestNonZeroValue = DoubleStream.of(rawValues).map(Math::abs).filter(val -> val != 0).min().orElse(0.0);
            double zeroThreshold = smallestNonZeroValue * randomDouble();
            try (ReleasableExponentialHistogram releaseAfterCopy = histo) {
                histo = ExponentialHistogram.builder(histo, breaker)
                    .zeroBucket(ZeroBucket.create(zeroThreshold, histo.zeroBucket().count()))
                    .build();
            }
        }
        return histo;
    }
}
