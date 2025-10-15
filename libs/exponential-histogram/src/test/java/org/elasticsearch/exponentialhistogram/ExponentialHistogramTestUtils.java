/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import java.util.List;
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
            double smallestNonZeroValue = DoubleStream.of(rawValues)
                .map(Math::abs)
                .filter(val -> val != 0)
                .min()
                .orElse(0.0);
            double zeroThreshold = smallestNonZeroValue * randomDouble();
            try(ReleasableExponentialHistogram releaseAfterCopy = histo) {
                histo = ExponentialHistogram.builder(histo, breaker)
                    .zeroBucket(ZeroBucket.create(zeroThreshold, histo.zeroBucket().count()))
                    .build();
            }
        }
        return histo;
    }
}
