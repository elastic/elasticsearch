/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialHistogramGeneratorTests extends ESTestCase {

    public void testVeryLargeValue() {
        double value = Double.MAX_VALUE / 10;
        ExponentialHistogram histo = ExponentialHistogramGenerator.createFor(value);

        long index = histo.positiveBuckets().iterator().peekIndex();
        int scale = histo.scale();

        double lowerBound = ExponentialScaleUtils.getLowerBucketBoundary(index, scale);
        double upperBound = ExponentialScaleUtils.getUpperBucketBoundary(index, scale);

        assertThat("Lower bucket boundary should be smaller than value", lowerBound, lessThanOrEqualTo(value));
        assertThat("Upper bucket boundary should be greater than value", upperBound, greaterThanOrEqualTo(value));
    }

}
