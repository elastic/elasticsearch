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

import static org.hamcrest.Matchers.equalTo;

public class FixedCapacityExponentialHistogramTests extends ESTestCase {

    public void testValueCountUpdatedCorrectly() {

        FixedCapacityExponentialHistogram histogram = new FixedCapacityExponentialHistogram(100);

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
}
