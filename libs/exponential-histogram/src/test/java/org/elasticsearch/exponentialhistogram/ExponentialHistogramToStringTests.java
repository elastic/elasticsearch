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

import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramToStringTests extends ExponentialHistogramTestCase {

    public void testFullHistogram() {
        try (FixedCapacityExponentialHistogram histogram = FixedCapacityExponentialHistogram.create(10, breaker())) {
            histogram.setZeroBucket(ZeroBucket.create(42.0, 7));
            histogram.resetBuckets(10);
            histogram.setMin(-100);
            histogram.setMax(200);
            histogram.setSum(1234.5);
            histogram.tryAddBucket(2, 3, false); // negative bucket
            histogram.tryAddBucket(3, 4, false); // negative bucket
            histogram.tryAddBucket(4, 2, false); // negative bucket
            histogram.tryAddBucket(1, 5, true); // positive bucket
            histogram.tryAddBucket(5, 7, true); // positive bucket
            histogram.tryAddBucket(6, 1, true); // positive bucket
            String expected = "FixedCapacityExponentialHistogram{"
                + "scale=10, sum=1234.5, valueCount=29, min=-100.0, max=200.0, zeroThreshold=42.0, zeroCount=7,"
                + " negative=[2: 3, 3: 4, 4: 2], positive=[1: 5, 5: 7, 6: 1]}";
            assertThat(histogram.toString(), equalTo(expected));
        }
    }

    public void testEmptyHistogram() {
        ExponentialHistogram emptyHistogram = ExponentialHistogram.empty();
        String expected = "EmptyExponentialHistogram{scale=" + emptyHistogram.scale() + ", sum=0.0, valueCount=0, min=NaN, max=NaN}";
        assertThat(emptyHistogram.toString(), equalTo(expected));
    }
}
