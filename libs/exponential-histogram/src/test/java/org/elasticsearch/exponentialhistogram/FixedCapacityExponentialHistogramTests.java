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
