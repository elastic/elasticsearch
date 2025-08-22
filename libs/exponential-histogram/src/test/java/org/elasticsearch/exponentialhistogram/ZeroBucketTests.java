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

public class ZeroBucketTests extends ExponentialHistogramTestCase {

    public void testMinimalBucketHasZeroThreshold() {
        assertThat(ZeroBucket.minimalWithCount(42).zeroThreshold(), equalTo(0.0));
    }

    public void testExactThresholdPreserved() {
        ZeroBucket bucket = new ZeroBucket(3.0, 10);
        assertThat(bucket.zeroThreshold(), equalTo(3.0));
    }

    public void testMergingPreservesExactThreshold() {
        ZeroBucket bucketA = new ZeroBucket(3.0, 10);
        ZeroBucket bucketB = new ZeroBucket(3.5, 20);
        ZeroBucket merged = bucketA.merge(bucketB);
        assertThat(merged.zeroThreshold(), equalTo(3.5));
        assertThat(merged.count(), equalTo(30L));
    }

    public void testBucketCollapsingPreservesExactThreshold() {
        FixedCapacityExponentialHistogram histo = createAutoReleasedHistogram(2);
        histo.resetBuckets(0);
        histo.tryAddBucket(0, 42, true); // bucket [1,2]

        ZeroBucket bucketA = new ZeroBucket(3.0, 10);

        CopyableBucketIterator iterator = histo.positiveBuckets().iterator();
        ZeroBucket merged = bucketA.collapseOverlappingBuckets(iterator);

        assertThat(iterator.hasNext(), equalTo(false));
        assertThat(merged.zeroThreshold(), equalTo(3.0));
        assertThat(merged.count(), equalTo(52L));
    }

}
