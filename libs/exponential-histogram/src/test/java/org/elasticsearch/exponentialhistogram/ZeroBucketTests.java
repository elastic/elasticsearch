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
import static org.hamcrest.Matchers.not;

public class ZeroBucketTests extends ExponentialHistogramTestCase {

    public void testMinimalBucketHasZeroThreshold() {
        assertThat(ZeroBucket.minimalWithCount(42).zeroThreshold(), equalTo(0.0));
    }

    public void testExactThresholdPreserved() {
        ZeroBucket bucket = ZeroBucket.create(3.0, 10);
        assertThat(bucket.zeroThreshold(), equalTo(3.0));
        assertThat(bucket.isIndexBased(), equalTo(false));
    }

    public void testMergingPreservesExactThreshold() {
        ZeroBucket bucketA = ZeroBucket.create(3.0, 10);
        ZeroBucket bucketB = ZeroBucket.create(3.5, 20);
        ZeroBucket merged = bucketA.merge(bucketB);
        assertThat(merged.zeroThreshold(), equalTo(3.5));
        assertThat(merged.count(), equalTo(30L));
        assertThat(merged.isIndexBased(), equalTo(false));
    }

    public void testBucketCollapsingPreservesExactThreshold() {
        ExponentialHistogram histo = createAutoReleasedHistogram(
            b -> b.scale(0).setPositiveBucket(0, 42) // bucket [1,2]
        );

        ZeroBucket bucketA = ZeroBucket.create(3.0, 10);

        CopyableBucketIterator iterator = histo.positiveBuckets().iterator();
        ZeroBucket merged = bucketA.collapseOverlappingBuckets(iterator);

        assertThat(iterator.hasNext(), equalTo(false));
        assertThat(merged.zeroThreshold(), equalTo(3.0));
        assertThat(merged.count(), equalTo(52L));
        assertThat(merged.isIndexBased(), equalTo(false));
    }

    public void testHashCodeEquality() {
        assertEqualsContract(ZeroBucket.minimalEmpty(), ZeroBucket.create(0.0, 0));
        assertThat(ZeroBucket.minimalEmpty(), not(equalTo(ZeroBucket.create(0.0, 1))));
        assertThat(ZeroBucket.minimalEmpty(), not(equalTo(ZeroBucket.create(0.1, 0))));

        assertEqualsContract(ZeroBucket.minimalWithCount(42), ZeroBucket.create(0.0, 42));
        assertThat(ZeroBucket.minimalWithCount(42), not(equalTo(ZeroBucket.create(0.0, 12))));
        assertThat(ZeroBucket.minimalWithCount(42), not(equalTo(ZeroBucket.create(0.1, 42))));

        ZeroBucket minimalEmpty = ZeroBucket.minimalEmpty();
        assertEqualsContract(ZeroBucket.minimalWithCount(42), ZeroBucket.create(minimalEmpty.index(), minimalEmpty.scale(), 42));
        assertThat(ZeroBucket.minimalWithCount(42), not(equalTo(ZeroBucket.create(minimalEmpty.index(), minimalEmpty.scale(), 41))));
        assertThat(ZeroBucket.minimalWithCount(42), not(equalTo(ZeroBucket.create(minimalEmpty.index() + 1, minimalEmpty.scale(), 42))));

        assertEqualsContract(ZeroBucket.create(123.56, 123), ZeroBucket.create(123.56, 123));
        assertThat(ZeroBucket.create(123.56, 123), not(equalTo(ZeroBucket.create(123.57, 123))));
        assertThat(ZeroBucket.create(123.56, 123), not(equalTo(ZeroBucket.create(123.56, 12))));

        // the exponentially scaled numbers (index=2, scale=0) and (index=1, scale=-1) both represent 4.0
        // therefore the zero buckets should be equal
        assertEqualsContract(ZeroBucket.create(2, 0, 123), ZeroBucket.create(1, -1, 123));
        assertEqualsContract(ZeroBucket.create(4, 1, 123), ZeroBucket.create(1, -1, 123));
    }

    void assertEqualsContract(ZeroBucket a, ZeroBucket b) {
        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), equalTo(b.hashCode()));
    }

}
