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

import java.util.OptionalLong;

import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramTests extends ExponentialHistogramTestCase {

    public void testDefaultBucketImplementation() {
        try (
            ReleasableExponentialHistogram delegate = ExponentialHistogram.builder(0, breaker())
                .setPositiveBucket(0, 2)
                .setPositiveBucket(10, 20)
                .setPositiveBucket(20, 30)
                .build()
        ) {

            ExponentialHistogram.Buckets bucketsWithDefaultImplementation = new ExponentialHistogram.Buckets() {
                @Override
                public CopyableBucketIterator iterator() {
                    return delegate.positiveBuckets().iterator();
                }

                @Override
                public OptionalLong maxBucketIndex() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public long valueCount() {
                    throw new UnsupportedOperationException();
                }
            };
            assertThat(bucketsWithDefaultImplementation.bucketCount(), equalTo(3));
        }

    }

}
