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

class EmptyExponentialHistogram extends AbstractExponentialHistogram implements ReleasableExponentialHistogram {

    static final EmptyExponentialHistogram INSTANCE = new EmptyExponentialHistogram();

    /**
     * The default empty histogram always has MAX_SCALE to not cause accidental downscaling
     * when combining with other histograms.
     */
    private static final int SCALE = ExponentialHistogram.MAX_SCALE;

    private static class EmptyBuckets implements Buckets {

        private static final EmptyBuckets INSTANCE = new EmptyBuckets();
        private static final CopyableBucketIterator EMPTY_ITERATOR = new BucketArrayIterator(SCALE, new long[0], new long[0], 0, 0);

        @Override
        public CopyableBucketIterator iterator() {
            return EMPTY_ITERATOR;
        }

        @Override
        public OptionalLong maxBucketIndex() {
            return OptionalLong.empty();
        }

        @Override
        public long valueCount() {
            return 0;
        }
    }

    @Override
    public void close() {}

    @Override
    public int scale() {
        return SCALE;
    }

    @Override
    public ZeroBucket zeroBucket() {
        return ZeroBucket.minimalEmpty();
    }

    @Override
    public Buckets positiveBuckets() {
        return EmptyBuckets.INSTANCE;
    }

    @Override
    public Buckets negativeBuckets() {
        return EmptyBuckets.INSTANCE;
    }

    @Override
    public double sum() {
        return 0;
    }

    @Override
    public double min() {
        return Double.NaN;
    }

    @Override
    public double max() {
        return Double.NaN;
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }
}
