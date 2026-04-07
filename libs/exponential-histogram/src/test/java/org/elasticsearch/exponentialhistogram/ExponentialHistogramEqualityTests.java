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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ExponentialHistogramEqualityTests extends ExponentialHistogramTestCase {

    public enum Modification {
        SCALE,
        SUM,
        MIN,
        MAX,
        ZERO_THRESHOLD,
        ZERO_COUNT,
        POSITIVE_BUCKETS,
        NEGATIVE_BUCKETS
    }

    private final Modification modification;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(Modification.values()).map(modification -> new Object[] { modification }).toList();
    }

    public ExponentialHistogramEqualityTests(Modification modification) {
        this.modification = modification;
    }

    public void testEquality() {
        ReleasableExponentialHistogram histo = ExponentialHistogramTestUtils.randomHistogram(breaker());
        autoReleaseOnTestEnd(histo);
        ReleasableExponentialHistogram copy = ExponentialHistogram.builder(histo, breaker()).build();
        autoReleaseOnTestEnd(copy);
        ExponentialHistogram modifiedCopy = copyWithModification(histo, modification);

        assertThat(histo, equalTo(copy));
        assertThat(histo.hashCode(), equalTo(copy.hashCode()));
        assertThat(histo, not(equalTo(modifiedCopy)));
    }

    public void testHashQuality() {
        ReleasableExponentialHistogram histo = ExponentialHistogramTestUtils.randomHistogram(breaker());
        autoReleaseOnTestEnd(histo);
        // of 10 tries, at least one should produce a different hash code
        for (int i = 0; i < 10; i++) {
            ExponentialHistogram modifiedCopy = copyWithModification(histo, modification);
            if (histo.hashCode() != modifiedCopy.hashCode()) {
                return;
            }
        }
        fail("Could not produce different hash code after 10 tries");
    }

    private ExponentialHistogram copyWithModification(ExponentialHistogram toCopy, Modification modification) {
        ExponentialHistogramBuilder copyBuilder = ExponentialHistogram.builder(toCopy, breaker());
        switch (modification) {
            case SCALE -> copyBuilder.scale((int) createRandomLongBetweenOtherThan(MIN_SCALE, MAX_SCALE, toCopy.scale()));
            case SUM -> copyBuilder.sum(randomDouble());
            case MIN -> copyBuilder.min(randomDouble());
            case MAX -> copyBuilder.max(randomDouble());
            case ZERO_THRESHOLD -> copyBuilder.zeroBucket(ZeroBucket.create(randomDouble(), toCopy.zeroBucket().count()));
            case ZERO_COUNT -> copyBuilder.zeroBucket(
                ZeroBucket.create(
                    toCopy.zeroBucket().zeroThreshold(),
                    createRandomLongBetweenOtherThan(0, Long.MAX_VALUE, toCopy.zeroBucket().count())
                )
            );
            case POSITIVE_BUCKETS -> modifyBuckets(copyBuilder, toCopy.positiveBuckets(), true);
            case NEGATIVE_BUCKETS -> modifyBuckets(copyBuilder, toCopy.negativeBuckets(), false);
        }

        ReleasableExponentialHistogram result = copyBuilder.build();
        autoReleaseOnTestEnd(result);
        return result;
    }

    private ExponentialHistogramBuilder modifyBuckets(
        ExponentialHistogramBuilder builder,
        ExponentialHistogram.Buckets buckets,
        boolean isPositive
    ) {
        List<Long> indices = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        BucketIterator it = buckets.iterator();
        while (it.hasNext()) {
            indices.add(it.peekIndex());
            counts.add(it.peekCount());
            it.advance();
        }
        long toModifyIndex;
        long toModifyCount;
        if (counts.isEmpty() == false && randomBoolean()) {
            // Modify existing bucket
            int position = randomIntBetween(0, indices.size() - 1);
            toModifyIndex = indices.get(position);
            toModifyCount = createRandomLongBetweenOtherThan(1, Long.MAX_VALUE, counts.get(position));
        } else {
            // Add new bucket
            long minIndex = indices.stream().mapToLong(Long::longValue).min().orElse(MIN_INDEX);
            long maxIndex = indices.stream().mapToLong(Long::longValue).min().orElse(MAX_INDEX);
            do {
                toModifyIndex = randomLongBetween(Math.max(MIN_INDEX, minIndex - 10), Math.min(MAX_INDEX, maxIndex + 10));
            } while (indices.contains(toModifyIndex));
            toModifyCount = randomLongBetween(1, Long.MAX_VALUE);
        }
        if (isPositive) {
            builder.setPositiveBucket(toModifyIndex, toModifyCount);
        } else {
            builder.setNegativeBucket(toModifyIndex, toModifyCount);
        }
        return builder;
    }

    private static long createRandomLongBetweenOtherThan(long min, long max, long notAllowedValue) {
        assert min != max || notAllowedValue != min;
        long result;
        do {
            result = randomLongBetween(min, max);
        } while (result == notAllowedValue);
        return result;
    }
}
