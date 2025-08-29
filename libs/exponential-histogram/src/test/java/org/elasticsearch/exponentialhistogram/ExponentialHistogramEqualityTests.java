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
import java.util.Collections;
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
        ExponentialHistogram histo = randomHistogram();
        ExponentialHistogram copy = copyWithModification(histo, null);
        ExponentialHistogram modifiedCopy = copyWithModification(histo, modification);

        assertThat(histo, equalTo(copy));
        assertThat(histo.hashCode(), equalTo(copy.hashCode()));
        assertThat(histo, not(equalTo(modifiedCopy)));
    }

    public void testHashQuality() {
        ExponentialHistogram histo = randomHistogram();
        // of 10 tries, at least one should produce a different hash code
        for (int i = 0; i < 10; i++) {
            ExponentialHistogram modifiedCopy = copyWithModification(histo, modification);
            if (histo.hashCode() != modifiedCopy.hashCode()) {
                return;
            }
        }
        fail("Could not produce different hash code after 10 tries");
    }

    private ExponentialHistogram randomHistogram() {
        return createAutoReleasedHistogram(randomIntBetween(4, 20), randomDoubles(randomIntBetween(0, 200)).toArray());
    }

    private ExponentialHistogram copyWithModification(ExponentialHistogram toCopy, Modification modification) {
        int totalBucketCount = getBucketCount(toCopy.positiveBuckets(), toCopy.negativeBuckets());
        FixedCapacityExponentialHistogram copy = createAutoReleasedHistogram(totalBucketCount + 2);
        if (modification == Modification.SCALE) {
            copy.resetBuckets((int) createRandomLongBetweenOtherThan(MIN_SCALE, MAX_SCALE, toCopy.scale()));
        } else {
            copy.resetBuckets(toCopy.scale());
        }
        if (modification == Modification.SUM) {
            copy.setSum(randomDouble());
        } else {
            copy.setSum(toCopy.sum());
        }
        if (modification == Modification.MIN) {
            copy.setMin(randomDouble());
        } else {
            copy.setMin(toCopy.min());
        }
        long zeroCount = toCopy.zeroBucket().count();
        double zeroThreshold = toCopy.zeroBucket().zeroThreshold();
        if (modification == Modification.ZERO_COUNT) {
            zeroCount = createRandomLongBetweenOtherThan(0, Long.MAX_VALUE, zeroCount);
        } else if (modification == Modification.ZERO_THRESHOLD) {
            zeroThreshold = randomDouble();
        }
        copy.setZeroBucket(ZeroBucket.create(zeroThreshold, zeroCount));
        copyBuckets(copy, toCopy.negativeBuckets(), modification == Modification.NEGATIVE_BUCKETS, false);
        copyBuckets(copy, toCopy.positiveBuckets(), modification == Modification.POSITIVE_BUCKETS, true);

        return copy;
    }

    private void copyBuckets(
        FixedCapacityExponentialHistogram into,
        ExponentialHistogram.Buckets buckets,
        boolean modify,
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
        if (modify) {
            if (counts.isEmpty() == false && randomBoolean()) {
                int toModify = randomIntBetween(0, indices.size() - 1);
                counts.set(toModify, createRandomLongBetweenOtherThan(1, Long.MAX_VALUE, counts.get(toModify)));
            } else {
                insertRandomBucket(indices, counts);
            }
        }
        for (int i = 0; i < indices.size(); i++) {
            into.tryAddBucket(indices.get(i), counts.get(i), isPositive);
        }
    }

    private void insertRandomBucket(List<Long> indices, List<Long> counts) {
        long minIndex = indices.stream().mapToLong(Long::longValue).min().orElse(MIN_INDEX);
        long maxIndex = indices.stream().mapToLong(Long::longValue).min().orElse(MAX_INDEX);
        long newIndex;
        do {
            newIndex = randomLongBetween(Math.max(MIN_INDEX, minIndex - 10), Math.min(MAX_INDEX, maxIndex + 10));
        } while (indices.contains(newIndex));
        int position = -(Collections.binarySearch(indices, newIndex) + 1);
        indices.add(position, newIndex);
        counts.add(position, randomLongBetween(1, Long.MAX_VALUE));
    }

    private static long createRandomLongBetweenOtherThan(long min, long max, long notAllowedValue) {
        assert min != max || notAllowedValue != min;
        long result;
        do {
            result = randomLongBetween(min, max);
        } while (result == notAllowedValue);
        return result;
    }

    private static int getBucketCount(ExponentialHistogram.Buckets... buckets) {
        int count = 0;
        for (ExponentialHistogram.Buckets val : buckets) {
            BucketIterator it = val.iterator();
            while (it.hasNext()) {
                count++;
                it.advance();
            }
        }
        return count;
    }
}
