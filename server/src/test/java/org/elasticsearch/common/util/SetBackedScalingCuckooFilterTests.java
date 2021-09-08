/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SetBackedScalingCuckooFilterTests extends AbstractWireSerializingTestCase<SetBackedScalingCuckooFilter> {

    @Override
    protected SetBackedScalingCuckooFilter createTestInstance() {
        SetBackedScalingCuckooFilter bloom = new SetBackedScalingCuckooFilter(1000, Randomness.get(), 0.01);

        int num = randomIntBetween(0, 10);
        for (int i = 0; i < num; i++) {
            bloom.add(randomLong());
        }

        return bloom;
    }

    @Override
    protected Writeable.Reader<SetBackedScalingCuckooFilter> instanceReader() {
        return in -> new SetBackedScalingCuckooFilter(in, Randomness.get());
    }

    @Override
    protected SetBackedScalingCuckooFilter mutateInstance(SetBackedScalingCuckooFilter instance) throws IOException {
        SetBackedScalingCuckooFilter newInstance =
            new SetBackedScalingCuckooFilter(instance.getThreshold(), instance.getRng(), instance.getFpp());
        newInstance.merge(instance);
        int num = randomIntBetween(1, 10);
        for (int i = 0; i < num; i++) {
            newInstance.add(randomLong());
        }
        return newInstance;
    }

    public void testExact() {
        int threshold = randomIntBetween(1000, 10000);
        SetBackedScalingCuckooFilter filter = new SetBackedScalingCuckooFilter(threshold, Randomness.get(), 0.01);

        int size = 0;
        Set<Long> values = new HashSet<>();
        Set<Long> hashed = new HashSet<>(values.size());
        while (size < threshold - 100) {
            long value = randomLong();
            filter.add(value);
            boolean newValue = values.add(value);
            if (newValue) {
                Long hash = MurmurHash3.murmur64(value);
                hashed.add(hash);

                size += 16;
            }
        }
        assertThat(filter.hashes.size(), equalTo(hashed.size()));
        assertThat(filter.hashes, equalTo(hashed));
        assertNull(filter.filters);

        for (Long value : values) {
            assertThat(filter.mightContain(value), equalTo(true));
        }
    }

    public void testConvert() {
        int threshold = randomIntBetween(1000, 10000);
        SetBackedScalingCuckooFilter filter = new SetBackedScalingCuckooFilter(threshold, Randomness.get(), 0.01);

        int counter = 0;
        Set<Long> values = new HashSet<>();
        while (counter < threshold + 100) {
            long value = randomLong();
            filter.add(value);
            boolean newValue = values.add(value);
            if (newValue) {
                counter += 1;
            }
        }
        assertNull(filter.hashes);
        assertThat(filter.filters.size(), greaterThan(0));

        int incorrect = 0;
        for (Long v : values) {
            if (filter.mightContain(v) == false) {
                incorrect += 1;
            }
        }
        double fppRate = (double) incorrect / values.size();
        assertThat(fppRate, lessThanOrEqualTo(0.001));
    }

    public void testConvertTwice() {
        int threshold = randomIntBetween(1000, 10000);
        SetBackedScalingCuckooFilter filter = new SetBackedScalingCuckooFilter(threshold, Randomness.get(), 0.01);

        int counter = 0;
        Set<Long> values = new HashSet<>();
        while (counter < threshold + 100) {
            long value = randomLong();
            filter.add(value);
            boolean newValue = values.add(value);
            if (newValue) {
                counter += 1;
            }
        }
        assertNull(filter.hashes);
        assertThat(filter.filters.size(), greaterThan(0));
        IllegalStateException e = expectThrows(IllegalStateException.class, filter::convert);
        assertThat(e.getMessage(), equalTo("Cannot convert SetBackedScalingCuckooFilter to approximate " +
            "when it has already been converted."));
    }

    public void testMergeSmall() {
        int threshold = 1000;

        // Setup the first filter
        SetBackedScalingCuckooFilter filter = new SetBackedScalingCuckooFilter(threshold, Randomness.get(), 0.01);

        int counter = 0;
        Set<Long> values = new HashSet<>();
        while (counter < threshold + 1) {
            long value = randomLong();
            filter.add(value);
            boolean newValue = values.add(value);
            if (newValue) {
                counter += 1;
            }
        }
        assertNull(filter.hashes);
        assertThat(filter.filters.size(), greaterThan(0));

        int incorrect = 0;
        for (Long v : values) {
            if (filter.mightContain(v) == false) {
                incorrect += 1;
            }
        }
        double fppRate = (double) incorrect / values.size();
        assertThat(fppRate, lessThanOrEqualTo(0.001));

        // Setup the second filter
        SetBackedScalingCuckooFilter filter2 = new SetBackedScalingCuckooFilter(threshold, Randomness.get(), 0.01);
        counter = 0;
        Set<Long> values2 = new HashSet<>();
        while (counter < threshold + 1) {
            long value = randomLong();
            filter2.add(value);
            boolean newValue = values2.add(value);
            if (newValue) {
                counter += 1;
            }
        }
        assertNull(filter2.hashes);
        assertThat(filter2.filters.size(), greaterThan(0));

        incorrect = 0;
        for (Long v : values2) {
            if (filter2.mightContain(v) == false) {
                incorrect += 1;
            }
        }
        fppRate = (double) incorrect / values2.size();
        assertThat(fppRate, lessThanOrEqualTo(0.001));

        // now merge and verify the combined set
        filter.merge(filter2);
        incorrect = 0;
        for (Long v : values) {
            if (filter.mightContain(v) == false) {
                incorrect += 1;
            }
        }
        for (Long v : values2) {
            if (filter.mightContain(v) == false) {
                incorrect += 1;
            }
        }
        fppRate = (double) incorrect / (values.size() + values2.size());
        assertThat(fppRate, lessThanOrEqualTo(0.001));
    }

    public void testMergeIncompatible() {
        SetBackedScalingCuckooFilter filter1 = new SetBackedScalingCuckooFilter(100, Randomness.get(), 0.01);
        SetBackedScalingCuckooFilter filter2 = new SetBackedScalingCuckooFilter(1000, Randomness.get(), 0.01);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> filter1.merge(filter2));
        assertThat(e.getMessage(), equalTo("Cannot merge other CuckooFilter because thresholds do not match: [100] vs [1000]"));

        SetBackedScalingCuckooFilter filter3 = new SetBackedScalingCuckooFilter(100, Randomness.get(), 0.001);
        e = expectThrows(IllegalStateException.class, () -> filter1.merge(filter3));
        assertThat(e.getMessage(), equalTo("Cannot merge other CuckooFilter because precisions do not match: [0.01] vs [0.001]"));
    }

    public void testBadParameters() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new SetBackedScalingCuckooFilter(-1, Randomness.get(), 0.11));
        assertThat(e.getMessage(), equalTo("[threshold] must be a positive integer"));

        e = expectThrows(IllegalArgumentException.class,
            () -> new SetBackedScalingCuckooFilter(1000000, Randomness.get(), 0.11));
        assertThat(e.getMessage(), equalTo("[threshold] must be smaller than [500000]"));

        e = expectThrows(IllegalArgumentException.class,
            () -> new SetBackedScalingCuckooFilter(100, Randomness.get(), -1.0));
        assertThat(e.getMessage(), equalTo("[fpp] must be a positive double"));
    }
}
