/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.util;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
        SetBackedScalingCuckooFilter newInstance = new SetBackedScalingCuckooFilter(instance);
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
        Set<MurmurHash3.Hash128> hashed = new HashSet<>(values.size());
        while (size < threshold - 100) {
            long value = randomLong();
            filter.add(value);
            boolean newValue = values.add(value);
            if (newValue) {
                byte[] bytes = Numbers.longToBytes(value);
                MurmurHash3.Hash128 hash128 = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
                hashed.add(hash128);

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
}
