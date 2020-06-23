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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class LongKeyedBucketOrdsTests extends ESTestCase {
    private final MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

    public void testExplicitCollectsFromSingleBucket() {
        collectsFromSingleBucketCase(LongKeyedBucketOrds.build(bigArrays, true));
    }

    public void testSurpriseCollectsFromSingleBucket() {
        collectsFromSingleBucketCase(LongKeyedBucketOrds.build(bigArrays, false));
    }

    private void collectsFromSingleBucketCase(LongKeyedBucketOrds ords) {
        try {
            // Test a few explicit values
            assertThat(ords.add(0, 0), equalTo(0L));
            assertThat(ords.add(0, 1000), equalTo(1L));
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(0, 1000), equalTo(-2L));
            assertThat(ords.find(0, 0), equalTo(0L));
            assertThat(ords.find(0, 1000), equalTo(1L));

            // And some random values
            Set<Long> seen = new HashSet<>();
            seen.add(0L);
            seen.add(1000L);
            assertThat(ords.size(), equalTo(2L));
            long[] values = new long[scaledRandomIntBetween(1, 10000)];
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(seen::contains, ESTestCase::randomLong);
                seen.add(values[i]);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.find(0, values[i]), equalTo(-1L));
                assertThat(ords.add(0, values[i]), equalTo(i + 2L));
                assertThat(ords.find(0, values[i]), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
                if (randomBoolean()) {
                    assertThat(ords.add(0, 0), equalTo(-1L));
                }
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.add(0, values[i]), equalTo(-1 - (i + 2L)));
            }

            // And the explicit values are still ok
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(0, 1000), equalTo(-2L));

            // Check counting values
            assertThat(ords.bucketsInOrd(0), equalTo(values.length + 2L));

            // Check iteration
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(0);
            assertTrue(ordsEnum.next());
            assertThat(ordsEnum.ord(), equalTo(0L));
            assertThat(ordsEnum.value(), equalTo(0L));
            assertTrue(ordsEnum.next());
            assertThat(ordsEnum.ord(), equalTo(1L));
            assertThat(ordsEnum.value(), equalTo(1000L));
            for (int i = 0; i < values.length; i++) {
                assertTrue(ordsEnum.next());
                assertThat(ordsEnum.ord(), equalTo(i + 2L));
                assertThat(ordsEnum.value(), equalTo(values[i]));
            }
            assertFalse(ordsEnum.next());

            assertThat(ords.maxOwningBucketOrd(), equalTo(0L));
        } finally {
            ords.close();
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/58353")
    public void testCollectsFromManyBuckets() {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, false)) {
            // Test a few explicit values
            assertThat(ords.add(0, 0), equalTo(0L));
            assertThat(ords.add(1, 0), equalTo(1L));
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(1, 0), equalTo(-2L));
            assertThat(ords.size(), equalTo(2L));
            assertThat(ords.find(0, 0), equalTo(0L));
            assertThat(ords.find(1, 0), equalTo(1L));

            // And some random values
            Set<OwningBucketOrdAndValue> seen = new HashSet<>();
            seen.add(new OwningBucketOrdAndValue(0, 0));
            seen.add(new OwningBucketOrdAndValue(1, 0));
            OwningBucketOrdAndValue[] values = new OwningBucketOrdAndValue[scaledRandomIntBetween(1, 10000)];
            long maxOwningBucketOrd = scaledRandomIntBetween(0, values.length);
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValueOtherThanMany(seen::contains, () ->
                        new OwningBucketOrdAndValue(randomLongBetween(0, maxOwningBucketOrd), randomLong()));
                seen.add(values[i]);
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(-1L));
                assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
                assertThat(ords.find(values[i].owningBucketOrd, values[i].value), equalTo(i + 2L));
                assertThat(ords.size(), equalTo(i + 3L));
                if (randomBoolean()) {
                    assertThat(ords.add(0, 0), equalTo(-1L));
                }
            }
            for (int i = 0; i < values.length; i++) {
                assertThat(ords.add(values[i].owningBucketOrd, values[i].value), equalTo(-1 - (i + 2L)));
            }

            // And the explicit values are still ok
            assertThat(ords.add(0, 0), equalTo(-1L));
            assertThat(ords.add(1, 0), equalTo(-2L));


            for (long owningBucketOrd = 0; owningBucketOrd <= maxOwningBucketOrd; owningBucketOrd++) {
                long expectedCount = 0;
                LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = ords.ordsEnum(owningBucketOrd);
                if (owningBucketOrd <= 1) {
                    expectedCount++;
                    assertTrue(ordsEnum.next());
                    assertThat(ordsEnum.ord(), equalTo(owningBucketOrd));
                    assertThat(ordsEnum.value(), equalTo(0L));
                }
                for (int i = 0; i < values.length; i++) {
                    if (values[i].owningBucketOrd == owningBucketOrd) {
                        expectedCount++;
                        assertTrue(ordsEnum.next());
                        assertThat(ordsEnum.ord(), equalTo(i + 2L));
                        assertThat(ordsEnum.value(), equalTo(values[i].value));
                    }
                }
                assertFalse(ordsEnum.next());

                assertThat(ords.bucketsInOrd(owningBucketOrd), equalTo(expectedCount));
            }
            assertFalse(ords.ordsEnum(randomLongBetween(maxOwningBucketOrd + 1, Long.MAX_VALUE)).next());
            assertThat(ords.bucketsInOrd(randomLongBetween(maxOwningBucketOrd + 1, Long.MAX_VALUE)), equalTo(0L));

            assertThat(ords.maxOwningBucketOrd(), greaterThanOrEqualTo(maxOwningBucketOrd));
        }
    }

    private class OwningBucketOrdAndValue {
        private final long owningBucketOrd;
        private final long value;

        OwningBucketOrdAndValue(long owningBucketOrd, long value) {
            this.owningBucketOrd = owningBucketOrd;
            this.value = value;
        }

        @Override
        public String toString() {
            return owningBucketOrd + "/" + value;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            OwningBucketOrdAndValue other = (OwningBucketOrdAndValue) obj;
            return owningBucketOrd == other.owningBucketOrd && value == other.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(owningBucketOrd, value);
        }
    }
}
