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

package org.elasticsearch.test;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

public abstract class InternalMultiBucketAggregationTestCase<T extends InternalAggregation & MultiBucketsAggregation>
        extends InternalAggregationTestCase<T> {

    private static final int DEFAULT_MAX_NUMBER_OF_BUCKETS = 10;

    private Supplier<InternalAggregations> subAggregationsSupplier;
    private int maxNumberOfBuckets = DEFAULT_MAX_NUMBER_OF_BUCKETS;

    protected int randomNumberOfBuckets() {
        return randomIntBetween(minNumberOfBuckets(), maxNumberOfBuckets());
    }

    protected int minNumberOfBuckets() {
        return 0;
    }

    protected int maxNumberOfBuckets() {
        return maxNumberOfBuckets;
    }

    public void setMaxNumberOfBuckets(int maxNumberOfBuckets) {
        this.maxNumberOfBuckets = maxNumberOfBuckets;
    }

    public void setSubAggregationsSupplier(Supplier<InternalAggregations> subAggregationsSupplier) {
        this.subAggregationsSupplier = subAggregationsSupplier;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (randomBoolean()) {
            subAggregationsSupplier = () -> InternalAggregations.EMPTY;
        } else {
            subAggregationsSupplier = () -> {
                final int numAggregations = randomIntBetween(1, 3);
                List<InternalAggregation> aggs = new ArrayList<>();
                for (int i = 0; i < numAggregations; i++) {
                    aggs.add(createTestInstance(randomAlphaOfLength(5), emptyMap(), InternalAggregations.EMPTY));
                }
                return new InternalAggregations(aggs);
            };
        }
    }

    @Override
    protected final T createTestInstance(String name, Map<String, Object> metadata) {
        T instance = createTestInstance(name, metadata, subAggregationsSupplier.get());
        assert instance.getBuckets().size() <= maxNumberOfBuckets() :
                "Maximum number of buckets exceeded for " + instance.getClass().getSimpleName() + " aggregation";
        return instance;
    }

    protected abstract T createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations);

    protected abstract Class<? extends ParsedMultiBucketAggregation> implementationClass();

    @Override
    protected final void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) {
        assertMultiBucketsAggregations(aggregation, parsedAggregation, false);
    }

    public void testIterators() throws IOException {
        final T aggregation = createTestInstance();
        assertMultiBucketsAggregations(aggregation, parseAndAssert(aggregation, false, false), true);
    }

    private void assertMultiBucketsAggregations(Aggregation expected, Aggregation actual, boolean checkOrder) {
        assertTrue(expected instanceof MultiBucketsAggregation);
        MultiBucketsAggregation expectedMultiBucketsAggregation = (MultiBucketsAggregation) expected;

        assertTrue(actual instanceof MultiBucketsAggregation);
        MultiBucketsAggregation actualMultiBucketsAggregation = (MultiBucketsAggregation) actual;

        assertMultiBucketsAggregation(expectedMultiBucketsAggregation, actualMultiBucketsAggregation, checkOrder);

        List<? extends MultiBucketsAggregation.Bucket> expectedBuckets = expectedMultiBucketsAggregation.getBuckets();
        List<? extends MultiBucketsAggregation.Bucket> actualBuckets = actualMultiBucketsAggregation.getBuckets();
        assertEquals(expectedBuckets.size(), actualBuckets.size());

        if (checkOrder) {
            Iterator<? extends MultiBucketsAggregation.Bucket> expectedIt = expectedBuckets.iterator();
            Iterator<? extends MultiBucketsAggregation.Bucket> actualIt = actualBuckets.iterator();
            while (expectedIt.hasNext()) {
                MultiBucketsAggregation.Bucket expectedBucket = expectedIt.next();
                MultiBucketsAggregation.Bucket actualBucket = actualIt.next();
                assertBucket(expectedBucket, actualBucket, true);
            }
        } else {
            for (MultiBucketsAggregation.Bucket expectedBucket : expectedBuckets) {
                final Object expectedKey = expectedBucket.getKey();
                boolean found = false;

                for (MultiBucketsAggregation.Bucket actualBucket : actualBuckets) {
                    final Object actualKey = actualBucket.getKey();
                    if ((actualKey != null && actualKey.equals(expectedKey)) || (actualKey == null && expectedKey == null)) {
                        found = true;
                        assertBucket(expectedBucket, actualBucket, false);
                        break;
                    }
                }
                assertTrue("Failed to find bucket with key [" + expectedBucket.getKey() + "]", found);
            }
        }
    }

    protected void assertMultiBucketsAggregation(MultiBucketsAggregation expected, MultiBucketsAggregation actual, boolean checkOrder) {
        Class<? extends ParsedMultiBucketAggregation> parsedClass = implementationClass();
        assertNotNull("Parsed aggregation class must not be null", parsedClass);
        assertTrue("Unexpected parsed class, expected instance of: " + actual + ", but was: " + parsedClass,
                parsedClass.isInstance(actual));

        assertTrue(expected instanceof InternalAggregation);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getMetadata(), actual.getMetadata());
        assertEquals(expected.getType(), actual.getType());
    }

    protected void assertBucket(MultiBucketsAggregation.Bucket expected, MultiBucketsAggregation.Bucket actual, boolean checkOrder) {
        assertTrue(expected instanceof InternalMultiBucketAggregation.InternalBucket);
        assertTrue(actual instanceof ParsedMultiBucketAggregation.ParsedBucket);

        assertEquals(expected.getKey(), actual.getKey());
        assertEquals(expected.getKeyAsString(), actual.getKeyAsString());
        assertEquals(expected.getDocCount(), actual.getDocCount());

        Aggregations expectedAggregations = expected.getAggregations();
        Aggregations actualAggregations = actual.getAggregations();
        assertEquals(expectedAggregations.asList().size(), actualAggregations.asList().size());

        if (checkOrder) {
            Iterator<Aggregation> expectedIt = expectedAggregations.iterator();
            Iterator<Aggregation> actualIt = actualAggregations.iterator();

            while (expectedIt.hasNext()) {
                Aggregation expectedAggregation = expectedIt.next();
                Aggregation actualAggregation = actualIt.next();
                assertMultiBucketsAggregations(expectedAggregation, actualAggregation, true);
            }
        } else {
            for (Aggregation expectedAggregation : expectedAggregations) {
                Aggregation actualAggregation = actualAggregations.get(expectedAggregation.getName());
                assertNotNull(actualAggregation);
                assertMultiBucketsAggregations(expectedAggregation, actualAggregation, false);
            }
        }
    }
}
