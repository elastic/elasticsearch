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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public abstract class GeoGridTestCase<B extends InternalGeoGridBucket, T extends InternalGeoGrid<B>>
        extends InternalMultiBucketAggregationTestCase<T> {

    /**
     * Instantiate a {@link InternalGeoGrid}-derived class using the same parameters as constructor.
     */
    protected abstract T createInternalGeoGrid(String name, int size, List<InternalGeoGridBucket> buckets,
                                               Map<String, Object> metadata);

    /**
     * Instantiate a {@link InternalGeoGridBucket}-derived class using the same parameters as constructor.
     */
    protected abstract B createInternalGeoGridBucket(Long key, long docCount, InternalAggregations aggregations);

    /**
     * Encode longitude and latitude with a given precision as a long hash.
     */
    protected abstract long longEncode(double lng, double lat, int precision);

    /**
     * Generate a random precision according to the rules of the given aggregation.
     */
    protected abstract int randomPrecision();

    @Override
    protected int minNumberOfBuckets() {
        return 1;
    }

    @Override
    protected int maxNumberOfBuckets() {
        return 3;
    }

    @Override
    protected T createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations) {
        final int precision = randomPrecision();
        int size = randomNumberOfBuckets();
        List<InternalGeoGridBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            double latitude = randomDoubleBetween(-90.0, 90.0, false);
            double longitude = randomDoubleBetween(-180.0, 180.0, false);

            long hashAsLong = longEncode(longitude, latitude, precision);
            buckets.add(createInternalGeoGridBucket(hashAsLong, randomInt(IndexWriter.MAX_DOCS), aggregations));
        }
        return createInternalGeoGrid(name, size, buckets, metadata);
    }

    @Override
    protected void assertReduced(T reduced, List<T> inputs) {
        Map<Long, List<B>> map = new HashMap<>();
        for (T input : inputs) {
            for (GeoGrid.Bucket bucketBase : input.getBuckets()) {
                B bucket = (B) bucketBase;
                List<B> buckets = map.get(bucket.hashAsLong);
                if (buckets == null) {
                    map.put(bucket.hashAsLong, buckets = new ArrayList<>());
                }
                buckets.add(bucket);
            }
        }
        List<B> expectedBuckets = new ArrayList<>();
        for (Map.Entry<Long, List<B>> entry : map.entrySet()) {
            long docCount = 0;
            for (B bucket : entry.getValue()) {
                docCount += bucket.docCount;
            }
            expectedBuckets.add(createInternalGeoGridBucket(entry.getKey(), docCount, InternalAggregations.EMPTY));
        }
        expectedBuckets.sort((first, second) -> {
            int cmp = Long.compare(second.docCount, first.docCount);
            if (cmp == 0) {
                return second.compareTo(first);
            }
            return cmp;
        });
        int requestedSize = inputs.get(0).getRequiredSize();
        expectedBuckets = expectedBuckets.subList(0, Math.min(requestedSize, expectedBuckets.size()));
        assertEquals(expectedBuckets.size(), reduced.getBuckets().size());
        for (int i = 0; i < reduced.getBuckets().size(); i++) {
            GeoGrid.Bucket expected = expectedBuckets.get(i);
            GeoGrid.Bucket actual = reduced.getBuckets().get(i);
            assertEquals(expected.getDocCount(), actual.getDocCount());
            assertEquals(expected.getKey(), actual.getKey());
        }
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedGeoGrid.class;
    }

    @Override
    protected T mutateInstance(T instance) {
        String name = instance.getName();
        int size = instance.getRequiredSize();
        List<InternalGeoGridBucket> buckets = instance.getBuckets();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            buckets = new ArrayList<>(buckets);
            buckets.add(
                    createInternalGeoGridBucket(randomNonNegativeLong(), randomInt(IndexWriter.MAX_DOCS), InternalAggregations.EMPTY));
            break;
        case 2:
            size = size + between(1, 10);
            break;
        case 3:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return createInternalGeoGrid(name, size, buckets, metadata);
    }

    public void testCreateFromBuckets() {
       InternalGeoGrid original = createTestInstance();
       assertThat(original, equalTo(original.create(original.buckets)));
    }
}
