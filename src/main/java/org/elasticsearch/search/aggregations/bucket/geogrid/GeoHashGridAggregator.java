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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Aggregates data expressed as GeoHash longs (for efficiency's sake) but formats results as Geohash strings.
 *
 */

public class GeoHashGridAggregator extends BucketsAggregator {

    private static final int INITIAL_CAPACITY = 50; // TODO sizing

    private final int requiredSize;
    private final int shardSize;
    private final ValuesSource.Numeric valuesSource;
    private final LongHash bucketOrds;
    private LongValues values;

    public GeoHashGridAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource,
                              int requiredSize, int shardSize, AggregationContext aggregationContext, Aggregator parent) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, INITIAL_CAPACITY, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        bucketOrds = new LongHash(INITIAL_CAPACITY, aggregationContext.bigArrays());
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.longValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final int valuesCount = values.setDocument(doc);

        for (int i = 0; i < valuesCount; ++i) {
            final long val = values.nextValue();
            long bucketOrdinal = bucketOrds.add(val);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
                collectExistingBucket(doc, bucketOrdinal);
            } else {
                collectBucket(doc, bucketOrdinal);
            }
        }
    }

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends InternalGeoHashGrid.Bucket {

        long bucketOrd;

        public OrdinalBucket() {
            super(0, 0, (InternalAggregations) null);
        }

    }

    @Override
    public InternalGeoHashGrid buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;
        final int size = (int) Math.min(bucketOrds.size(), shardSize);

        InternalGeoHashGrid.BucketPriorityQueue ordered = new InternalGeoHashGrid.BucketPriorityQueue(size);
        OrdinalBucket spare = null;
        for (long i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new OrdinalBucket();
            }

            spare.geohashAsLong = bucketOrds.get(i);
            spare.docCount = bucketDocCount(i);
            spare.bucketOrd = i;
            spare = (OrdinalBucket) ordered.insertWithOverflow(spare);
        }

        final InternalGeoHashGrid.Bucket[] list = new InternalGeoHashGrid.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final OrdinalBucket bucket = (OrdinalBucket) ordered.pop();
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }
        return new InternalGeoHashGrid(name, requiredSize, Arrays.asList(list));
    }

    @Override
    public InternalGeoHashGrid buildEmptyAggregation() {
        return new InternalGeoHashGrid(name, requiredSize, Collections.<InternalGeoHashGrid.Bucket>emptyList());
    }


    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

}
