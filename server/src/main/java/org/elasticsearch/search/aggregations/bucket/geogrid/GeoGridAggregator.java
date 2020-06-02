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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregates data expressed as longs (for efficiency's sake) but formats results as aggregation-specific strings.
 */
public abstract class GeoGridAggregator<T extends InternalGeoGrid> extends BucketsAggregator {

    protected final int requiredSize;
    protected final int shardSize;
    protected final ValuesSource.Numeric valuesSource;
    protected final LongHash bucketOrds;
    protected SortedNumericDocValues values;

    GeoGridAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource,
                      int requiredSize, int shardSize, SearchContext aggregationContext,
                      Aggregator parent, Map<String, Object> metadata) throws IOException {
        super(name, factories, aggregationContext, parent, metadata);
        this.valuesSource = valuesSource;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        final long val = values.nextValue();
                        if (previous != val || i == 0) {
                            long bucketOrdinal = bucketOrds.add(val);
                            if (bucketOrdinal < 0) { // already seen
                                bucketOrdinal = -1 - bucketOrdinal;
                                collectExistingBucket(sub, doc, bucketOrdinal);
                            } else {
                                collectBucket(sub, doc, bucketOrdinal);
                            }
                            previous = val;
                        }
                    }
                }
            }
        };
    }

    abstract T buildAggregation(String name, int requiredSize, List<InternalGeoGridBucket> buckets, Map<String, Object> metadata);

    /**
     * This method is used to return a re-usable instance of the bucket when building
     * the aggregation.
     * @return a new {@link InternalGeoGridBucket} implementation with empty parameters
     */
    abstract InternalGeoGridBucket newEmptyBucket();

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        assert owningBucketOrds.length == 1 && owningBucketOrds[0] == 0;
        final int size = (int) Math.min(bucketOrds.size(), shardSize);
        consumeBucketsAndMaybeBreak(size);

        BucketPriorityQueue<InternalGeoGridBucket> ordered = new BucketPriorityQueue<>(size);
        InternalGeoGridBucket spare = null;
        for (long i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = newEmptyBucket();
            }

            // need a special function to keep the source bucket
            // up-to-date so it can get the appropriate key
            spare.hashAsLong = bucketOrds.get(i);
            spare.docCount = bucketDocCount(i);
            spare.bucketOrd = i;
            spare = ordered.insertWithOverflow(spare);
        }

        final InternalGeoGridBucket[] list = new InternalGeoGridBucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            list[i] = ordered.pop();
        }
        buildSubAggsForBuckets(list, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        return new InternalAggregation[] {buildAggregation(name, requiredSize, Arrays.asList(list), metadata())};
    }

    @Override
    public InternalGeoGrid buildEmptyAggregation() {
        return buildAggregation(name, requiredSize, Collections.emptyList(), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }
}
