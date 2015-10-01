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
import org.apache.lucene.util.XGeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregates data expressed as GeoHash longs (for efficiency's sake) but formats results as Geohash strings.
 *
 */

public class GeoHashGridAggregator extends BucketsAggregator {

    private final int requiredSize;
    private final int shardSize;
    private final GeoHashGridParser.GeoGridFactory.CellIdSource valuesSource;
    private final LongHash bucketOrds;
    private LongArray bucketCentroids;

    public GeoHashGridAggregator(String name, AggregatorFactories factories, GeoHashGridParser.GeoGridFactory.CellIdSource valuesSource,
            int requiredSize, int shardSize, AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        bucketOrds = new LongHash(1, aggregationContext.bigArrays());
        bucketCentroids = aggregationContext.bigArrays().newLongArray(1, true);
    }

    @Override
    public boolean needsScores() {
        return (valuesSource != null && valuesSource.needsScores()) || super.needsScores();
    }

    @Override
    public void collectBucket(LeafBucketCollector subCollector, int doc, long bucketOrd) throws IOException {
        bucketCentroids = bigArrays.grow(bucketCentroids, bucketOrd + 1);
        super.collectBucket(subCollector, doc, bucketOrd);
    }

    protected final void adjustCentroid(long bucketOrd, long geohash) {
        final int numDocs = getDocCounts().get(bucketOrd);
        final GeoPoint oldCentroid = new GeoPoint();
        final GeoPoint nextLoc = new GeoPoint();

        if (numDocs > 1) {
            final long curCentroid = bucketCentroids.get(bucketOrd);
            oldCentroid.resetFromGeoHash(curCentroid);
            nextLoc.resetFromGeoHash(geohash);
            bucketCentroids.set(bucketOrd, XGeoHashUtils.longEncode(oldCentroid.lon() + (nextLoc.lon() - oldCentroid.lon()) / numDocs,
                    oldCentroid.lat() + (nextLoc.lat() - oldCentroid.lat()) / numDocs, XGeoHashUtils.PRECISION));
        } else {
            bucketCentroids.set(bucketOrd, geohash);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                values.setDocument(doc);
                final int valuesCount = values.count();

                long previous = Long.MAX_VALUE;
                for (int i = 0; i < valuesCount; ++i) {
                    final long valFullRes = values.valueAt(i);
                    final long val = XGeoHashUtils.longEncode(valFullRes, valuesSource.precision());
                    if (previous != val || i == 0) {
                        long bucketOrdinal = bucketOrds.add(val);
                        if (bucketOrdinal < 0) { // already seen
                            bucketOrdinal = - 1 - bucketOrdinal;
                            collectExistingBucket(sub, doc, bucketOrdinal);
                        } else {
                            collectBucket(sub, doc, bucketOrdinal);
                        }
                        adjustCentroid(bucketOrdinal, valFullRes);
                        previous = val;
                    }
                }
            }
        };
    }

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends InternalGeoHashGrid.Bucket {

        long bucketOrd;

        public OrdinalBucket() {
            super(0, 0, new GeoPoint(), (InternalAggregations) null);
        }

    }

    @Override
    public InternalGeoHashGrid buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final int size = (int) Math.min(bucketOrds.size(), shardSize);

        InternalGeoHashGrid.BucketPriorityQueue ordered = new InternalGeoHashGrid.BucketPriorityQueue(size);
        OrdinalBucket spare = null;
        for (long i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new OrdinalBucket();
            }

            spare.geohashAsLong = bucketOrds.get(i);
            spare.centroid.resetFromGeoHash(bucketCentroids.get(i));
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
        return new InternalGeoHashGrid(name, requiredSize, Arrays.asList(list), pipelineAggregators(), metaData());
    }

    @Override
    public InternalGeoHashGrid buildEmptyAggregation() {
        return new InternalGeoHashGrid(name, requiredSize, Collections.<InternalGeoHashGrid.Bucket> emptyList(), pipelineAggregators(), metaData());
    }


    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
        Releasables.close(bucketCentroids);
    }

}
