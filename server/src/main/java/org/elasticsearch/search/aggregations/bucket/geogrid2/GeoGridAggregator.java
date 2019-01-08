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
package org.elasticsearch.search.aggregations.bucket.geogrid2;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregates data expressed as Geogrid hash longs (for efficiency's sake) but formats results as geo grid strings.
 */
public class GeoGridAggregator extends BucketsAggregator {

    private final int requiredSize;
    private final int shardSize;
    private final GeoGridAggregationBuilder.CellIdSource valuesSource;
    private final LongHash bucketOrds;
    private final GeoGridType type;

    GeoGridAggregator(String name, AggregatorFactories factories, GeoGridAggregationBuilder.CellIdSource valuesSource,
                      int requiredSize, int shardSize, SearchContext aggregationContext, Aggregator parent,
                      List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData, GeoGridType type
    ) throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.type = type;
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
        final SortedNumericDocValues values = valuesSource.longValues(ctx);
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

    // private impl that stores a bucket ord. This allows for computing the aggregations lazily.
    static class OrdinalBucket extends InternalGeoGrid.Bucket {

        long bucketOrd;

        OrdinalBucket() {
            super(0, 0, null);
        }

    }

    @Override
    public InternalGeoGrid buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        final int size = (int) Math.min(bucketOrds.size(), shardSize);
        consumeBucketsAndMaybeBreak(size);

        InternalGeoGrid.BucketPriorityQueue ordered = new InternalGeoGrid.BucketPriorityQueue(size);
        OrdinalBucket spare = null;
        for (long i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new OrdinalBucket();
            }

            spare.hashAsLong = bucketOrds.get(i);
            spare.docCount = bucketDocCount(i);
            spare.bucketOrd = i;
            spare = (OrdinalBucket) ordered.insertWithOverflow(spare);
        }

        final InternalGeoGrid.Bucket[] list = new InternalGeoGrid.Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final OrdinalBucket bucket = (OrdinalBucket) ordered.pop();
            bucket.aggregations = bucketAggregations(bucket.bucketOrd);
            list[i] = bucket;
        }
        return new InternalGeoGrid(name, type, requiredSize, Arrays.asList(list), pipelineAggregators(), metaData());
    }

    @Override
    public InternalGeoGrid buildEmptyAggregation() {
        return new InternalGeoGrid(name, type, requiredSize, Collections.emptyList(), pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

}
