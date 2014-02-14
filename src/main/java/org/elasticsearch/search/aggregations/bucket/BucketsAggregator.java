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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public abstract class BucketsAggregator extends Aggregator {

    private LongArray docCounts;

    private final Aggregator[] collectableSugAggregators;

    public BucketsAggregator(String name, BucketAggregationMode bucketAggregationMode, AggregatorFactories factories,
                             long estimatedBucketsCount, AggregationContext context, Aggregator parent) {
        super(name, bucketAggregationMode, factories, estimatedBucketsCount, context, parent);
        docCounts = BigArrays.newLongArray(estimatedBucketsCount, context.pageCacheRecycler(), true);
        List<Aggregator> collectables = new ArrayList<Aggregator>(subAggregators.length);
        for (int i = 0; i < subAggregators.length; i++) {
            if (subAggregators[i].shouldCollect()) {
                collectables.add((subAggregators[i]));
            }
        }
        collectableSugAggregators = collectables.toArray(new Aggregator[collectables.size()]);
    }

    /**
     * Utility method to collect the given doc in the given bucket (identified by the bucket ordinal)
     */
    protected final void collectBucket(int doc, long bucketOrd) throws IOException {
        docCounts = BigArrays.grow(docCounts, bucketOrd + 1);
        docCounts.increment(bucketOrd, 1);
        for (int i = 0; i < collectableSugAggregators.length; i++) {
            collectableSugAggregators[i].collect(doc, bucketOrd);
        }
    }

    /**
     * Utility method to collect the given doc in the given bucket but not to update the doc counts of the bucket
     */
    protected final void collectBucketNoCounts(int doc, long bucketOrd) throws IOException {
        for (int i = 0; i < collectableSugAggregators.length; i++) {
            collectableSugAggregators[i].collect(doc, bucketOrd);
        }
    }

    /**
     * Utility method to increment the doc counts of the given bucket (identified by the bucket ordinal)
     */
    protected final void incrementBucketDocCount(int inc, long bucketOrd) throws IOException {
        docCounts = BigArrays.grow(docCounts, bucketOrd + 1);
        docCounts.increment(bucketOrd, inc);
    }

    /**
     * Utility method to return the number of documents that fell in the given bucket (identified by the bucket ordinal)
     */
    protected final long bucketDocCount(long bucketOrd) {
        if (bucketOrd >= docCounts.size()) {
            // This may happen eg. if no document in the highest buckets is accepted by a sub aggregator.
            // For example, if there is a long terms agg on 3 terms 1,2,3 with a sub filter aggregator and if no document with 3 as a value
            // matches the filter, then the filter will never collect bucket ord 3. However, the long terms agg will call bucketAggregations(3)
            // on the filter aggregator anyway to build sub-aggregations.
            return 0L;
        } else {
            return docCounts.get(bucketOrd);
        }
    }

    /**
     * Utility method to build the aggregations of the given bucket (identified by the bucket ordinal)
     */
    protected final InternalAggregations bucketAggregations(long bucketOrd) {
        final InternalAggregation[] aggregations = new InternalAggregation[subAggregators.length];
        final long bucketDocCount = bucketDocCount(bucketOrd);
        for (int i = 0; i < subAggregators.length; i++) {
            aggregations[i] = bucketDocCount == 0L
                    ? subAggregators[i].buildEmptyAggregation()
                    : subAggregators[i].buildAggregation(bucketOrd);
        }
        return new InternalAggregations(Arrays.asList(aggregations));
    }

    @Override
    public final boolean release() {
        boolean success = false;
        try {
            super.release();
            success = true;
        } finally {
            Releasables.release(success, docCounts);
        }
        return true;
    }

}
