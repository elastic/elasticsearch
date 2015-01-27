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

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public abstract class BucketsAggregator extends AggregatorBase {

    private final BigArrays bigArrays;
    private IntArray docCounts;

    public BucketsAggregator(String name, AggregatorFactories factories,
                             AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, metaData);
        bigArrays = context.bigArrays();
        docCounts = bigArrays.newIntArray(1, true);
    }

    /**
     * Return an upper bound of the maximum bucket ordinal seen so far.
     */
    public final long maxBucketOrd() {
        return docCounts.size();
    }

    /**
     * Ensure there are at least <code>maxBucketOrd</code> buckets available.
     */
    public final void grow(long maxBucketOrd) {
        docCounts = bigArrays.grow(docCounts, maxBucketOrd);
    }

    /**
     * Utility method to collect the given doc in the given bucket (identified by the bucket ordinal)
     */
    public final void collectBucket(LeafBucketCollector subCollector, int doc, long bucketOrd) throws IOException {
        grow(bucketOrd + 1);
        collectExistingBucket(subCollector, doc, bucketOrd);
    }

    /**
     * Same as {@link #collectBucket(int, long)}, but doesn't check if the docCounts needs to be re-sized.
     */
    public final void collectExistingBucket(LeafBucketCollector subCollector, int doc, long bucketOrd) throws IOException {
        docCounts.increment(bucketOrd, 1);
        subCollector.collect(doc, bucketOrd);
    }

    public IntArray getDocCounts() {
        return docCounts;
    }

    /**
     * Utility method to increment the doc counts of the given bucket (identified by the bucket ordinal)
     */
    public final void incrementBucketDocCount(long bucketOrd, int inc) {
        docCounts = bigArrays.grow(docCounts, bucketOrd + 1);
        docCounts.increment(bucketOrd, inc);
    }

    /**
     * Utility method to return the number of documents that fell in the given bucket (identified by the bucket ordinal)
     */
    public final int bucketDocCount(long bucketOrd) {
        if (bucketOrd >= docCounts.size()) {
            // This may happen eg. if no document in the highest buckets is accepted by a sub aggregator.
            // For example, if there is a long terms agg on 3 terms 1,2,3 with a sub filter aggregator and if no document with 3 as a value
            // matches the filter, then the filter will never collect bucket ord 3. However, the long terms agg will call bucketAggregations(3)
            // on the filter aggregator anyway to build sub-aggregations.
            return 0;
        } else {
            return docCounts.get(bucketOrd);
        }
    }

    /**
     * Required method to build the child aggregations of the given bucket (identified by the bucket ordinal).
     */
    protected final InternalAggregations bucketAggregations(long bucket) throws IOException {
        final InternalAggregation[] aggregations = new InternalAggregation[subAggregators.length];
        for (int i = 0; i < subAggregators.length; i++) {
            aggregations[i] = subAggregators[i].buildAggregation(bucket);
        }
        return new InternalAggregations(Arrays.asList(aggregations));
    }

    /**
     * Utility method to build empty aggregations of the sub aggregators.
     */
    protected final InternalAggregations bucketEmptyAggregations() {
        final InternalAggregation[] aggregations = new InternalAggregation[subAggregators.length];
        for (int i = 0; i < subAggregators.length; i++) {
            aggregations[i] = subAggregators[i].buildEmptyAggregation();
        }
        return new InternalAggregations(Arrays.asList(aggregations));
    }

    @Override
    public final void close() {
        try (Releasable releasable = docCounts) {
            super.close();
        }
    }

}
