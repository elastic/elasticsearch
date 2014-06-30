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
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 */
public abstract class BucketsAggregator extends Aggregator {

    private IntArray docCounts;

    public BucketsAggregator(String name, BucketAggregationMode bucketAggregationMode, AggregatorFactories factories,
                             long estimatedBucketsCount, AggregationContext context, Aggregator parent) {
        super(name, bucketAggregationMode, factories, estimatedBucketsCount, context, parent);
        docCounts = bigArrays.newIntArray(estimatedBucketsCount, true);
    }

    /**
     * Return an upper bound of the maximum bucket ordinal seen so far.
     */
    protected final long maxBucketOrd() {
        return docCounts.size();
    }

    /**
     * Utility method to collect the given doc in the given bucket (identified by the bucket ordinal)
     */
    protected final void collectBucket(int doc, long bucketOrd) throws IOException {
        docCounts = bigArrays.grow(docCounts, bucketOrd + 1);
        collectExistingBucket(doc, bucketOrd);
    }

    /**
     * Same as {@link #collectBucket(int, long)}, but doesn't check if the docCounts needs to be re-sized.
     */
    protected final void collectExistingBucket(int doc, long bucketOrd) throws IOException {
        docCounts.increment(bucketOrd, 1);
        collectBucketNoCounts(doc, bucketOrd);
    }

    public IntArray getDocCounts() {
        return docCounts;
    }

    /**
     * Utility method to collect the given doc in the given bucket but not to update the doc counts of the bucket
     */
    protected final void collectBucketNoCounts(int doc, long bucketOrd) throws IOException {
        collectableSubAggregators.collect(doc, bucketOrd);
    }

    /**
     * Utility method to increment the doc counts of the given bucket (identified by the bucket ordinal)
     */
    protected final void incrementBucketDocCount(int inc, long bucketOrd) throws IOException {
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
    protected final InternalAggregations bucketAggregations(long bucketOrd) {
        final ArrayList<InternalAggregation> childAggs = new ArrayList<>();
        final long bucketDocCount = bucketDocCount(bucketOrd);
        if (bucketDocCount == 0L) {
            // All child aggs marked as empty
            for (int i = 0; i < subAggregators.length; i++) {
                childAggs.add(subAggregators[i].buildEmptyAggregation());
            }
        } else {
            BucketAnalysisCollector analysisCollector = new BucketAnalysisCollector() {               
                @Override
                public void add(Aggregation analysis) {
                    childAggs.add((InternalAggregation) analysis);
                }
            };
            // Add the collectable sub aggs by asking the collect tree to gather
            // results using ordinals that may have undergone transformation as the 
            // result of the collection process e.g. filtering
            // to a subset of buckets then rebasing the numbers in the deferred collection
            collectableSubAggregators.gatherAnalysis(analysisCollector, bucketOrd);

            // Also add the results of any non-collecting sub aggs using the top-level ordinals
            for (int i = 0; i < subAggregators.length; i++) {
                if (!subAggregators[i].shouldCollect()) {
                    // Agg is not part of the collect tree - call directly
                    childAggs.add(subAggregators[i].buildAggregation(bucketOrd));
                }
            }
        }

        return new InternalAggregations(childAggs);
        
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
