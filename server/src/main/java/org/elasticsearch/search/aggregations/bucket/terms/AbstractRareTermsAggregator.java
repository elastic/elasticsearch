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

import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.MergingBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

public abstract class AbstractRareTermsAggregator<T extends ValuesSource, U extends IncludeExclude.Filter, V>
    extends DeferableBucketAggregator {

    /**
     Sets the number of "removed" values to accumulate before we purge ords
     via the MergingBucketCollector's mergeBuckets() method
     */
    private static final long GC_THRESHOLD = 1000000;
    static final BucketOrder ORDER = BucketOrder.compound(BucketOrder.count(true), BucketOrder.key(true)); // sort by count ascending

    protected final long maxDocCount;
    protected final double precision;
    protected final DocValueFormat format;
    protected final T valuesSource;
    protected final U includeExclude;

    // Counter used during collection to track map entries that need GC'ing
    private long numDeleted = 0;

    MergingBucketsDeferringCollector deferringCollector;
    LeafBucketCollector subCollectors;
    final SetBackedScalingCuckooFilter filter;

    AbstractRareTermsAggregator(String name, AggregatorFactories factories, SearchContext context,
                                Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                Map<String, Object> metaData, long maxDocCount, double precision,
                                DocValueFormat format, T valuesSource, U includeExclude) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);

        // We seed the rng with the ShardID so results are deterministic and don't change randomly
        this.filter = new SetBackedScalingCuckooFilter(10000, new Random(context.indexShard().shardId().hashCode()), precision);
        this.filter.registerBreaker(this::addRequestCircuitBreakerBytes);

        this.maxDocCount = maxDocCount;
        this.precision = precision;
        this.format = format;
        this.valuesSource = valuesSource;
        this.includeExclude = includeExclude;
        String scoringAgg = subAggsNeedScore();
        String nestedAgg = descendsFromNestedAggregator(parent);
        if (scoringAgg != null && nestedAgg != null) {
            /*
             * Terms agg would force the collect mode to depth_first here, because
             * we need to access the score of nested documents in a sub-aggregation
             * and we are not able to generate this score while replaying deferred documents.
             *
             * But the RareTerms agg _must_ execute in breadth first since it relies on
             * deferring execution, so we just have to throw up our hands and refuse
             */
            throw new IllegalStateException("RareTerms agg [" + name() + "] is the child of the nested agg [" + nestedAgg
                + "], and also has a scoring child agg [" + scoringAgg + "].  This combination is not supported because " +
                "it requires executing in [depth_first] mode, which the RareTerms agg cannot do.");
        }
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        deferringCollector = new MergingBucketsDeferringCollector(context);
        return deferringCollector;
    }

    @Override
    protected void doPostCollection() {
        // Make sure we do one final GC to clean up any deleted ords
        // that may be lingering (but still below GC threshold)
        gcDeletedEntries(-1);
    }

    private String subAggsNeedScore() {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return subAgg.name();
            }
        }
        return null;
    }

    private String descendsFromNestedAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == NestedAggregator.class) {
                return parent.name();
            }
            parent = parent.parent();
        }
        return null;
    }

    protected void doCollect(V val, int docId) throws IOException {
        if (filterMightContain(val) == false) {
            long bucketOrdinal = findOrdinal(val);

            if (bucketOrdinal == -1) {
                // Brand new term, save into map
                long ord = addValueToOrds(val);
                assert ord >= 0;
                collectBucket(subCollectors, docId, ord);

            } else {
                // we've seen this value before, see if it is below threshold
                long termCount = bucketDocCount(bucketOrdinal);
                if (termCount < maxDocCount) {
                    // TODO if we only need maxDocCount==1, we could specialize
                    // and use a bitset instead of a counter scheme

                    collectExistingBucket(subCollectors, docId, bucketOrdinal);

                } else {
                    // Otherwise we've breached the threshold, add to the cuckoo filter
                    addValueToFilter(val);
                    numDeleted += 1;

                    // This is a bit hacky, but we need to collect the value once more to
                    // make sure the doc_count is over threshold (used later when gc'ing)
                    collectExistingBucket(subCollectors, docId, bucketOrdinal);

                    if (numDeleted > GC_THRESHOLD) {
                        gcDeletedEntries(numDeleted);
                        numDeleted = 0;
                    }
                }
            }
        }
    }

    /**
     * Remove entries from the ordinal map which are no longer tracked in the active key's map.
     * Will internally call the merge function of {@link MergingBucketsDeferringCollector}, so this
     * should be called sparingly for performance reasons
     *
     * @param numDeleted the number of keys that are expected to be pruned during GC.
     *                   Used to help verify correct functioning of GC
     */
    abstract void gcDeletedEntries(long numDeleted);

    /**
     * Returns true if the aggregator's approximate filter contains the value, false otherwise
     */
    abstract boolean filterMightContain(V value);

    /**
     * Returns the bucket ordinal associated with the value, -1 if the value was not found
     */
    abstract long findOrdinal(V value);

    /**
     * Add's the value to the ordinal map.  Return the newly allocated id if it wasn't in the ordinal map yet,
     * or <code>-1-id</code> if it was already present
     */
    abstract long addValueToOrds(V value);

    /**
     * Adds the value to the aggregator's approximate filter.
     */
    abstract void addValueToFilter(V value);
}
