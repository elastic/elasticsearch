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
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.MergingBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public abstract class AbstractRareTermsAggregator extends DeferableBucketAggregator {

    static final BucketOrder ORDER = BucketOrder.compound(BucketOrder.count(true), BucketOrder.key(true)); // sort by count ascending

    protected final long maxDocCount;
    private final double precision;
    protected final DocValueFormat format;
    private final int filterSeed;

    protected MergingBucketsDeferringCollector deferringCollector;

    AbstractRareTermsAggregator(
        String name,
        AggregatorFactories factories,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        long maxDocCount,
        double precision,
        DocValueFormat format
    ) throws IOException {
        super(name, factories, context, parent, metadata);

        this.maxDocCount = maxDocCount;
        this.precision = precision;
        this.format = format;
        // We seed the rng with the ShardID so results are deterministic and don't change randomly
        this.filterSeed = context.indexShard().shardId().hashCode();
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

    protected SetBackedScalingCuckooFilter newFilter() {
        SetBackedScalingCuckooFilter filter = new SetBackedScalingCuckooFilter(10000, new Random(filterSeed), precision);
        filter.registerBreaker(this::addRequestCircuitBreakerBytes);
        return filter;
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        deferringCollector = new MergingBucketsDeferringCollector(context, descendsFromGlobalAggregator(parent()));
        return deferringCollector;
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
}
