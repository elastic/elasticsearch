/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public abstract class AbstractRareTermsAggregator extends DeferableBucketAggregator {

    static final BucketOrder ORDER = BucketOrder.compound(BucketOrder.count(true), BucketOrder.key(true)); // sort by count ascending

    protected final long maxDocCount;
    private final double precision;
    protected final DocValueFormat format;
    private final int filterSeed;

    AbstractRareTermsAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
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
        this.filterSeed = context.shardRandomSeed();
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
            throw new IllegalStateException(
                "RareTerms agg ["
                    + name()
                    + "] is the child of the nested agg ["
                    + nestedAgg
                    + "], and also has a scoring child agg ["
                    + scoringAgg
                    + "].  This combination is not supported because "
                    + "it requires executing in [depth_first] mode, which the RareTerms agg cannot do."
            );
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

    private String subAggsNeedScore() {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return subAgg.name();
            }
        }
        return null;
    }

    private static String descendsFromNestedAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == NestedAggregator.class) {
                return parent.name();
            }
            parent = parent.parent();
        }
        return null;
    }
}
