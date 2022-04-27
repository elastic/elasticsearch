/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.MultiBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public abstract class DeferableBucketAggregator extends BucketsAggregator {
    /**
     * Wrapper that records collections. Non-null if any aggregations have
     * been deferred.
     */
    private DeferringBucketCollector deferringCollector;
    private List<String> deferredAggregationNames;

    protected DeferableBucketAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        // Assumes that we're collecting MANY buckets.
        super(name, factories, context, parent, CardinalityUpperBound.MANY, metadata);
    }

    @Override
    protected void doPreCollection() throws IOException {
        List<BucketCollector> collectors = new ArrayList<>(subAggregators.length);
        List<BucketCollector> deferredAggregations = null;
        for (int i = 0; i < subAggregators.length; ++i) {
            if (shouldDefer(subAggregators[i])) {
                if (deferringCollector == null) {
                    deferringCollector = buildDeferringCollector();
                    deferredAggregations = new ArrayList<>(subAggregators.length);
                    deferredAggregationNames = new ArrayList<>(subAggregators.length);
                }
                deferredAggregations.add(subAggregators[i]);
                deferredAggregationNames.add(subAggregators[i].name());
                subAggregators[i] = deferringCollector.wrap(subAggregators[i]);
            } else {
                collectors.add(subAggregators[i]);
            }
        }
        if (deferringCollector != null) {
            deferringCollector.setDeferredCollector(deferredAggregations);
            collectors.add(deferringCollector);
        }
        collectableSubAggregators = MultiBucketCollector.wrap(false, collectors);
    }

    /**
     * Get the deferring collector.
     */
    protected DeferringBucketCollector deferringCollector() {
        return deferringCollector;
    }

    /**
     * Build the {@link DeferringBucketCollector}. The default implementation
     * replays all hits against the buckets selected by
     * {#link {@link DeferringBucketCollector#prepareSelectedBuckets(long...)}.
     */
    protected DeferringBucketCollector buildDeferringCollector() {
        return new BestBucketsDeferringCollector(topLevelQuery(), searcher(), descendsFromGlobalAggregator(parent()));
    }

    /**
     * This method should be overridden by subclasses that want to defer
     * calculation of a child aggregation until a first pass is complete and a
     * set of buckets has been pruned.
     *
     * @param aggregator the child aggregator
     * @return true if the aggregator should be deferred until a first pass at
     *         collection has completed
     */
    protected boolean shouldDefer(Aggregator aggregator) {
        return false;
    }

    @Override
    protected final void prepareSubAggs(long[] bucketOrdsToCollect) throws IOException {
        if (deferringCollector != null) {
            deferringCollector.prepareSelectedBuckets(bucketOrdsToCollect);
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        if (deferredAggregationNames != null) {
            add.accept("deferred_aggregators", deferredAggregationNames);
        }
        super.collectDebugInfo(add);
    }
}
