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

    protected DeferableBucketAggregator(String name, AggregatorFactories factories, AggregationContext context, Aggregator parent,
            Map<String, Object> metadata) throws IOException {
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
        collectableSubAggregators = MultiBucketCollector.wrap(collectors);
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
