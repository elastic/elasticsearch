/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.search.profile.aggregation.InternalAggregationProfileTree;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An {@linkplain Aggregator} that delegates collection to another
 * {@linkplain Aggregator} and then translates its results into the results
 * you'd expect from another aggregation.
 */
public abstract class AdaptingAggregator extends Aggregator {
    private final Aggregator parent;
    private final Aggregator delegate;

    public AdaptingAggregator(
        Aggregator parent,
        AggregatorFactories subAggregators,
        CheckedFunction<AggregatorFactories, Aggregator, IOException> delegate
    ) throws IOException {
        // Its important we set parent first or else when we build the sub-aggregators they can fail because they'll call this.parent.
        this.parent = parent;
        /*
         * Lock the parent of the sub-aggregators to *this* instead of to
         * the delegate. This keeps the parent link shaped like the requested
         * agg tree. Thisis how it has always been and some aggs rely on it.
         */
        this.delegate = delegate.apply(subAggregators.fixParent(this));
        assert this.delegate.parent() == parent : "invalid parent set on delegate";
    }

    /**
     * Adapt the result from the collecting {@linkplain Aggregator} into the
     * result expected by this {@linkplain Aggregator}.
     */
    protected abstract InternalAggregation adapt(InternalAggregation delegateResult);

    @Override
    public final void close() {
        delegate.close();
    }

    @Override
    public final ScoreMode scoreMode() {
        return delegate.scoreMode();
    }

    @Override
    public final String name() {
        return delegate.name();
    }

    @Override
    public final Aggregator parent() {
        return parent;
    }

    @Override
    public final Aggregator subAggregator(String name) {
        return delegate.subAggregator(name);
    }

    @Override
    public final LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        return delegate.getLeafCollector(ctx);
    }

    @Override
    public final void preCollection() throws IOException {
        delegate.preCollection();
    }

    @Override
    public final void postCollection() throws IOException {
        delegate.postCollection();
    }

    @Override
    public final InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] delegateResults = delegate.buildAggregations(owningBucketOrds);
        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = adapt(delegateResults[ordIdx]);
        }
        return result;
    }

    @Override
    public final InternalAggregation buildEmptyAggregation() {
        return adapt(delegate.buildEmptyAggregation());
    }

    @Override
    public final Aggregator[] subAggregators() {
        return delegate.subAggregators();
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("delegate", InternalAggregationProfileTree.typeFromAggregator(delegate));
        Map<String, Object> delegateDebug = new HashMap<>();
        delegate.collectDebugInfo(delegateDebug::put);
        add.accept("delegate_debug", delegateDebug);
    }

    public Aggregator delegate() {
        return delegate;
    }
}
