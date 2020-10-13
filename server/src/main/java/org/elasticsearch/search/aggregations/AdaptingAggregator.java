package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AdaptingAggregator extends Aggregator {
    private final Aggregator delegate;

    public AdaptingAggregator(Aggregator delegate) {
        this.delegate = delegate;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public ScoreMode scoreMode() {
        return delegate.scoreMode();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public SearchContext context() {
        return delegate.context();
    }

    @Override
    public Aggregator parent() {
        return delegate.parent();
    }

    @Override
    public Aggregator subAggregator(String name) {
        return delegate.subAggregator(name);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        return delegate.getLeafCollector(ctx);
    }

    @Override
    public void preCollection() throws IOException {
        delegate.preCollection();
    }

    @Override
    public void postCollection() throws IOException {
        delegate.postCollection();
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] delegateResults = delegate.buildAggregations(owningBucketOrds);
        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = adapt(delegateResults[ordIdx]);
        }
        return result;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return adapt(delegate.buildEmptyAggregation());
    }

    @Override
    public Aggregator[] subAggregators() {
        return delegate.subAggregators();
    }

    protected abstract InternalAggregation adapt(InternalAggregation delegateResult);
}
