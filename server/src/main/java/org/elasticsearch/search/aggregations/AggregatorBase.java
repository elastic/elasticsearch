/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.metrics.MinAggregator;
import org.elasticsearch.search.aggregations.metrics.SumAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Base implementation for concrete aggregators.
 */
public abstract class AggregatorBase extends Aggregator {

    /** The default "weight" that a bucket takes when performing an aggregation */
    public static final int DEFAULT_WEIGHT = 1024 * 5; // 5kb

    protected final String name;
    protected final Aggregator parent;
    private final AggregationContext context;
    private final Map<String, Object> metadata;

    protected final Aggregator[] subAggregators;
    protected BucketCollector collectableSubAggregators;

    private Map<String, Aggregator> subAggregatorbyName;
    private long requestBytesUsed;

    /**
     * Constructs a new Aggregator.
     *
     * @param name                  The name of the aggregation
     * @param factories             The factories for all the sub-aggregators under this aggregator
     * @param context               The aggregation context
     * @param parent                The parent aggregator (may be {@code null} for top level aggregators)
     * @param subAggregatorCardinality Upper bound of the number of buckets that sub aggregations will collect
     * @param metadata              The metadata associated with this aggregator
     */
    protected AggregatorBase(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound subAggregatorCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        this.name = name;
        this.metadata = metadata;
        this.parent = parent;
        this.context = context;
        assert factories != null : "sub-factories provided to BucketAggregator must not be null, use AggragatorFactories.EMPTY instead";
        this.subAggregators = factories.createSubAggregators(this, subAggregatorCardinality);
        context.addReleasable(this);
        // Register a safeguard to highlight any invalid construction logic (call to this constructor without subsequent preCollection call)
        collectableSubAggregators = new BucketCollector() {
            static void badState() {
                throw new IllegalStateException("preCollection not called on new Aggregator before use");
            }

            @Override
            public LeafBucketCollector getLeafCollector(LeafReaderContext reader) {
                badState();
                assert false;
                return null; // unreachable but compiler does not agree
            }

            @Override
            public void preCollection() throws IOException {
                badState();
            }

            @Override
            public void postCollection() throws IOException {
                badState();
            }

            @Override
            public ScoreMode scoreMode() {
                badState();
                return ScoreMode.COMPLETE; // unreachable
            }
        };
        addRequestCircuitBreakerBytes(DEFAULT_WEIGHT);
    }

    /**
     * Returns a converter for point values if it's safe to use the indexed data instead of
     * doc values.  Generally, this means that the query has no filters or scripts, the aggregation is
     * top level, and the underlying field is indexed, and the index is sorted in the right order.
     *
     * If those conditions aren't met, return <code>null</code> to indicate a point reader cannot
     * be used in this case.
     *
     * @param config The config for the values source metric.
     */
    public final Function<byte[], Number> pointReaderIfAvailable(ValuesSourceConfig config) {
        if (topLevelQuery() != null && topLevelQuery().getClass() != MatchAllDocsQuery.class) {
            return null;
        }
        if (parent != null) {
            return null;
        }
        return config.getPointReaderOrNull();
    }

    /**
     * Increment or decrement the number of bytes that have been allocated to service
     * this request and potentially trigger a {@link CircuitBreakingException}. The
     * number of bytes allocated is automatically decremented with the circuit breaker
     * service on closure of this aggregator.
     * If memory has been returned, decrement it without tripping the breaker.
     * For performance reasons subclasses should not call this millions of times
     * each with small increments and instead batch up into larger allocations.
     *
     * @param bytes the number of bytes to register or negative to deregister the bytes
     * @return the cumulative size in bytes allocated by this aggregator to service this request
     */
    protected long addRequestCircuitBreakerBytes(long bytes) {
        // Only use the potential to circuit break if bytes are being incremented
        if (bytes > 0) {
            context.breaker().addEstimateBytesAndMaybeBreak(bytes, "<agg [" + name + "]>");
        } else {
            context.breaker().addWithoutBreaking(bytes);
        }
        this.requestBytesUsed += bytes;
        return requestBytesUsed;
    }

    /**
     * Most aggregators don't need scores, make sure to extend this method if
     * your aggregator needs them.
     */
    @Override
    public ScoreMode scoreMode() {
        for (Aggregator agg : subAggregators) {
            if (agg.scoreMode().needsScores()) {
                return ScoreMode.COMPLETE;
            }
        }
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    public Map<String, Object> metadata() {
        return this.metadata;
    }

    /**
     * Collect results for this leaf.
     * <p>
     * Most {@linkplain Aggregator}s will return a custom
     * {@link LeafBucketCollector} that collects document information for
     * every hit. Callers of this method will make sure to call
     * {@link LeafBucketCollector#collect(int, long) collect} for every hit. So any
     * {@link Aggregator} that returns a customer {@linkplain LeafBucketCollector}
     * from this method runs at best {@code O(hits)} time. See the
     * {@link SumAggregator#getLeafCollector(LeafReaderContext, LeafBucketCollector) sum}
     * {@linkplain Aggregator} for a fairly strait forward example of this.
     * <p>
     * Some {@linkplain Aggregator}s are able to correctly collect results on
     * their own, without being iterated by the top level query or the rest
     * of the aggregations framework. These aggregations collect what they
     * need by calling methods on {@link LeafReaderContext} and then they
     * return {@link LeafBucketCollector#NO_OP_COLLECTOR} to signal that they've
     * done their own collection. These aggregations can do better than
     * {@code O(hits)}. See the
     * {@link MinAggregator#getLeafCollector(LeafReaderContext, LeafBucketCollector) min}
     * {@linkplain Aggregator} for an example of an aggregation that does this. It
     * happens to run in constant time in some cases.
     * <p>
     * In other cases {@link MinAggregator} can't get correct results by
     * taking the constant time path so instead it returns a custom
     * {@link LeafBucketCollector}. This is fairly common for aggregations
     * that have these fast paths because most of these fast paths are
     * only possible when the aggregation is at the root of the tree.
     * <p>
     * Its also useful to look at the {@link FiltersAggregator#build filters}
     * {@linkplain Aggregator} chooses whether or not it can use the fast
     * path before building the {@linkplain Aggregator} rather than on each
     * leaf. Either is fine.
     */
    protected abstract LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException;

    /**
     * Collect results for this leaf.
     * <p>
     * Implemented by the {@linkplain Aggregator} base class to correctly set
     * up sub {@linkplain Aggregator}s. See the
     * {@link #getLeafCollector(LeafReaderContext, LeafBucketCollector) abstract delegate}
     * for more details on what this does.
     */
    @Override
    public final LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        preGetSubLeafCollectors(ctx);
        final LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);
        return getLeafCollector(ctx, sub);
    }

    /**
     * Can be overridden by aggregator implementations that like the perform an operation before the leaf collectors
     * of children aggregators are instantiated for the next segment.
     */
    protected void preGetSubLeafCollectors(LeafReaderContext ctx) throws IOException {}

    /**
     * Can be overridden by aggregator implementation to be called back when the collection phase starts.
     */
    protected void doPreCollection() throws IOException {}

    @Override
    public final void preCollection() throws IOException {
        collectableSubAggregators = MultiBucketCollector.wrap(false, Arrays.asList(subAggregators));
        doPreCollection();
        collectableSubAggregators.preCollection();
    }

    /**
     * @return  The name of the aggregation.
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * @return  The parent aggregator of this aggregator. The addAggregation are hierarchical in the sense that some can
     *          be composed out of others (more specifically, bucket addAggregation can define other addAggregation that will
     *          be aggregated per bucket). This method returns the direct parent aggregator that contains this aggregator, or
     *          {@code null} if there is none (meaning, this aggregator is a top level one)
     */
    @Override
    public Aggregator parent() {
        return parent;
    }

    @Override
    public Aggregator[] subAggregators() {
        return subAggregators;
    }

    @Override
    public Aggregator subAggregator(String aggName) {
        if (subAggregatorbyName == null) {
            subAggregatorbyName = Maps.newMapWithExpectedSize(subAggregators.length);
            for (int i = 0; i < subAggregators.length; i++) {
                subAggregatorbyName.put(subAggregators[i].name(), subAggregators[i]);
            }
        }
        return subAggregatorbyName.get(aggName);
    }

    /**
     * Called after collection of all document is done.
     * <p>
     * Warning: this is not final only to allow the parent join aggregator
     * to delay this until building buckets.
     */
    @Override
    public void postCollection() throws IOException {
        // post-collect this agg before subs to make it possible to buffer and then replay in postCollection()
        doPostCollection();
        collectableSubAggregators.postCollection();
    }

    /** Called upon release of the aggregator. */
    @Override
    public void close() {
        try {
            doClose();
        } finally {
            context.breaker().addWithoutBreaking(-this.requestBytesUsed);
        }
    }

    /** Release instance-specific data. */
    protected void doClose() {}

    /**
     * Can be overridden by aggregator implementation to be called back when the collection phase ends.
     */
    protected void doPostCollection() throws IOException {}

    protected final InternalAggregations buildEmptySubAggregations() {
        List<InternalAggregation> aggs = new ArrayList<>();
        for (Aggregator aggregator : subAggregators) {
            aggs.add(aggregator.buildEmptyAggregation());
        }
        return InternalAggregations.from(aggs);
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Utilities for sharing large primitive arrays and tracking their usage.
     * Used by all subclasses.
     */
    protected final BigArrays bigArrays() {
        return context.bigArrays();
    }

    /**
     * The "top level" query that will filter the results sent to this
     * {@linkplain Aggregator}. Used by all {@linkplain Aggregator}s that
     * perform extra collection phases in addition to the one done in
     * {@link #getLeafCollector(LeafReaderContext, LeafBucketCollector)}.
     */
    protected final Query topLevelQuery() {
        return context.query();
    }

    /**
     * The searcher for the shard this {@linkplain Aggregator} is running
     * against. Used by all {@linkplain Aggregator}s that perform extra
     * collection phases in addition to the one done in
     * {@link #getLeafCollector(LeafReaderContext, LeafBucketCollector)}
     * and by to look up extra "background" information about contents of
     * the shard itself.
     */
    protected final IndexSearcher searcher() {
        return context.searcher();
    }
}
