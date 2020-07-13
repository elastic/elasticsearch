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
package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;
import org.elasticsearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
    protected final SearchContext context;
    private final Map<String, Object> metadata;

    protected final Aggregator[] subAggregators;
    protected BucketCollector collectableSubAggregators;

    private Map<String, Aggregator> subAggregatorbyName;
    private final CircuitBreakerService breakerService;
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
    protected AggregatorBase(String name, AggregatorFactories factories, SearchContext context, Aggregator parent,
            CardinalityUpperBound subAggregatorCardinality, Map<String, Object> metadata) throws IOException {
        this.name = name;
        this.metadata = metadata;
        this.parent = parent;
        this.context = context;
        this.breakerService = context.bigArrays().breakerService();
        assert factories != null : "sub-factories provided to BucketAggregator must not be null, use AggragatorFactories.EMPTY instead";
        this.subAggregators = factories.createSubAggregators(context, this, subAggregatorCardinality);
        context.addReleasable(this, Lifetime.PHASE);
        final SearchShardTarget shardTarget = context.shardTarget();
        // Register a safeguard to highlight any invalid construction logic (call to this constructor without subsequent preCollection call)
        collectableSubAggregators = new BucketCollector() {
            void badState(){
                throw new QueryPhaseExecutionException(shardTarget, "preCollection not called on new Aggregator before use", null);
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
        if (context.query() != null && context.query().getClass() != MatchAllDocsQuery.class) {
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
            this.breakerService
                    .getBreaker(CircuitBreaker.REQUEST)
                    .addEstimateBytesAndMaybeBreak(bytes, "<agg [" + name + "]>");
        } else {
            this.breakerService
                    .getBreaker(CircuitBreaker.REQUEST)
                    .addWithoutBreaking(bytes);
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
     * Get a {@link LeafBucketCollector} for the given ctx, which should
     * delegate to the given collector.
     */
    protected abstract LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException;

    @Override
    public final LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        preGetSubLeafCollectors();
        final LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);
        return getLeafCollector(ctx, sub);
    }

    /**
     * Can be overridden by aggregator implementations that like the perform an operation before the leaf collectors
     * of children aggregators are instantiated for the next segment.
     */
    protected void preGetSubLeafCollectors() throws IOException {
    }

    /**
     * Can be overridden by aggregator implementation to be called back when the collection phase starts.
     */
    protected void doPreCollection() throws IOException {
    }

    @Override
    public final void preCollection() throws IOException {
        List<BucketCollector> collectors = Arrays.asList(subAggregators);
        collectableSubAggregators = MultiBucketCollector.wrap(collectors);
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

    public Aggregator[] subAggregators() {
        return subAggregators;
    }

    @Override
    public Aggregator subAggregator(String aggName) {
        if (subAggregatorbyName == null) {
            subAggregatorbyName = new HashMap<>(subAggregators.length);
            for (int i = 0; i < subAggregators.length; i++) {
                subAggregatorbyName.put(subAggregators[i].name(), subAggregators[i]);
            }
        }
        return subAggregatorbyName.get(aggName);
    }

    /**
     * @return  The current aggregation context.
     */
    @Override
    public SearchContext context() {
        return context;
    }

    /**
     * Called after collection of all document is done.
     */
    @Override
    public final void postCollection() throws IOException {
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
            this.breakerService.getBreaker(CircuitBreaker.REQUEST).addWithoutBreaking(-this.requestBytesUsed);
        }
    }

    /** Release instance-specific data. */
    protected void doClose() {}

    /**
     * Can be overridden by aggregator implementation to be called back when the collection phase ends.
     */
    protected void doPostCollection() throws IOException {
    }

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
}
