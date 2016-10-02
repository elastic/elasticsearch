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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.aggregations.bucket.BestBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;
import org.elasticsearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base implementation for concrete aggregators.
 */
public abstract class AggregatorBase extends Aggregator {

    /** The default "weight" that a bucket takes when performing an aggregation */
    public static final int DEFAULT_WEIGHT = 1024 * 5; // 5kb

    protected final String name;
    protected final Aggregator parent;
    protected final AggregationContext context;
    private final Map<String, Object> metaData;

    protected final Aggregator[] subAggregators;
    protected BucketCollector collectableSubAggregators;

    private Map<String, Aggregator> subAggregatorbyName;
    private DeferringBucketCollector recordingWrapper;
    private final List<PipelineAggregator> pipelineAggregators;
    private final CircuitBreakerService breakerService;
    private boolean failed = false;

    /**
     * Constructs a new Aggregator.
     *
     * @param name                  The name of the aggregation
     * @param factories             The factories for all the sub-aggregators under this aggregator
     * @param context               The aggregation context
     * @param parent                The parent aggregator (may be {@code null} for top level aggregators)
     * @param metaData              The metaData associated with this aggregator
     */
    protected AggregatorBase(String name, AggregatorFactories factories, AggregationContext context, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        this.name = name;
        this.pipelineAggregators = pipelineAggregators;
        this.metaData = metaData;
        this.parent = parent;
        this.context = context;
        this.breakerService = context.bigArrays().breakerService();
        assert factories != null : "sub-factories provided to BucketAggregator must not be null, use AggragatorFactories.EMPTY instead";
        this.subAggregators = factories.createSubAggregators(this);
        context.searchContext().addReleasable(this, Lifetime.PHASE);
        // Register a safeguard to highlight any invalid construction logic (call to this constructor without subsequent preCollection call)
        collectableSubAggregators = new BucketCollector() {
            void badState(){
                throw new QueryPhaseExecutionException(AggregatorBase.this.context.searchContext(),
                        "preCollection not called on new Aggregator before use", null);
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
            public boolean needsScores() {
                badState();
                return false; // unreachable
            }
        };
        try {
            this.breakerService
                    .getBreaker(CircuitBreaker.REQUEST)
                    .addEstimateBytesAndMaybeBreak(DEFAULT_WEIGHT, "<agg [" + name + "]>");
        } catch (CircuitBreakingException cbe) {
            this.failed = true;
            throw cbe;
        }
    }

    /**
     * Most aggregators don't need scores, make sure to extend this method if
     * your aggregator needs them.
     */
    @Override
    public boolean needsScores() {
        for (Aggregator agg : subAggregators) {
            if (agg.needsScores()) {
                return true;
            }
        }
        return false;
    }

    public Map<String, Object> metaData() {
        return this.metaData;
    }

    public List<PipelineAggregator> pipelineAggregators() {
        return this.pipelineAggregators;
    }

    /**
     * Get a {@link LeafBucketCollector} for the given ctx, which should
     * delegate to the given collector.
     */
    protected abstract LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException;

    @Override
    public final LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        final LeafBucketCollector sub = collectableSubAggregators.getLeafCollector(ctx);
        return getLeafCollector(ctx, sub);
    }

    /**
     * Can be overridden by aggregator implementation to be called back when the collection phase starts.
     */
    protected void doPreCollection() throws IOException {
    }

    @Override
    public final void preCollection() throws IOException {
        List<BucketCollector> collectors = new ArrayList<>();
        List<BucketCollector> deferredCollectors = new ArrayList<>();
        for (int i = 0; i < subAggregators.length; ++i) {
            if (shouldDefer(subAggregators[i])) {
                if (recordingWrapper == null) {
                    recordingWrapper = getDeferringCollector();
                }
                deferredCollectors.add(subAggregators[i]);
                subAggregators[i] = recordingWrapper.wrap(subAggregators[i]);
            } else {
                collectors.add(subAggregators[i]);
            }
        }
        if (recordingWrapper != null) {
            recordingWrapper.setDeferredCollector(deferredCollectors);
            collectors.add(recordingWrapper);
        }
        collectableSubAggregators = BucketCollector.wrap(collectors);
        doPreCollection();
        collectableSubAggregators.preCollection();
    }

    public DeferringBucketCollector getDeferringCollector() {
        // Default impl is a collector that selects the best buckets
        // but an alternative defer policy may be based on best docs.
        return new BestBucketsDeferringCollector(context());
    }

    /**
     * This method should be overidden by subclasses that want to defer calculation
     * of a child aggregation until a first pass is complete and a set of buckets has
     * been pruned.
     * Deferring collection will require the recording of all doc/bucketIds from the first
     * pass and then the sub class should call {@link #runDeferredCollections(long...)}
     * for the selected set of buckets that survive the pruning.
     * @param aggregator the child aggregator
     * @return true if the aggregator should be deferred
     * until a first pass at collection has completed
     */
    protected boolean shouldDefer(Aggregator aggregator) {
        return false;
    }

    protected final void runDeferredCollections(long... bucketOrds) throws IOException{
        // Being lenient here - ignore calls where there are no deferred collections to playback
        if (recordingWrapper != null) {
            recordingWrapper.replay(bucketOrds);
        }
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
    public AggregationContext context() {
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
            if (!this.failed) {
                this.breakerService.getBreaker(CircuitBreaker.REQUEST).addWithoutBreaking(-DEFAULT_WEIGHT);
            }
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
        return new InternalAggregations(aggs);
    }

    @Override
    public String toString() {
        return name;
    }
}
