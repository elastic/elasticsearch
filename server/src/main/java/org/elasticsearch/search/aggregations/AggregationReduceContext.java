/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

/**
 * Dependencies used to reduce aggs.
 */
public abstract sealed class AggregationReduceContext permits AggregationReduceContext.ForPartial, AggregationReduceContext.ForFinal {
    /**
     * Builds {@link AggregationReduceContext}s.
     */
    public interface Builder {
        /**
         * Build an {@linkplain AggregationReduceContext} to perform a partial reduction.
         */
        default AggregationReduceContext forPartialReduction() {
            return forPartialReduction(null);
        }

        /**
         * Build an {@linkplain AggregationReduceContext} to perform the final reduction.
         */
        default AggregationReduceContext forFinalReduction() {
            return forFinalReduction(null);
        }

        /**
         * Build an {@linkplain AggregationReduceContext} to perform a partial reduction.
         * @param topHitsToRelease optional list to collect SearchHits from top_hits aggs for release by SearchResponse
         */
        AggregationReduceContext forPartialReduction(@Nullable List<SearchHits> topHitsToRelease);

        /**
         * Build an {@linkplain AggregationReduceContext} to perform the final reduction.
         * @param topHitsToRelease optional list to collect SearchHits from top_hits aggs for release by SearchResponse
         */
        AggregationReduceContext forFinalReduction(@Nullable List<SearchHits> topHitsToRelease);
    }

    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final Supplier<Boolean> isCanceled;
    /**
     * Builder for the agg being processed or {@code null} if this context
     * was built for the top level or a pipeline aggregation.
     */
    @Nullable
    private final AggregationBuilder builder;
    private final AggregatorFactories.Builder subBuilders;
    @Nullable
    protected final List<SearchHits> topHitsToRelease;
    private boolean hasBatchedResult;

    private AggregationReduceContext(
        BigArrays bigArrays,
        ScriptService scriptService,
        Supplier<Boolean> isCanceled,
        AggregatorFactories.Builder subBuilders,
        @Nullable List<SearchHits> topHitsToRelease
    ) {
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.isCanceled = isCanceled;
        this.builder = null;
        this.subBuilders = subBuilders;
        this.topHitsToRelease = topHitsToRelease;
    }

    private AggregationReduceContext(
        BigArrays bigArrays,
        ScriptService scriptService,
        Supplier<Boolean> isCanceled,
        AggregationBuilder builder,
        @Nullable List<SearchHits> topHitsToRelease
    ) {
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.isCanceled = isCanceled;
        this.builder = builder;
        this.subBuilders = builder.factoriesBuilder;
        this.topHitsToRelease = topHitsToRelease;
    }

    /**
     * Register SearchHits from a top_hits aggregation so that SearchResponse can release them.
     */
    public final void registerTopHitsForRelease(SearchHits searchHits) {
        if (topHitsToRelease != null) {
            topHitsToRelease.add(searchHits);
        }
    }

    /**
     * Returns <code>true</code> iff the current reduce phase is the final
     * reduce phase. This indicates if operations like pipeline aggregations
     * should be applied or if specific features like {@code minDocCount}
     * should be taken into account. Operations that are potentially losing
     * information can only be applied during the final reduce phase.
     */
    public abstract boolean isFinalReduce();

    public final BigArrays bigArrays() {
        return bigArrays;
    }

    public final ScriptService scriptService() {
        return scriptService;
    }

    public final Supplier<Boolean> isCanceled() {
        return isCanceled;
    }

    /**
     * Builder for the agg being processed or {@code null} if this context
     * was built for the top level or a pipeline aggregation.
     */
    public AggregationBuilder builder() {
        return builder;
    }

    /**
     * The root of the tree of pipeline aggregations for this request.
     */
    public abstract PipelineTree pipelineTreeRoot();

    /**
     * Adds {@code count} buckets to the global count for the request and fails if this number is greater than
     * the maximum number of buckets allowed in a response
     */
    public final void consumeBucketsAndMaybeBreak(int size) {
        // This is a volatile read.
        if (isCanceled.get()) {
            throw new TaskCancelledException("Cancelled");
        }
        consumeBucketCountAndMaybeBreak(size);
    }

    protected abstract void consumeBucketCountAndMaybeBreak(int size);

    /**
     * Build a {@link AggregationReduceContext} for a sub-aggregation.
     */
    public final AggregationReduceContext forAgg(String name) {
        for (AggregationBuilder b : subBuilders.getAggregatorFactories()) {
            if (b.getName().equals(name)) {
                return forSubAgg(b);
            }
        }
        throw new IllegalArgumentException("reducing an aggregation [" + name + "] that wasn't requested");
    }

    protected abstract AggregationReduceContext forSubAgg(AggregationBuilder sub);

    public boolean hasBatchedResult() {
        return hasBatchedResult;
    }

    public void setHasBatchedResult(boolean hasBatchedResult) {
        this.hasBatchedResult = hasBatchedResult;
    }

    /**
     * A {@linkplain AggregationReduceContext} to perform a partial reduction.
     */
    public static final class ForPartial extends AggregationReduceContext {
        private final IntConsumer multiBucketConsumer;

        public ForPartial(
            BigArrays bigArrays,
            ScriptService scriptService,
            Supplier<Boolean> isCanceled,
            AggregatorFactories.Builder builders,
            IntConsumer multiBucketConsumer,
            @Nullable List<SearchHits> topHitsToRelease
        ) {
            super(bigArrays, scriptService, isCanceled, builders, topHitsToRelease);
            this.multiBucketConsumer = multiBucketConsumer;
        }

        public ForPartial(
            BigArrays bigArrays,
            ScriptService scriptService,
            Supplier<Boolean> isCanceled,
            AggregationBuilder builder,
            IntConsumer multiBucketConsumer,
            @Nullable List<SearchHits> topHitsToRelease
        ) {
            super(bigArrays, scriptService, isCanceled, builder, topHitsToRelease);
            this.multiBucketConsumer = multiBucketConsumer;
        }

        @Override
        public boolean isFinalReduce() {
            return false;
        }

        @Override
        protected void consumeBucketCountAndMaybeBreak(int size) {
            multiBucketConsumer.accept(size);
        }

        @Override
        public PipelineTree pipelineTreeRoot() {
            return null;
        }

        @Override
        protected AggregationReduceContext forSubAgg(AggregationBuilder sub) {
            return new ForPartial(bigArrays(), scriptService(), isCanceled(), sub, multiBucketConsumer, topHitsToRelease);
        }
    }

    /**
     * A {@linkplain AggregationReduceContext} to perform the final reduction.
     */
    public static final class ForFinal extends AggregationReduceContext {
        private final IntConsumer multiBucketConsumer;
        private final PipelineTree pipelineTreeRoot;

        public ForFinal(
            BigArrays bigArrays,
            ScriptService scriptService,
            Supplier<Boolean> isCanceled,
            AggregatorFactories.Builder builders,
            IntConsumer multiBucketConsumer,
            @Nullable List<SearchHits> topHitsToRelease
        ) {
            super(bigArrays, scriptService, isCanceled, builders, topHitsToRelease);
            this.multiBucketConsumer = multiBucketConsumer;
            this.pipelineTreeRoot = builders == null ? null : builders.buildPipelineTree();
        }

        public ForFinal(
            BigArrays bigArrays,
            ScriptService scriptService,
            Supplier<Boolean> isCanceled,
            AggregationBuilder builder,
            IntConsumer multiBucketConsumer,
            PipelineTree pipelineTreeRoot,
            @Nullable List<SearchHits> topHitsToRelease
        ) {
            super(bigArrays, scriptService, isCanceled, builder, topHitsToRelease);
            this.multiBucketConsumer = multiBucketConsumer;
            this.pipelineTreeRoot = pipelineTreeRoot;
        }

        @Override
        public boolean isFinalReduce() {
            return true;
        }

        @Override
        protected void consumeBucketCountAndMaybeBreak(int size) {
            multiBucketConsumer.accept(size);
        }

        @Override
        public PipelineTree pipelineTreeRoot() {
            return pipelineTreeRoot;
        }

        @Override
        protected AggregationReduceContext forSubAgg(AggregationBuilder sub) {
            ForFinal subContext = new ForFinal(
                bigArrays(),
                scriptService(),
                isCanceled(),
                sub,
                multiBucketConsumer,
                pipelineTreeRoot,
                topHitsToRelease
            );
            subContext.setHasBatchedResult(hasBatchedResult());
            return subContext;
        }
    }
}
