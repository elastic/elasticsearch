/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.tasks.TaskCancelledException;

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
        AggregationReduceContext forPartialReduction();

        /**
         * Build an {@linkplain AggregationReduceContext} to perform the final reduction.
         */
        AggregationReduceContext forFinalReduction();
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

    private AggregationReduceContext(
        BigArrays bigArrays,
        ScriptService scriptService,
        Supplier<Boolean> isCanceled,
        AggregatorFactories.Builder subBuilders
    ) {
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.isCanceled = isCanceled;
        this.builder = null;
        this.subBuilders = subBuilders;
    }

    private AggregationReduceContext(
        BigArrays bigArrays,
        ScriptService scriptService,
        Supplier<Boolean> isCanceled,
        AggregationBuilder builder
    ) {
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.isCanceled = isCanceled;
        this.builder = builder;
        this.subBuilders = builder.factoriesBuilder;
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

    /**
     * A {@linkplain AggregationReduceContext} to perform a partial reduction.
     */
    public static final class ForPartial extends AggregationReduceContext {
        public ForPartial(
            BigArrays bigArrays,
            ScriptService scriptService,
            Supplier<Boolean> isCanceled,
            AggregatorFactories.Builder builders
        ) {
            super(bigArrays, scriptService, isCanceled, builders);
        }

        public ForPartial(BigArrays bigArrays, ScriptService scriptService, Supplier<Boolean> isCanceled, AggregationBuilder builder) {
            super(bigArrays, scriptService, isCanceled, builder);
        }

        @Override
        public boolean isFinalReduce() {
            return false;
        }

        @Override
        protected void consumeBucketCountAndMaybeBreak(int size) {}

        @Override
        public PipelineTree pipelineTreeRoot() {
            return null;
        }

        @Override
        protected AggregationReduceContext forSubAgg(AggregationBuilder sub) {
            return new ForPartial(bigArrays(), scriptService(), isCanceled(), sub);
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
            IntConsumer multiBucketConsumer
        ) {
            super(bigArrays, scriptService, isCanceled, builders);
            this.multiBucketConsumer = multiBucketConsumer;
            this.pipelineTreeRoot = builders == null ? null : builders.buildPipelineTree();
        }

        public ForFinal(
            BigArrays bigArrays,
            ScriptService scriptService,
            Supplier<Boolean> isCanceled,
            AggregationBuilder builder,
            IntConsumer multiBucketConsumer,
            PipelineTree pipelineTreeRoot
        ) {
            super(bigArrays, scriptService, isCanceled, builder);
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
            return new ForFinal(bigArrays(), scriptService(), isCanceled(), sub, multiBucketConsumer, pipelineTreeRoot);
        }
    }
}
