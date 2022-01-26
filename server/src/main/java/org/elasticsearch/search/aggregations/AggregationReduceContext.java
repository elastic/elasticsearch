/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.function.IntConsumer;
import java.util.function.Supplier;

public abstract sealed class AggregationReduceContext permits AggregationReduceContext.ForPartial,AggregationReduceContext.ForFinal {
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

    public AggregationReduceContext(BigArrays bigArrays, ScriptService scriptService, Supplier<Boolean> isCanceled) {
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.isCanceled = isCanceled;
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
     * A {@linkplain AggregationReduceContext} to perform a partial reduction.
     */
    public static final class ForPartial extends AggregationReduceContext {
        public ForPartial(BigArrays bigArrays, ScriptService scriptService, Supplier<Boolean> isCanceled) {
            super(bigArrays, scriptService, isCanceled);
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
            IntConsumer multiBucketConsumer,
            PipelineTree pipelineTreeRoot,
            Supplier<Boolean> isCanceled
        ) {
            super(bigArrays, scriptService, isCanceled);
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
    }
}
