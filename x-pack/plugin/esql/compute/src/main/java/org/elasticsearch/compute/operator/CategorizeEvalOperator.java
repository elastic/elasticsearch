/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash.CategorizeDef;
import org.elasticsearch.compute.aggregation.blockhash.CategorizeBlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.AnalysisRegistry;

/**
 * Data-node operator for distributed {@code LIMIT BY CATEGORIZE} and {@code TOPN BY CATEGORIZE}.
 *
 * <p>Wraps an inner grouping operator (e.g. {@link GroupedLimitOperator} or
 * {@link org.elasticsearch.compute.operator.topn.GroupedTopNOperator}). On each call to
 * {@link #addInput}, the text field at {@code textChannel} is classified by the ML categorizer
 * and the resulting integer category-ID block is appended to the page before delegating to the
 * inner operator.
 *
 * <p>When {@code isSingleNode=false} (distributed query with an exchange), all inner-operator
 * output is withheld until input is exhausted. The final categorizer state — reflecting every
 * log line seen by this shard — is then serialized once and appended as a constant
 * {@link BytesRefBlock} to every page before it is returned. Buffering is necessary because a
 * mid-stream snapshot would be stale: {@link GroupedLimitOperator} can accept pages and update
 * the categorizer without producing any output (when every row falls in an already-full group),
 * so snapshots taken page-by-page omit those refinements. The coordinator's global category set
 * is append-only and cannot retroactively collapse a category admitted from a stale snapshot.
 * The resulting memory cost is bounded by {@code limitPerGroup × categoryCount} rows — the same
 * bound that {@code STATS … BY CATEGORIZE} already carries via {@code CategorizeBlockHash}.
 *
 * <p>When {@code isSingleNode=true} (local-only queries without an exchange), no state channel
 * is appended; the inner operator's output is returned directly.
 */
public class CategorizeEvalOperator implements Operator {

    public static final class Factory implements Operator.OperatorFactory {
        private final int textChannel;
        private final CategorizeDef categorizeDef;
        private final AnalysisRegistry analysisRegistry;
        private final Operator.OperatorFactory innerFactory;
        private final boolean isSingleNode;

        public Factory(
            int textChannel,
            CategorizeDef categorizeDef,
            AnalysisRegistry analysisRegistry,
            Operator.OperatorFactory innerFactory,
            boolean isSingleNode
        ) {
            this.textChannel = textChannel;
            this.categorizeDef = categorizeDef;
            this.analysisRegistry = analysisRegistry;
            this.innerFactory = innerFactory;
            this.isSingleNode = isSingleNode;
        }

        @Override
        public CategorizeEvalOperator get(DriverContext driverContext) {
            return new CategorizeEvalOperator(
                textChannel,
                categorizeDef,
                analysisRegistry,
                innerFactory.get(driverContext),
                isSingleNode,
                driverContext.blockFactory()
            );
        }

        @Override
        public String describe() {
            return "CategorizeEvalOperator[channel=" + textChannel + ", inner=" + innerFactory.describe() + "]";
        }
    }

    private final int textChannel;
    private final CategorizeBlockHash blockHash;
    private final Operator inner;
    private final CategorizerStateBuffer buffer;

    private CategorizeEvalOperator(
        int textChannel,
        CategorizeDef categorizeDef,
        AnalysisRegistry analysisRegistry,
        Operator inner,
        boolean isSingleNode,
        BlockFactory blockFactory
    ) {
        this.textChannel = textChannel;
        this.inner = inner;
        AggregatorMode aggregatorMode = isSingleNode ? AggregatorMode.SINGLE : AggregatorMode.INITIAL;
        this.blockHash = new CategorizeBlockHash(blockFactory, 0, aggregatorMode, categorizeDef, analysisRegistry);
        // emitState == true only for distributed (non-single-node) execution
        this.buffer = new CategorizerStateBuffer(blockFactory, inner, blockHash, isSingleNode == false);
    }

    @Override
    public boolean needsInput() {
        return inner.needsInput();
    }

    @Override
    public void addInput(Page page) {
        IntBlock catIds = blockHash.categorize(page.getBlock(textChannel));
        boolean success = false;
        try {
            Page withCatIds = page.appendBlock(catIds);
            success = true;
            inner.addInput(withCatIds);
        } finally {
            if (success == false) {
                catIds.close();
            }
        }
        // Drain any pages the inner operator has produced. This is required when the inner is
        // GroupedLimitOperator: it stores at most one pending page in `lastOutput`, and its
        // needsInput() returns false while a page is pending. Without eagerly draining here,
        // our own needsInput() (which delegates to inner.needsInput()) would go false while
        // getOutput() still returns null (buffered output is withheld pre-finish), stalling the
        // driver pipeline.
        buffer.drainInner();
    }

    @Override
    public void finish() {
        inner.finish();
        buffer.finish();
    }

    @Override
    public boolean isFinished() {
        return buffer.isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return buffer.canProduceMoreDataWithoutExtraInput();
    }

    @Override
    public Page getOutput() {
        return buffer.getOutput();
    }

    @Override
    public String toString() {
        return "CategorizeEvalOperator[channel=" + textChannel + ", inner=" + inner + "]";
    }

    @Override
    public void close() {
        Releasables.close(buffer, inner, blockHash);
    }
}
