/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash.CategorizeDef;
import org.elasticsearch.compute.aggregation.blockhash.CategorizeBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

/**
 * Operator for the FINAL phase (coordinator) and INTERMEDIATE phase (node-reduce driver) of
 * distributed {@code LIMIT BY CATEGORIZE} and {@code TOPN BY CATEGORIZE}.
 *
 * <p>Wraps an inner grouping operator (e.g. {@link GroupedLimitOperator} or
 * {@link org.elasticsearch.compute.operator.topn.GroupedTopNOperator}). Each page received from
 * the exchange carries the current categorizer state as a constant {@link BytesRefBlock} at
 * {@code stateChannel} (appended by {@link CategorizeEvalOperator} in INITIAL mode, or by
 * this operator in INTERMEDIATE mode). For each page, this operator:
 * <ol>
 *   <li>Deserializes the state and merges each category into the operator's global categorizer
 *       via {@link org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer#mergeWireCategory},
 *       building a local→global ID map.</li>
 *   <li>Remaps the local category-ID block at {@code catIdChannel} to global IDs, preserving
 *       multi-valued structure.</li>
 *   <li>Drops the state channel and passes the remapped page to the inner operator.</li>
 * </ol>
 *
 * <p>When {@code emitState=true} (INTERMEDIATE mode, node-reduce driver), all inner-operator
 * output is withheld until input is exhausted. The final node-level categorizer state is then
 * serialized once and appended as a constant {@link BytesRefBlock} to every output page, so that
 * the downstream FINAL instance receives an accurate, complete model. Buffering is required for
 * the same reason as {@link CategorizeEvalOperator}: the inner {@link GroupedLimitOperator}
 * can process pages without emitting output, and a mid-stream snapshot would leave the global
 * category set stale.
 *
 * <p>When {@code emitState=false} (FINAL mode, coordinator), output pages carry only the base
 * columns. Category IDs are assigned per incoming page as the global model grows — identical to
 * the append-only behaviour of {@code CategorizeBlockHash} in FINAL mode.
 */
public class CategorizeGroupingMergeOperator implements Operator {

    public static final class Factory implements Operator.OperatorFactory {
        private final int catIdChannel;
        private final int stateChannel;
        private final CategorizeDef categorizeDef;
        private final Operator.OperatorFactory innerFactory;
        private final boolean emitState;

        public Factory(int catIdChannel, int stateChannel, CategorizeDef categorizeDef, Operator.OperatorFactory innerFactory) {
            this(catIdChannel, stateChannel, categorizeDef, innerFactory, false);
        }

        public Factory(
            int catIdChannel,
            int stateChannel,
            CategorizeDef categorizeDef,
            Operator.OperatorFactory innerFactory,
            boolean emitState
        ) {
            this.catIdChannel = catIdChannel;
            this.stateChannel = stateChannel;
            this.categorizeDef = categorizeDef;
            this.innerFactory = innerFactory;
            this.emitState = emitState;
        }

        @Override
        public CategorizeGroupingMergeOperator get(DriverContext driverContext) {
            return new CategorizeGroupingMergeOperator(
                catIdChannel,
                stateChannel,
                categorizeDef,
                innerFactory.get(driverContext),
                emitState,
                driverContext.blockFactory()
            );
        }

        @Override
        public String describe() {
            return "CategorizeGroupingMergeOperator[catIdChannel="
                + catIdChannel
                + ", stateChannel="
                + stateChannel
                + ", emitState="
                + emitState
                + ", inner="
                + innerFactory.describe()
                + "]";
        }
    }

    private final int catIdChannel;
    private final int stateChannel;
    private final CategorizeBlockHash blockHash;
    private final Operator inner;
    private final BlockFactory blockFactory;
    private final boolean emitState;
    private final CategorizerStateBuffer buffer;

    private CategorizeGroupingMergeOperator(
        int catIdChannel,
        int stateChannel,
        CategorizeDef categorizeDef,
        Operator inner,
        boolean emitState,
        BlockFactory blockFactory
    ) {
        this.catIdChannel = catIdChannel;
        this.stateChannel = stateChannel;
        this.blockFactory = blockFactory;
        this.inner = inner;
        this.emitState = emitState;
        boolean success = false;
        try {
            AggregatorMode aggregatorMode = emitState ? AggregatorMode.INTERMEDIATE : AggregatorMode.FINAL;
            this.blockHash = new CategorizeBlockHash(blockFactory, 0, aggregatorMode, categorizeDef, null);
            this.buffer = new CategorizerStateBuffer(blockFactory, inner, blockHash, emitState);
            success = true;
        } finally {
            if (success == false) {
                inner.close();
            }
        }
    }

    @Override
    public boolean needsInput() {
        return inner.needsInput();
    }

    @Override
    public void addInput(Page page) {
        boolean pageConsumed = false;
        try {
            Page remapped = mergeAndRemap(page);
            // mergeAndRemap consumes page (closes the state and catId blocks, moves the rest
            // into remapped). The inner operator (e.g. GroupedLimitOperator) always releases
            // the page it receives, even on exception, so we must not release remapped ourselves.
            pageConsumed = true;
            inner.addInput(remapped);
        } finally {
            if (pageConsumed == false) {
                page.releaseBlocks();
            }
        }
        // See CategorizeEvalOperator.addInput for the rationale: GroupedLimitOperator can
        // leave a page in `lastOutput` which turns inner.needsInput() false. Drain eagerly so
        // our own needsInput() (which delegates) does not stall the driver pipeline.
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

    private Page mergeAndRemap(Page page) {
        BytesRefBlock stateBlock = page.getBlock(stateChannel);
        IntBlock localCatIds = page.getBlock(catIdChannel);
        BytesRef stateBytes = stateBlock.getBytesRef(stateBlock.getFirstValueIndex(0), new BytesRef());

        IntBlock globalCatIds = blockHash.recategorize(stateBytes, localCatIds);

        boolean success = false;
        try {
            Page result = rebuildPage(page, catIdChannel, globalCatIds, stateChannel);
            success = true;
            return result;
        } finally {
            if (success == false) {
                globalCatIds.close();
            }
        }
    }

    /**
     * Reconstructs the page replacing the block at {@code catIdChannel} with {@code newCatIds}
     * and dropping the block at {@code stateChannel}.
     *
     * <p>Ownership of all original blocks transfers to this method. Blocks not included in the
     * new page are explicitly closed here; blocks included in the new page are owned by it.
     */
    private static Page rebuildPage(Page page, int catIdChannel, IntBlock newCatIds, int stateChannel) {
        int blockCount = page.getBlockCount();
        Block[] blocks = new Block[blockCount - 1];
        int out = 0;
        for (int i = 0; i < blockCount; i++) {
            if (i == stateChannel) {
                page.getBlock(i).close();
                continue;
            }
            if (i == catIdChannel) {
                page.getBlock(i).close();
                blocks[out++] = newCatIds;
            } else {
                blocks[out++] = page.getBlock(i);
            }
        }
        return new Page(blocks);
    }

    @Override
    public String toString() {
        return "CategorizeGroupingMergeOperator[catIdChannel="
            + catIdChannel
            + ", stateChannel="
            + stateChannel
            + ", emitState="
            + emitState
            + ", inner="
            + inner
            + "]";
    }

    @Override
    public void close() {
        Releasables.close(buffer, inner, blockHash);
    }
}
