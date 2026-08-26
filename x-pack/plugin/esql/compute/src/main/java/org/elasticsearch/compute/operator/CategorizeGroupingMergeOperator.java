/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash.CategorizeDef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;

import java.util.Map;

/**
 * Operator for the FINAL phase (coordinator) and INTERMEDIATE phase (node-reduce driver) of
 * distributed {@code LIMIT BY CATEGORIZE} and {@code TOPN BY CATEGORIZE}.
 *
 * <p>Wraps an inner grouping operator (e.g. {@link GroupedLimitOperator} or
 * {@link org.elasticsearch.compute.operator.topn.GroupedTopNOperator}). Each page received from the exchange carries the current
 * categorizer state as a constant {@link BytesRefBlock} at {@code stateChannel} (appended by
 * {@link CategorizeGroupingOperator} in INITIAL mode, or by this operator in INTERMEDIATE mode).
 * For each page, this operator:
 * <ol>
 *   <li>Deserializes the state and merges each category into the operator's global categorizer
 *       via {@link TokenListCategorizer#mergeWireCategory}, building a local→global ID map.</li>
 *   <li>Remaps the local category-ID block at {@code catIdChannel} to global IDs, preserving
 *       multi-valued structure.</li>
 *   <li>Drops the state channel and passes the remapped page to the inner operator.</li>
 * </ol>
 *
 * <p>When {@code emitState=true} (INTERMEDIATE mode, node-reduce driver), after the inner operator
 * produces a page, this operator appends its own global categorizer state as a constant
 * {@link BytesRefBlock}. The downstream coordinator's FINAL instance merges that state. When
 * {@code emitState=false} (FINAL mode, coordinator), output pages carry only the base columns.
 *
 * <p>Because {@code mergeWireCategory} is idempotent and categories are monotonically growing,
 * re-merging an older state snapshot from a previous page is safe. This makes three-phase (or
 * four-phase in CCS) categorization equivalent to single-phase categorization.
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
    private final TokenListCategorizer.CloseableTokenListCategorizer globalCategorizer;
    private final Operator inner;
    private final boolean emitState;
    private final BlockFactory blockFactory;

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
        this.globalCategorizer = new TokenListCategorizer.CloseableTokenListCategorizer(
            new CategorizationBytesRefHash(new BytesRefHash(2048, blockFactory.bigArrays())),
            CategorizationPartOfSpeechDictionary.getInstance(),
            categorizeDef.similarityThreshold() / 100.0f
        );
    }

    @Override
    public boolean needsInput() {
        return inner.needsInput();
    }

    @Override
    public void addInput(Page page) {
        inner.addInput(mergeAndRemap(page));
    }

    @Override
    public void finish() {
        inner.finish();
    }

    @Override
    public boolean isFinished() {
        return inner.isFinished();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return inner.canProduceMoreDataWithoutExtraInput();
    }

    @Override
    public Page getOutput() {
        Page p = inner.getOutput();
        if (p == null || emitState == false) {
            return p;
        }
        BytesRefBlock stateBlock = null;
        boolean success = false;
        try {
            BytesRef state = CategorizerStateCodec.serialize(globalCategorizer);
            stateBlock = blockFactory.newConstantBytesRefBlockWith(state, p.getPositionCount());
            Page result = p.appendBlock(stateBlock);
            success = true;
            return result;
        } finally {
            if (success == false) {
                if (stateBlock != null) {
                    stateBlock.close();
                }
                p.releaseBlocks();
            }
        }
    }

    private Page mergeAndRemap(Page page) {
        BytesRefBlock stateBlock = page.getBlock(stateChannel);
        IntBlock localCatIds = page.getBlock(catIdChannel);

        Map<Integer, Integer> idMap = CategorizerStateCodec.buildIdMap(stateBlock, globalCategorizer);
        IntBlock globalCatIds = remapIntBlock(localCatIds, idMap);

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

    /** Remaps an {@link IntBlock} using {@code idMap}, preserving multi-valued structure. */
    private IntBlock remapIntBlock(IntBlock original, Map<Integer, Integer> idMap) {
        IntVector vec = original.asVector();
        if (vec != null) {
            try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(vec.getPositionCount())) {
                for (int p = 0; p < vec.getPositionCount(); p++) {
                    builder.appendInt(p, idMap.getOrDefault(vec.getInt(p), CategorizerStateCodec.NULL_ORD));
                }
                return builder.build().asBlock();
            }
        }
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(original.getPositionCount())) {
            for (int pos = 0; pos < original.getPositionCount(); pos++) {
                if (original.isNull(pos)) {
                    builder.appendInt(CategorizerStateCodec.NULL_ORD);
                    continue;
                }
                int first = original.getFirstValueIndex(pos);
                int count = original.getValueCount(pos);
                if (count == 1) {
                    builder.appendInt(idMap.getOrDefault(original.getInt(first), CategorizerStateCodec.NULL_ORD));
                } else {
                    builder.beginPositionEntry();
                    for (int i = first; i < first + count; i++) {
                        builder.appendInt(idMap.getOrDefault(original.getInt(i), CategorizerStateCodec.NULL_ORD));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
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
        Releasables.close(inner, globalCategorizer);
    }
}
