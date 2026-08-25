/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
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
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Coordinator-side operator for the FINAL phase of distributed {@code LIMIT BY CATEGORIZE}.
 *
 * <p>Receives pages whose last channel is a constant {@link BytesRefBlock} containing a
 * serialized {@link org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer} state
 * (written by a data-node {@link CategorizeStateEmitOperator}), and whose second-to-last
 * channel is an {@link IntBlock} of local category IDs (written by
 * {@link CategorizeEvalOperator}).
 *
 * <p>For each page:
 * <ol>
 *   <li>Deserializes the state and merges each category into the coordinator's global
 *       categorizer via
 *       {@link TokenListCategorizer#mergeWireCategory(SerializableTokenListCategory)},
 *       building a {@code localId → globalId} mapping.</li>
 *   <li>Remaps the {@link IntBlock} of local category IDs to global IDs, preserving
 *       multi-valued structure ({@code [a, b] ≠ [b, a]}) and mapping
 *       {@code NULL_ORD (0) → 0}.</li>
 *   <li>Drops the state channel and replaces the local-ID channel with the remapped
 *       global-ID channel.</li>
 * </ol>
 *
 * <p>The output pages then feed a {@link GroupedLimitOperator} that applies the final
 * global limit using the global category IDs.
 */
public class CategorizeStateMergeOperator extends AbstractPageMappingOperator {

    public static final class Factory implements Operator.OperatorFactory {
        private final int catIdChannel;
        private final int stateChannel;
        private final CategorizeDef categorizeDef;

        public Factory(int catIdChannel, int stateChannel, CategorizeDef categorizeDef) {
            this.catIdChannel = catIdChannel;
            this.stateChannel = stateChannel;
            this.categorizeDef = categorizeDef;
        }

        @Override
        public CategorizeStateMergeOperator get(DriverContext driverContext) {
            return new CategorizeStateMergeOperator(catIdChannel, stateChannel, categorizeDef, driverContext.blockFactory());
        }

        @Override
        public String describe() {
            return "CategorizeStateMergeOperator[catIdChannel=" + catIdChannel + ", stateChannel=" + stateChannel + "]";
        }
    }

    private static final int NULL_ORD = 0;

    private final int catIdChannel;
    private final int stateChannel;
    private final TokenListCategorizer.CloseableTokenListCategorizer globalCategorizer;
    private final BlockFactory blockFactory;

    private CategorizeStateMergeOperator(int catIdChannel, int stateChannel, CategorizeDef categorizeDef, BlockFactory blockFactory) {
        this.catIdChannel = catIdChannel;
        this.stateChannel = stateChannel;
        this.blockFactory = blockFactory;
        this.globalCategorizer = new TokenListCategorizer.CloseableTokenListCategorizer(
            new CategorizationBytesRefHash(new BytesRefHash(2048, blockFactory.bigArrays())),
            CategorizationPartOfSpeechDictionary.getInstance(),
            categorizeDef.similarityThreshold() / 100.0f
        );
    }

    @Override
    protected Page process(Page page) {
        BytesRefBlock stateBlock = page.getBlock(stateChannel);
        IntBlock localCatIds = page.getBlock(catIdChannel);

        Map<Integer, Integer> idMap = buildIdMap(stateBlock);
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

    /**
     * Deserializes the state from the constant BytesRefBlock and builds a local→global ID map.
     */
    private Map<Integer, Integer> buildIdMap(BytesRefBlock stateBlock) {
        Map<Integer, Integer> idMap = new HashMap<>();
        idMap.put(NULL_ORD, NULL_ORD);

        BytesRef stateBytes = stateBlock.getBytesRef(stateBlock.getFirstValueIndex(0), new BytesRef());
        try (StreamInput in = new BytesArray(stateBytes).streamInput()) {
            boolean seenNull = in.readBoolean();
            if (seenNull) {
                idMap.put(NULL_ORD, NULL_ORD);
            }
            int count = in.readVInt();
            for (int oldId = 0; oldId < count; oldId++) {
                int newGlobalId = globalCategorizer.mergeWireCategory(new SerializableTokenListCategory(in)).getId();
                idMap.put(oldId + 1, newGlobalId + 1);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return idMap;
    }

    /**
     * Remaps an {@link IntBlock} using {@code idMap}, preserving multi-valued structure.
     */
    private IntBlock remapIntBlock(IntBlock original, Map<Integer, Integer> idMap) {
        IntVector vec = original.asVector();
        if (vec != null) {
            try (IntVector.FixedBuilder builder = blockFactory.newIntVectorFixedBuilder(vec.getPositionCount())) {
                for (int p = 0; p < vec.getPositionCount(); p++) {
                    builder.appendInt(p, idMap.getOrDefault(vec.getInt(p), NULL_ORD));
                }
                return builder.build().asBlock();
            }
        }
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(original.getPositionCount())) {
            for (int pos = 0; pos < original.getPositionCount(); pos++) {
                if (original.isNull(pos)) {
                    builder.appendInt(NULL_ORD);
                    continue;
                }
                int first = original.getFirstValueIndex(pos);
                int count = original.getValueCount(pos);
                if (count == 1) {
                    builder.appendInt(idMap.getOrDefault(original.getInt(first), NULL_ORD));
                } else {
                    builder.beginPositionEntry();
                    for (int i = first; i < first + count; i++) {
                        builder.appendInt(idMap.getOrDefault(original.getInt(i), NULL_ORD));
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
                // Drop the state block — release it since it's not in the new page.
                page.getBlock(i).close();
                continue;
            }
            if (i == catIdChannel) {
                // Replace the local-ID block with the global-ID block — release the original.
                page.getBlock(i).close();
                blocks[out++] = newCatIds;
            } else {
                // Keep this block in the new page; ownership transfers.
                blocks[out++] = page.getBlock(i);
            }
        }
        return new Page(blocks);
    }

    @Override
    public String toString() {
        return "CategorizeStateMergeOperator[catIdChannel=" + catIdChannel + ", stateChannel=" + stateChannel + "]";
    }

    @Override
    public void close() {
        Releasables.close(super::close, globalCategorizer);
    }
}
