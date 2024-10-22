/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CategorizedIntermediateBlockHash extends AbstractCategorizeBlockHash {
    private final IntBlockHash hash;

    CategorizedIntermediateBlockHash(
        BlockFactory blockFactory,
        boolean outputPartial,
        TokenListCategorizer.CloseableTokenListCategorizer categorizer,
        IntBlockHash hash,
        int channel
    ) {
        super(blockFactory, channel, outputPartial, categorizer);
        this.hash = hash;
    }

    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        CompositeBlock block = page.getBlock(channel());
        BytesRefBlock groupingState = block.getBlock(0);
        BytesRefBlock groups = block.getBlock(0);
        Map<Integer, Integer> idMap;
        if (groupingState.areAllValuesNull() == false) {
            idMap = readIntermediate(groupingState.getBytesRef(0, new BytesRef()));
        } else {
            idMap = Collections.emptyMap();
        }
        try (IntBlock.Builder newIdsBuilder = blockFactory.newIntBlockBuilder(groups.getTotalValueCount())) {
            for (int i = 0; i < groups.getTotalValueCount(); i++) {
                newIdsBuilder.appendInt(idMap.get(i));
            }
            IntBlock newIds = newIdsBuilder.build();
            addInput.add(0, hash.add(newIds));
        }
    }

    private Map<Integer, Integer> readIntermediate(BytesRef bytes) {
        Map<Integer, Integer> idMap = new HashMap<>();
        try (StreamInput in = new BytesArray(bytes).streamInput()) {
            int count = in.readVInt();
            for (int oldCategoryId = 0; oldCategoryId < count; oldCategoryId++) {
                SerializableTokenListCategory category = new SerializableTokenListCategory(in);
                int newCategoryId = categorizer.mergeWireCategory(category).getId();
                System.err.println("category id map: " + oldCategoryId + " -> " + newCategoryId + " (" + category.getRegex() + ")");
                idMap.put(oldCategoryId, newCategoryId);
            }
            return idMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return hash.nonEmpty();
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return hash.seenGroupIds(bigArrays);
    }

    @Override
    public void close() {

    }
}
