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
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.ml.aggs.categorization.SerializableTokenListCategory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CategorizedIntermediateBlockHash extends AbstractCategorizeBlockHash {

    CategorizedIntermediateBlockHash(int channel, BlockFactory blockFactory, boolean outputPartial) {
        super(blockFactory, channel, outputPartial);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        BytesRefBlock categorizerState = page.getBlock(channel());
        Map<Integer, Integer> idMap;
        if (categorizerState.areAllValuesNull() == false) {
            idMap = readIntermediate(categorizerState.getBytesRef(0, new BytesRef()));
        } else {
            idMap = Collections.emptyMap();
        }
        try (IntBlock.Builder newIdsBuilder = blockFactory.newIntBlockBuilder(idMap.size())) {
            for (int i = 0; i < idMap.size(); i++) {
                newIdsBuilder.appendInt(idMap.get(i));
            }
            try (IntBlock newIds = newIdsBuilder.build()) {
                addInput.add(0, newIds);
            }
        }
    }

    private Map<Integer, Integer> readIntermediate(BytesRef bytes) {
        Map<Integer, Integer> idMap = new HashMap<>();
        try (StreamInput in = new BytesArray(bytes).streamInput()) {
            int count = in.readVInt();
            for (int oldCategoryId = 0; oldCategoryId < count; oldCategoryId++) {
                int newCategoryId = categorizer.mergeWireCategory(new SerializableTokenListCategory(in)).getId();
                idMap.put(oldCategoryId, newCategoryId);
            }
            return idMap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        categorizer.close();
    }
}
