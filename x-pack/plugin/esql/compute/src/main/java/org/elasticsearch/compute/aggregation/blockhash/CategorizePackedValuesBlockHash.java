/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.AnalysisRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * BlockHash implementation for {@code Categorize} grouping function as first
 * grouping expression, followed by one or mode other grouping expressions.
 */
public class CategorizePackedValuesBlockHash extends BlockHash {

    private final List<GroupSpec> specs;
    private final AggregatorMode aggregatorMode;
    private final CategorizeBlockHash categorizeBlockHash;
    private final PackedValuesBlockHash packedValuesBlockHash;

    CategorizePackedValuesBlockHash(
        List<GroupSpec> specs,
        BlockFactory blockFactory,
        AggregatorMode aggregatorMode,
        AnalysisRegistry analysisRegistry,
        int emitBatchSize
    ) {
        super(blockFactory);
        this.specs = specs;
        this.aggregatorMode = aggregatorMode;

        List<GroupSpec> delegateSpecs = new ArrayList<>();
        delegateSpecs.add(new GroupSpec(0, ElementType.INT));
        for (int i = 1; i < specs.size(); i++) {
            delegateSpecs.add(new GroupSpec(i, specs.get(i).elementType()));
        }

        boolean success = false;
        try {
            categorizeBlockHash = new CategorizeBlockHash(blockFactory, specs.get(0).channel(), aggregatorMode, analysisRegistry);
            packedValuesBlockHash = new PackedValuesBlockHash(delegateSpecs, blockFactory, emitBatchSize);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        try (IntBlock categories = getCategories(page)) {
            Block[] blocks = new Block[specs.size()];
            blocks[0] = categories;
            for (int i = 1; i < specs.size(); i++) {
                blocks[i] = page.getBlock(specs.get(i).channel());
            }
            packedValuesBlockHash.add(new Page(blocks), addInput);
        }
    }

    private IntBlock getCategories(Page page) {
        if (aggregatorMode.isInputPartial() == false) {
            return categorizeBlockHash.addInitial(page);
        } else {
            BytesRefBlock stateBlock = page.getBlock(0);
            BytesRef stateBytes = stateBlock.getBytesRef(0, new BytesRef());

            try (StreamInput in = new BytesArray(stateBytes).streamInput()) {
                BytesRef categorizerState = in.readBytesRef();
                Map<Integer, Integer> idMap = categorizeBlockHash.readIntermediate(categorizerState);
                int[] oldIds = in.readIntArray();
                try (IntBlock.Builder newIds = blockFactory.newIntBlockBuilder(page.getPositionCount())) {
                    for (int oldId : oldIds) {
                        newIds.appendInt(idMap.get(oldId));
                    }
                    return newIds.build();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Block[] getKeys() {
        Block[] keys = packedValuesBlockHash.getKeys();
        if (aggregatorMode.isOutputPartial() == false) {
            try (
                BytesRefBlock regexes = (BytesRefBlock) categorizeBlockHash.getKeys()[0];
                BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(keys[0].getPositionCount())
            ) {
                IntVector idsVector = (IntVector) keys[0].asVector();
                int idsOffset = categorizeBlockHash.seenNull() ? 0 : -1;
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < idsVector.getPositionCount(); i++) {
                    int id = idsVector.getInt(i);
                    if (id == 0) {
                        builder.appendNull();
                    } else {
                        builder.appendBytesRef(regexes.getBytesRef(id + idsOffset, scratch));
                    }
                }
                keys[0].close();
                keys[0] = builder.build();
            }
        } else {
            BytesRef state;
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeBytesRef(categorizeBlockHash.serializeCategorizer());
                IntVector idsVector = (IntVector) keys[0].asVector();
                int[] idsArray = new int[idsVector.getPositionCount()];
                for (int i = 0; i < idsVector.getPositionCount(); i++) {
                    idsArray[i] = idsVector.getInt(i);
                }
                out.writeIntArray(idsArray);
                state = out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            keys[0].close();
            keys[0] = blockFactory.newConstantBytesRefBlockWith(state, keys[0].getPositionCount());
        }
        return keys;
    }

    @Override
    public IntVector nonEmpty() {
        return packedValuesBlockHash.nonEmpty();
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return packedValuesBlockHash.seenGroupIds(bigArrays);
    }

    @Override
    public final ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        Releasables.close(categorizeBlockHash, packedValuesBlockHash);
    }
}
