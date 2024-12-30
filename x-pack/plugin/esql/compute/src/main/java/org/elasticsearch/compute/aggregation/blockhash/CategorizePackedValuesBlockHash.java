/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
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

/**
 * BlockHash implementation for {@code Categorize} grouping function as first
 * grouping expression, followed by one or mode other grouping expressions.
 * <p>
 * For the first grouping (the {@code Categorize} grouping function), a
 * {@code CategorizeBlockHash} is used, which outputs integers (category IDs).
 * Next, a {@code PackedValuesBlockHash} is used on the category IDs and the
 * other groupings (which are not {@code Categorize}s).
 */
public class CategorizePackedValuesBlockHash extends BlockHash {

    private final List<GroupSpec> specs;
    private final AggregatorMode aggregatorMode;
    private final Block[] blocks;
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
        blocks = new Block[specs.size()];

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
                try (IntVector ids = IntVector.readFrom(blockFactory, in)) {
                    return categorizeBlockHash.recategorize(categorizerState, ids).asBlock();
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
            // For final output, the keys are the category regexes.
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
            // For intermediate output, the keys are the delegate PackedValuesBlockHash's
            // keys, with the category IDs replaced by the categorizer's internal state
            // together with the list of category IDs.
            Long copyEstimate = null;
            try (ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(blockFactory.bigArrays())) {
                CategorizeBlockHash.SerializedCategorizer serializedCategorizer = null;
                try {
                    serializedCategorizer = categorizeBlockHash.serializeCategorizer();
                    out.writeBytesRef(serializedCategorizer.bytesRef());
                } finally {
                    if (serializedCategorizer != null) {
                        blockFactory.breaker().addWithoutBreaking(-serializedCategorizer.preAdjustedBytes());
                    }
                }
                ((IntVector) keys[0].asVector()).writeTo(out);
                // Converting a BytesStreamOutput to a BytesRef may copy the bytes anyway for bigger paged arrays,
                // so we're using ReleasableBytesStreamOutput instead and copying it ourselves
                copyEstimate = (long) out.size();
                blockFactory.breaker().addEstimateBytesAndMaybeBreak(copyEstimate, "CategorizePackedValuesBlockHash getKeys");
                BytesRef state = out.copyBytes().toBytesRef();
                keys[0].close();
                keys[0] = blockFactory.newConstantBytesRefBlockWith(state, keys[0].getPositionCount(), copyEstimate);
                copyEstimate = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if (copyEstimate != null) {
                    blockFactory.breaker().addWithoutBreaking(-copyEstimate);
                }
            }
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
