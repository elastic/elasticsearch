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
import org.elasticsearch.compute.operator.BreakingBytesStreamOutput;
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
    private static final String CIRCUIT_BREAKER_LABEL = "CategorizePackedValuesBlockHash";

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
        assert specs.get(0).categorizeDef() != null;

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
            categorizeBlockHash = new CategorizeBlockHash(
                blockFactory,
                specs.get(0).channel(),
                aggregatorMode,
                specs.get(0).categorizeDef(),
                analysisRegistry
            );
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
    public Block[] getKeys(IntVector selected) {
        Block[] keys = packedValuesBlockHash.getKeys(selected);
        boolean success = false;
        try {
            Block categoryKeys = aggregatorMode.isOutputPartial()
                ? buildIntermediateCategoryKeys((IntVector) keys[0].asVector())
                : categorizeBlockHash.getKeys((IntVector) keys[0].asVector())[0];
            keys[0].close();
            keys[0] = categoryKeys;
            success = true;
            return keys;
        } finally {
            if (success == false) {
                Releasables.close(keys);
            }
        }
    }

    /**
     * For intermediate output, the keys are the delegate PackedValuesBlockHash's
     * keys, with the category IDs replaced by the categorizer's internal state
     * together with the list of category IDs.
     */
    private Block buildIntermediateCategoryKeys(IntVector ids) {
        try (BreakingBytesStreamOutput out = new BreakingBytesStreamOutput(blockFactory.breaker(), CIRCUIT_BREAKER_LABEL)) {
            try (
                BreakingBytesStreamOutput categorizerState = new BreakingBytesStreamOutput(blockFactory.breaker(), CIRCUIT_BREAKER_LABEL)
            ) {
                categorizeBlockHash.serializeCategorizer(categorizerState);
                out.writeBytesRef(categorizerState.bytesRefView());
            }
            ids.writeTo(out);
            return blockFactory.newConstantBytesRefBlockWith(out.bytesRefView(), ids.getPositionCount());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IntVector nonEmpty() {
        return packedValuesBlockHash.nonEmpty();
    }

    @Override
    public int numKeys() {
        return packedValuesBlockHash.numKeys();
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
