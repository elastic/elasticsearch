/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public record GroupingKey(int channel, AggregatorMode mode, Thing thing) {
    public interface Thing {
        int intermediateBlockCount();

        ElementType intermediateElementType();

        ElementType finalElementType();

        void fetchIntermediateState(BlockFactory blockFactory, Block[] blocks, int offset, int positionCount);

        void receiveIntermediateState(Page page, int offset);

        void replaceIntermediateKeys(BlockFactory blockFactory, Block[] blocks, int offset);
    }

    public interface Supplier {
        GroupingKey get(AggregatorMode mode);
    }

    public static GroupingKey.Supplier forStatelessGrouping(int channel, ElementType elementType) {
        return mode -> new GroupingKey(channel, mode, new Thing() {
            @Override
            public int intermediateBlockCount() {
                return 0;
            }

            @Override
            public ElementType intermediateElementType() {
                return elementType;
            }

            @Override
            public ElementType finalElementType() {
                return elementType;
            }

            @Override
            public void receiveIntermediateState(Page page, int offset) {}

            @Override
            public void fetchIntermediateState(BlockFactory blockFactory, Block[] blocks, int offset, int positionCount) {}

            @Override
            public void replaceIntermediateKeys(BlockFactory blockFactory, Block[] blocks, int offset) {}

            @Override
            public String toString() {
                return "Stateless";
            }
        });
    }

    public static List<BlockHash.GroupSpec> toBlockHashGroupSpec(List<GroupingKey> keys) {
        return keys.stream().map(GroupingKey::toBlockHashSpec).toList();
    }

    public BlockHash.GroupSpec toBlockHashSpec() {
        return new BlockHash.GroupSpec(channel, elementType()); // NOCOMMIT this should probably be an evaluator and a BlockType
    }

    public ElementType elementType() {
        return mode.isOutputPartial() ? thing.intermediateElementType() : thing.finalElementType();
    }

    public void receive(Page page, int offset) {
        if (mode.isInputPartial()) {
            thing.receiveIntermediateState(page, offset);
        }
    }

    public int evaluateBlockCount() {
        return 1 + (mode.isOutputPartial() ? thing.intermediateBlockCount() : 0);
    }

    public void evaluate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        if (mode.isOutputPartial()) {
            thing.fetchIntermediateState(driverContext.blockFactory(), blocks, offset + 1, selected.getPositionCount());
        } else {
            thing.replaceIntermediateKeys(driverContext.blockFactory(), blocks, offset);
        }
    }
}
