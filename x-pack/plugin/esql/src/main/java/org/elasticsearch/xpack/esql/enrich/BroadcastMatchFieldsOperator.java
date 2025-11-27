/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;

/**
 * Operator that broadcasts matchFields from the inputPage to each output Page
 * using the Positions block at the specified channel to determine which left-hand row each right-hand row corresponds to.
 * This is similar to how RightChunkedLeftJoin broadcasts left-hand blocks.
 */
public class BroadcastMatchFieldsOperator extends AbstractPageMappingOperator {
    private final DriverContext driverContext;
    private final Page inputPage;
    private final List<MatchConfig> matchFields;
    private final int positionsChannel;

    public BroadcastMatchFieldsOperator(DriverContext driverContext, Page inputPage, List<MatchConfig> matchFields, int positionsChannel) {
        this.driverContext = driverContext;
        this.inputPage = inputPage;
        this.matchFields = matchFields;
        this.positionsChannel = positionsChannel;
    }

    @Override
    protected Page process(Page page) {
        // Extract Positions block from the specified channel
        IntBlock positionsBlock = page.<IntBlock>getBlock(positionsChannel);
        IntVector positions = positionsBlock.asVector();

        Block[] newBlocks = new Block[matchFields.size()];
        BlockFactory blockFactory = driverContext.blockFactory();
        int positionCount = positionsBlock.getPositionCount();

        if (positions == null) {
            // Multivalued positions - use the first value index for each position
            for (int i = 0; i < matchFields.size(); i++) {
                MatchConfig matchField = matchFields.get(i);
                Block inputBlock = inputPage.getBlock(matchField.channel());
                Block.Builder builder = PlannerUtils.toElementType(matchField.type()).newBlockBuilder(positionCount, blockFactory);
                for (int p = 0; p < positionCount; p++) {
                    int firstValueIndex = positionsBlock.getFirstValueIndex(p);
                    int valueCount = positionsBlock.getValueCount(p);
                    if (valueCount > 0) {
                        // Use the first position value
                        int pos = positionsBlock.getInt(firstValueIndex);
                        builder.copyFrom(inputBlock, pos, pos + 1);
                    } else {
                        builder.appendNull();
                    }
                }
                newBlocks[i] = builder.build();
            }
        } else {
            // Single-valued positions - extract position array
            int[] positionArray = new int[positions.getPositionCount()];
            for (int i = 0; i < positions.getPositionCount(); i++) {
                positionArray[i] = positions.getInt(i);
            }
            // Filter/broadcast matchFields from inputPage
            // We need to copy into new blocks using driverContext.blockFactory() to avoid thread safety issues
            // We manually copy selected positions instead of using filter() to avoid accessing the wrong thread's circuit breaker
            for (int i = 0; i < matchFields.size(); i++) {
                MatchConfig matchField = matchFields.get(i);
                Block inputBlock = inputPage.getBlock(matchField.channel());
                // Manually copy selected positions into a new block using driverContext's block factory
                Block.Builder builder = PlannerUtils.toElementType(matchField.type()).newBlockBuilder(positionArray.length, blockFactory);
                for (int pos : positionArray) {
                    builder.copyFrom(inputBlock, pos, pos + 1);
                }
                newBlocks[i] = builder.build();
            }
        }

        // Use appendBlocks to create a new Page with matchFields appended
        // This handles reference counting automatically
        return page.appendBlocks(newBlocks);
    }

    @Override
    public String toString() {
        return "BroadcastMatchFieldsOperator[matchFields=" + matchFields.size() + ", positionsChannel=" + positionsChannel + "]";
    }
}
