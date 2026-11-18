/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;

/**
 * Visits group ids from a hash build without depending on the backing type of each batch.
 */
abstract class BlockHashGroupIdsAddInput implements GroupingAggregatorFunction.AddInput {
    @Override
    public final void add(int positionOffset, IntArrayBlock groupIds) {
        addBlock(positionOffset, groupIds);
    }

    @Override
    public final void add(int positionOffset, IntBigArrayBlock groupIds) {
        addBlock(positionOffset, groupIds);
    }

    @Override
    public final void add(int positionOffset, IntVector groupIds) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            addGroupId(positionOffset + p, groupIds.getInt(p));
        }
    }

    private void addBlock(int positionOffset, IntBlock groupIds) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            int start = groupIds.getFirstValueIndex(p);
            int end = start + groupIds.getValueCount(p);
            for (int i = start; i < end; i++) {
                addGroupId(positionOffset + p, groupIds.getInt(i));
            }
        }
    }

    protected abstract void addGroupId(int row, int groupId);

    @Override
    public final void close() {}
}
