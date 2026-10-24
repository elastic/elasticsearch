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

abstract class BlockHashGroupIdsAddInput implements GroupingAggregatorFunction.AddInput {
    @Override
    public final void add(int positionOffset, IntArrayBlock groupIds) {
        processBlock(positionOffset, groupIds);
    }

    @Override
    public final void add(int positionOffset, IntBigArrayBlock groupIds) {
        processBlock(positionOffset, groupIds);
    }

    @Override
    public final void add(int positionOffset, IntVector groupIds) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            processGroupId(positionOffset + p, groupIds.getInt(p));
        }
    }

    protected void processPosition(int row, IntBlock groupIds, int firstValueIndex, int valueCount) {
        int end = firstValueIndex + valueCount;
        for (int i = firstValueIndex; i < end; i++) {
            processGroupId(row, groupIds.getInt(i));
        }
    }

    protected abstract void processGroupId(int row, int groupId);

    @Override
    public final void close() {}

    private void processBlock(int positionOffset, IntBlock groupIds) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            processPosition(positionOffset + p, groupIds, groupIds.getFirstValueIndex(p), groupIds.getValueCount(p));
        }
    }
}
