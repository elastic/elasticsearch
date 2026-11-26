/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.core.Nullable;

/**
 * Collects the group id of each position from a hash that puts every position in exactly one group.
 * With {@code positions}, the id of position {@code p} is written at {@code positions[p]}.
 */
final class PositionGroupIds implements GroupingAggregatorFunction.AddInput {
    private final int[] groupIds;
    @Nullable
    private final int[] positions;

    PositionGroupIds(int[] groupIds, @Nullable int[] positions) {
        this.groupIds = groupIds;
        this.positions = positions;
    }

    @Override
    public void add(int positionOffset, IntVector ids) {
        for (int i = 0; i < ids.getPositionCount(); i++) {
            set(positionOffset + i, ids.getInt(i));
        }
    }

    @Override
    public void add(int positionOffset, IntArrayBlock ids) {
        addBlock(positionOffset, ids);
    }

    @Override
    public void add(int positionOffset, IntBigArrayBlock ids) {
        addBlock(positionOffset, ids);
    }

    private void addBlock(int positionOffset, IntBlock ids) {
        for (int i = 0; i < ids.getPositionCount(); i++) {
            if (ids.getValueCount(i) != 1) {
                throw new IllegalStateException("expected one group per position but got [" + ids.getValueCount(i) + "]");
            }
            set(positionOffset + i, ids.getInt(ids.getFirstValueIndex(i)));
        }
    }

    private void set(int position, int groupId) {
        groupIds[positions == null ? position : positions[position]] = groupId;
    }

    @Override
    public void close() {}
}
