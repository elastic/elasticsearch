/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
$if(boolean)$
import org.elasticsearch.common.util.BitArray;
$else$
import org.elasticsearch.common.util.$Type$Array;
$endif$
import org.elasticsearch.compute.data.Block;
$if(long)$
import org.elasticsearch.compute.data.IntVector;
$endif$
import org.elasticsearch.compute.data.$Type$Block;
$if(int)$
import org.elasticsearch.compute.data.$Type$Vector;
$endif$
$if(boolean||double||float)$
import org.elasticsearch.compute.data.IntVector;
$endif$
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Aggregator state for an array of $type$s. It is created in a mode where it
 * won't track the {@code groupId}s that are sent to it and it is the
 * responsibility of the caller to only fetch values for {@code groupId}s
 * that it has sent using the {@code selected} parameter when building the
 * results. This is fine when there are no {@code null} values in the input
 * data. But once there are null values in the input data it is
 * <strong>much</strong> more convenient to only send non-null values and
 * the tracking built into the grouping code can't track that. In that case
 * call {@link #enableGroupIdTracking} to transition the state into a mode
 * where it'll track which {@code groupIds} have been written.
 * <p>
 * This class is generated. Do not edit it.
 * </p>
 */
final class $Type$ArrayState extends AbstractArrayState implements GroupingAggregatorState {
    private final $type$ init;

$if(boolean)$
    private BitArray values;
    private int size;

$else$
    private $Type$Array values;
$endif$

    $Type$ArrayState(BigArrays bigArrays, $type$ init) {
        super(bigArrays);
$if(boolean)$
        this.values = new BitArray(1, bigArrays);
        this.size = 1;
$else$
        this.values = bigArrays.new$Type$Array(1, false);
$endif$
        this.values.set(0, init);
        this.init = init;
    }

    $type$ get(int groupId) {
        return values.get(groupId);
    }

    $type$ getOrDefault(int groupId) {
$if(boolean)$
        return groupId < size ? values.get(groupId) : init;
$else$
        return groupId < values.size() ? values.get(groupId) : init;
$endif$
    }

    void set(int groupId, $type$ value) {
        ensureCapacity(groupId);
        values.set(groupId, value);
        trackGroupId(groupId);
    }

$if(long)$
    void increment(int groupId, long value) {
        ensureCapacity(groupId);
        values.increment(groupId, value);
        trackGroupId(groupId);
    }
$endif$

    Block toValuesBlock(org.elasticsearch.compute.data.IntVector selected, DriverContext driverContext) {
        if (false == trackingGroupIds()) {
            try (var builder = driverContext.blockFactory().new$Type$VectorFixedBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    builder.append$Type$(i, values.get(selected.getInt(i)));
                }
                return builder.build().asBlock();
            }
        }
        try ($Type$Block.Builder builder = driverContext.blockFactory().new$Type$BlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    builder.append$Type$(values.get(group));
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private void ensureCapacity(int groupId) {
$if(boolean)$
        if (groupId >= size) {
            values.fill(size, groupId + 1, init);
            size = groupId + 1;
        }
$else$
        if (groupId >= values.size()) {
            long prevSize = values.size();
            values = bigArrays.grow(values, groupId + 1);
            values.fill(prevSize, values.size(), init);
        }
$endif$
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(
        Block[] blocks,
        int offset,
        IntVector selected,
        org.elasticsearch.compute.operator.DriverContext driverContext
    ) {
        assert blocks.length >= offset + 2;
        try (
            var valuesBuilder = driverContext.blockFactory().new$Type$BlockBuilder(selected.getPositionCount());
            var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (group < $if(boolean)$size$else$values.size()$endif$) {
                    valuesBuilder.append$Type$(values.get(group));
                } else {
                    valuesBuilder.append$Type$($if(boolean)$false$else$0$endif$); // TODO can we just use null?
                }
                hasValueBuilder.appendBoolean(i, hasValue(group));
            }
            blocks[offset + 0] = valuesBuilder.build();
            blocks[offset + 1] = hasValueBuilder.build().asBlock();
        }
    }

    @Override
    public void close() {
        Releasables.close(values, super::close);
    }
}
