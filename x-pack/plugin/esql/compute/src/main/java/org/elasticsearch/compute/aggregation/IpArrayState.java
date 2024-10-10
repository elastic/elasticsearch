/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Aggregator state for an array of IPs. It is created in a mode where it
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
 *     This class is a specialized version of the {@code X-ArrayState.java.st} template.
 * </p>
 */
public final class IpArrayState extends AbstractArrayState implements GroupingAggregatorState {
    private static final int IP_LENGTH = 16;

    private final byte[] init;

    private ByteArray values;

    IpArrayState(BigArrays bigArrays, BytesRef init) {
        super(bigArrays);
        assert init.length == IP_LENGTH;
        this.values = bigArrays.newByteArray(IP_LENGTH, false);
        this.init = new byte[IP_LENGTH];

        System.arraycopy(init.bytes, init.offset, this.init, 0, IP_LENGTH);
        this.values.set(0, this.init, 0, IP_LENGTH);
    }

    BytesRef get(int groupId, BytesRef scratch) {
        var ipIndex = getIndex(groupId);
        values.get(ipIndex, IP_LENGTH, scratch);
        return scratch;
    }

    BytesRef getOrDefault(int groupId, BytesRef scratch) {
        var ipIndex = getIndex(groupId);
        if (ipIndex + IP_LENGTH <= values.size()) {
            values.get(ipIndex, IP_LENGTH, scratch);
        } else {
            scratch.bytes = init;
            scratch.offset = 0;
            scratch.length = IP_LENGTH;
        }
        return scratch;
    }

    void set(int groupId, BytesRef ip) {
        assert ip.length == IP_LENGTH;
        ensureCapacity(groupId);
        var ipIndex = getIndex(groupId);
        values.set(ipIndex, ip.bytes, ip.offset, ip.length);
        trackGroupId(groupId);
    }

    Block toValuesBlock(IntVector selected, DriverContext driverContext) {
        var scratch = new BytesRef();
        if (false == trackingGroupIds()) {
            try (var builder = driverContext.blockFactory().newBytesRefVectorBuilder(selected.getPositionCount())) {
                for (int i = 0; i < selected.getPositionCount(); i++) {
                    int group = selected.getInt(i);
                    var value = get(group, scratch);
                    builder.appendBytesRef(value);
                }
                return builder.build().asBlock();
            }
        }
        try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                if (hasValue(group)) {
                    var value = get(group, scratch);
                    builder.appendBytesRef(value);
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    private void ensureCapacity(int groupId) {
        var minIpIndex = getIndex(groupId);
        var minSize = minIpIndex + IP_LENGTH;
        if (minSize > values.size()) {
            long prevSize = values.size();
            values = bigArrays.grow(values, minSize);
            var prevLastIpIndex = prevSize - prevSize % IP_LENGTH;
            var lastIpIndex = values.size() - values.size() % IP_LENGTH;
            for (long i = prevLastIpIndex; i < lastIpIndex; i += IP_LENGTH) {
                values.set(i, init, 0, IP_LENGTH);
            }
        }
    }

    /** Extracts an intermediate view of the contents of this state.  */
    @Override
    public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        assert blocks.length >= offset + 2;
        try (
            var valuesBuilder = driverContext.blockFactory().newBytesRefBlockBuilder(selected.getPositionCount());
            var hasValueBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())
        ) {
            var scratch = new BytesRef();
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                int ipIndex = getIndex(group);
                if (ipIndex < values.size()) {
                    var value = get(group, scratch);
                    valuesBuilder.appendBytesRef(value);
                } else {
                    scratch.length = 0;
                    valuesBuilder.appendBytesRef(scratch); // TODO can we just use null?
                }
                hasValueBuilder.appendBoolean(i, hasValue(group));
            }
            blocks[offset] = valuesBuilder.build();
            blocks[offset + 1] = hasValueBuilder.build().asBlock();
        }
    }

    @Override
    public void close() {
        Releasables.close(values, super::close);
    }

    /**
     * Returns the index of the ip at {@code groupId}
     */
    private int getIndex(int groupId) {
        return groupId * IP_LENGTH;
    }
}
