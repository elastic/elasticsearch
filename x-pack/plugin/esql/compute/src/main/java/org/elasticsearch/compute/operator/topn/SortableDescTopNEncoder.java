/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * A {@link TopNEncoder} that encodes values to byte arrays that may be sorted directly.
 */
public abstract class SortableDescTopNEncoder implements TopNEncoder {
    @Override
    public final void encodeLong(long value, BreakingBytesRefBuilder bytesRefBuilder) {
        TopNEncoder.DEFAULT_SORTABLE.encodeLong(~value, bytesRefBuilder);
    }

    @Override
    public final long decodeLong(BytesRef bytes) {
        return ~TopNEncoder.DEFAULT_SORTABLE.decodeLong(bytes);
    }

    @Override
    public final void encodeInt(int value, BreakingBytesRefBuilder bytesRefBuilder) {
        TopNEncoder.DEFAULT_SORTABLE.encodeInt(~value, bytesRefBuilder);
    }

    @Override
    public final int decodeInt(BytesRef bytes) {
        return ~TopNEncoder.DEFAULT_SORTABLE.decodeInt(bytes);
    }

    @Override
    public final void encodeFloat(float value, BreakingBytesRefBuilder bytesRefBuilder) {
        encodeInt(NumericUtils.floatToSortableInt(value), bytesRefBuilder);
    }

    @Override
    public final float decodeFloat(BytesRef bytes) {
        return NumericUtils.sortableIntToFloat(decodeInt(bytes));
    }

    @Override
    public final void encodeDouble(double value, BreakingBytesRefBuilder bytesRefBuilder) {
        encodeLong(NumericUtils.doubleToSortableLong(value), bytesRefBuilder);
    }

    @Override
    public final double decodeDouble(BytesRef bytes) {
        return NumericUtils.sortableLongToDouble(decodeLong(bytes));
    }

    @Override
    public final void encodeBoolean(boolean value, BreakingBytesRefBuilder bytesRefBuilder) {
        TopNEncoder.DEFAULT_SORTABLE.encodeBoolean(value == false, bytesRefBuilder);
    }

    @Override
    public final boolean decodeBoolean(BytesRef bytes) {
        return TopNEncoder.DEFAULT_SORTABLE.decodeBoolean(bytes) == false;
    }

    protected void bitwiseNot(byte[] bytes, int from, int to) {
        for (int i = from; i < to; i++) {
            bytes[i] = (byte) ~bytes[i];
        }
    }
}
