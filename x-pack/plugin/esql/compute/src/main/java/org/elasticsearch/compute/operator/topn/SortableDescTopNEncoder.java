/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;

/**
 * A {@link TopNEncoder} that encodes values to byte arrays that may be sorted directly.
 */
public abstract class SortableDescTopNEncoder implements TopNEncoder {
    @Override
    public final void encodeLong(long value, PagedBytesBuilder builder) {
        TopNEncoder.DEFAULT_SORTABLE.encodeLong(~value, builder);
    }

    @Override
    public final long decodeLong(PagedBytesCursor bytes) {
        return ~TopNEncoder.DEFAULT_SORTABLE.decodeLong(bytes);
    }

    @Override
    public final void encodeInt(int value, PagedBytesBuilder builder) {
        TopNEncoder.DEFAULT_SORTABLE.encodeInt(~value, builder);
    }

    @Override
    public final int decodeInt(PagedBytesCursor bytes) {
        return ~TopNEncoder.DEFAULT_SORTABLE.decodeInt(bytes);
    }

    @Override
    public final void encodeFloat(float value, PagedBytesBuilder builder) {
        encodeInt(NumericUtils.floatToSortableInt(value), builder);
    }

    @Override
    public final float decodeFloat(PagedBytesCursor bytes) {
        return NumericUtils.sortableIntToFloat(decodeInt(bytes));
    }

    @Override
    public final void encodeDouble(double value, PagedBytesBuilder builder) {
        encodeLong(NumericUtils.doubleToSortableLong(value), builder);
    }

    @Override
    public final double decodeDouble(PagedBytesCursor bytes) {
        return NumericUtils.sortableLongToDouble(decodeLong(bytes));
    }

    @Override
    public final void encodeBoolean(boolean value, PagedBytesBuilder builder) {
        TopNEncoder.DEFAULT_SORTABLE.encodeBoolean(value == false, builder);
    }

    @Override
    public final boolean decodeBoolean(PagedBytesCursor bytes) {
        return TopNEncoder.DEFAULT_SORTABLE.decodeBoolean(bytes) == false;
    }

    protected void bitwiseNot(byte[] bytes, int from, int to) {
        for (int i = from; i < to; i++) {
            bytes[i] = (byte) ~bytes[i];
        }
    }
}
