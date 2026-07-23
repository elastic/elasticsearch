/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.internal;

import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.operator.DriverContext;

final class InternalPacks {
    public static final int INITIAL_SIZE_IN_BYTES = 6 * 1024;

    static int estimateForBytesBuilder(int positionCount) {
        // allocate at least one page for the bytes block builder to avoid copying during resizing
        return Math.max(INITIAL_SIZE_IN_BYTES, positionCount);
    }

    static BytesRefBlock packBytesValues(DriverContext driverContext, BytesRefBlock raw) {
        BytesRefVector vector = raw.asVector();
        if (vector != null) {
            OrdinalBytesRefVector ordinals = vector.asOrdinals();
            if (ordinals != null) {
                var encoded = packBytesVector(driverContext, ordinals.getDictionaryVector());
                ordinals.getOrdinalsVector().incRef();
                return new OrdinalBytesRefVector(ordinals.getOrdinalsVector(), encoded).asBlock();
            } else {
                return packBytesVector(driverContext, vector).asBlock();
            }
        }
        int positionCount = raw.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefBlockBuilder(estimateForBytesBuilder(positionCount));
            var work = new PagedBytesBuilder(
                driverContext.blockFactory().bigArrays().recycler(),
                driverContext.breaker(),
                "pack_dimensions",
                1024
            )
        ) {
            PagedBytesCursor valueCursor = new PagedBytesCursor();
            PagedBytesCursor scratch = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                work.clear();
                int first = raw.getFirstValueIndex(p);
                int end = first + valueCount;
                for (int i = first; i < end; i++) {
                    raw.get(i, valueCursor);
                    work.appendLengthPrefixed(valueCursor);
                }
                builder.append(work.view(scratch));
            }
            return builder.build();
        }
    }

    static BytesRefVector packBytesVector(DriverContext driverContext, BytesRefVector encode) {
        int positionCount = encode.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefVectorBuilder(estimateForBytesBuilder(positionCount));
            var work = new PagedBytesBuilder(
                driverContext.blockFactory().bigArrays().recycler(),
                driverContext.breaker(),
                "pack_values",
                1024
            )
        ) {
            PagedBytesCursor valueCursor = new PagedBytesCursor();
            PagedBytesCursor scratch = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                encode.get(p, valueCursor);
                work.appendLengthPrefixed(valueCursor);
                builder.append(work.view(scratch));
                work.clear();
            }
            return builder.build();
        }
    }

    static BytesRefBlock unpackBytesValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (var builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            PagedBytesCursor cursor = new PagedBytesCursor();
            PagedBytesCursor sliceScratch = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                encoded.get(p, cursor);
                PagedBytesCursor slice = cursor.readLengthPrefixed(sliceScratch);
                if (cursor.remaining() == 0) {
                    builder.append(slice);
                } else {
                    builder.beginPositionEntry();
                    builder.append(slice);
                    while (cursor.remaining() > 0) {
                        slice = cursor.readLengthPrefixed(sliceScratch);
                        builder.append(slice);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefBlock packLongValues(DriverContext driverContext, LongBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefBlockBuilder(estimateForBytesBuilder(positionCount));
            var work = new PagedBytesBuilder(
                driverContext.blockFactory().bigArrays().recycler(),
                driverContext.breaker(),
                "pack_values",
                32
            )
        ) {
            PagedBytesCursor scratch = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                work.clear();
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    work.append(raw.getLong(first));
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        work.append(raw.getLong(i));
                    }
                }
                builder.append(work.view(scratch));
            }
            return builder.build();
        }
    }

    static LongBlock unpackLongValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (var builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
            PagedBytesCursor cursor = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                encoded.get(p, cursor);
                long v = cursor.readLong();
                if (cursor.remaining() == 0) {
                    builder.appendLong(v);
                } else {
                    builder.beginPositionEntry();
                    builder.appendLong(v);
                    while (cursor.remaining() > 0) {
                        builder.appendLong(cursor.readLong());
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefBlock packIntValues(DriverContext driverContext, IntBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefBlockBuilder(estimateForBytesBuilder(positionCount));
            var work = new PagedBytesBuilder(
                driverContext.blockFactory().bigArrays().recycler(),
                driverContext.breaker(),
                "pack_values",
                32
            )
        ) {
            PagedBytesCursor scratch = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                work.clear();
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    work.append(raw.getInt(first));
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        work.append(raw.getInt(i));
                    }
                }
                builder.append(work.view(scratch));
            }
            return builder.build();
        }
    }

    static IntBlock unpackIntValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
            PagedBytesCursor cursor = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                encoded.get(p, cursor);
                int v = cursor.readInt();
                if (cursor.remaining() == 0) {
                    builder.appendInt(v);
                } else {
                    builder.beginPositionEntry();
                    builder.appendInt(v);
                    while (cursor.remaining() > 0) {
                        builder.appendInt(cursor.readInt());
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    static BytesRefBlock packBooleanValues(DriverContext driverContext, BooleanBlock raw) {
        int positionCount = raw.getPositionCount();
        try (
            var builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount);
            var work = new PagedBytesBuilder(
                driverContext.blockFactory().bigArrays().recycler(),
                driverContext.breaker(),
                "pack_values",
                32
            )
        ) {
            PagedBytesCursor scratch = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    work.append(raw.getBoolean(first) ? (byte) 1 : (byte) 0);
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        work.append(raw.getBoolean(i) ? (byte) 1 : (byte) 0);
                    }
                }
                builder.append(work.view(scratch));
            }
            return builder.build();
        }
    }

    static BooleanBlock unpackBooleanValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (var builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            PagedBytesCursor cursor = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                encoded.get(p, cursor);
                boolean v = cursor.readByte() == 1;
                if (cursor.remaining() == 0) {
                    builder.appendBoolean(v);
                } else {
                    builder.beginPositionEntry();
                    builder.appendBoolean(v);
                    while (cursor.remaining() > 0) {
                        builder.appendBoolean(cursor.readByte() == 1);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }
}
