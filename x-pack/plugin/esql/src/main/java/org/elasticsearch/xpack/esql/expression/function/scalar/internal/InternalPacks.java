/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.internal;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.topn.TopNEncoder;

final class InternalPacks {
    private static final TopNEncoder ENCODER = TopNEncoder.DEFAULT_UNSORTABLE;
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
                BytesRefVector encoded = packBytesVector(driverContext, ordinals.getDictionaryVector());
                ordinals.getOrdinalsVector().incRef();
                return new OrdinalBytesRefVector(ordinals.getOrdinalsVector(), encoded).asBlock();
            } else {
                return packBytesVector(driverContext, vector).asBlock();
            }
        }
        int positionCount = raw.getPositionCount();
        try (
            BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(estimateForBytesBuilder(positionCount));
            PagedBytesBuilder work = new PagedBytesBuilder(
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                driverContext.breaker(),
                "pack_dimensions",
                1024
            )
        ) {
            BytesRef scratch = new BytesRef();
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
                    raw.getBytesRef(i, scratch);
                    ENCODER.encodeBytesRef(scratch, work);
                }
                builder.appendBytesRef(work);
            }
            return builder.build();
        }
    }

    static BytesRefVector packBytesVector(DriverContext driverContext, BytesRefVector encode) {
        int positionCount = encode.getPositionCount();
        try (
            BytesRefVector.Builder builder = driverContext.blockFactory().newBytesRefVectorBuilder(estimateForBytesBuilder(positionCount));
            PagedBytesBuilder work = new PagedBytesBuilder(
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                driverContext.breaker(),
                "pack_values",
                1024
            )
        ) {
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                ENCODER.encodeBytesRef(encode.getBytesRef(p, scratch), work);
                builder.appendBytesRef(work);
                work.clear();
            }
            return builder.build();
        }
    }

    static BytesRefBlock unpackBytesValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            PagedBytesCursor outScratch = new PagedBytesCursor();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                PagedBytesCursor cursor = PagedBytesCursor.fromBytesRef(encoded.getBytesRef(p, inScratch));
                PagedBytesCursor v = ENCODER.decodeBytesRef(cursor, outScratch);
                if (cursor.remaining() == 0) {
                    builder.append(v);
                } else {
                    builder.beginPositionEntry();
                    builder.append(v);
                    while (cursor.remaining() > 0) {
                        builder.append(ENCODER.decodeBytesRef(cursor, outScratch));
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
            BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(estimateForBytesBuilder(positionCount));
            PagedBytesBuilder work = new PagedBytesBuilder(
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                driverContext.breaker(),
                "pack_values",
                32
            )
        ) {
            for (int p = 0; p < positionCount; p++) {
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                work.clear();
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    ENCODER.encodeLong(raw.getLong(first), work);
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        ENCODER.encodeLong(raw.getLong(i), work);
                    }
                }
                builder.appendBytesRef(work);
            }
            return builder.build();
        }
    }

    static LongBlock unpackLongValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (LongBlock.Builder builder = driverContext.blockFactory().newLongBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                PagedBytesCursor cursor = PagedBytesCursor.fromBytesRef(encoded.getBytesRef(p, inScratch));
                long v = ENCODER.decodeLong(cursor);
                if (cursor.remaining() == 0) {
                    builder.appendLong(v);
                } else {
                    builder.beginPositionEntry();
                    builder.appendLong(v);
                    while (cursor.remaining() > 0) {
                        builder.appendLong(ENCODER.decodeLong(cursor));
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
            BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(estimateForBytesBuilder(positionCount));
            PagedBytesBuilder work = new PagedBytesBuilder(
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                driverContext.breaker(),
                "pack_values",
                32
            )
        ) {
            for (int p = 0; p < positionCount; p++) {
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                work.clear();
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    ENCODER.encodeInt(raw.getInt(first), work);
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        ENCODER.encodeInt(raw.getInt(i), work);
                    }
                }
                builder.appendBytesRef(work);
            }
            return builder.build();
        }
    }

    static IntBlock unpackIntValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (IntBlock.Builder builder = driverContext.blockFactory().newIntBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                PagedBytesCursor cursor = PagedBytesCursor.fromBytesRef(encoded.getBytesRef(p, inScratch));
                int v = ENCODER.decodeInt(cursor);
                if (cursor.remaining() == 0) {
                    builder.appendInt(v);
                } else {
                    builder.beginPositionEntry();
                    builder.appendInt(v);
                    while (cursor.remaining() > 0) {
                        builder.appendInt(ENCODER.decodeInt(cursor));
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
            BytesRefBlock.Builder builder = driverContext.blockFactory().newBytesRefBlockBuilder(positionCount);
            PagedBytesBuilder work = new PagedBytesBuilder(
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                driverContext.breaker(),
                "pack_values",
                32
            )
        ) {
            for (int p = 0; p < positionCount; p++) {
                work.clear();
                int valueCount = raw.getValueCount(p);
                if (valueCount == 0) {
                    builder.appendNull();
                    continue;
                }
                int first = raw.getFirstValueIndex(p);
                if (valueCount == 1) {
                    ENCODER.encodeBoolean(raw.getBoolean(first), work);
                } else {
                    int end = first + valueCount;
                    for (int i = first; i < end; i++) {
                        ENCODER.encodeBoolean(raw.getBoolean(i), work);
                    }
                }
                builder.appendBytesRef(work);
            }
            return builder.build();
        }
    }

    static BooleanBlock unpackBooleanValues(DriverContext driverContext, BytesRefBlock encoded) {
        int positionCount = encoded.getPositionCount();
        try (BooleanBlock.Builder builder = driverContext.blockFactory().newBooleanBlockBuilder(positionCount)) {
            BytesRef inScratch = new BytesRef();
            for (int p = 0; p < positionCount; p++) {
                if (encoded.isNull(p)) {
                    builder.appendNull();
                    continue;
                }
                PagedBytesCursor cursor = PagedBytesCursor.fromBytesRef(encoded.getBytesRef(p, inScratch));
                boolean v = ENCODER.decodeBoolean(cursor);
                if (cursor.remaining() == 0) {
                    builder.appendBoolean(v);
                } else {
                    builder.beginPositionEntry();
                    builder.appendBoolean(v);
                    while (cursor.remaining() > 0) {
                        builder.appendBoolean(ENCODER.decodeBoolean(cursor));
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }
}
