/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.FieldArrayContext;

import java.io.IOException;
import java.util.function.IntConsumer;

/**
 * Helpers shared by the offsets-aware {@code ArrayOrder} readers across the numeric, ords, and binary families.
 */
final class OffsetsAwareBlockLoaderHelper {

    private OffsetsAwareBlockLoaderHelper() {}

    /**
     * Decode the {@code <field>.offsets} companion entry for the given doc. Returns {@code null} when the doc has no offsets entry.
     */
    static int[] readOffsets(SortedDocValues offsets, ByteArrayStreamInput scratch, int doc) throws IOException {
        if (offsets.advanceExact(doc) == false) {
            return null;
        }
        BytesRef encoded = offsets.lookupOrd(offsets.ordValue());
        scratch.reset(encoded.bytes, encoded.offset, encoded.length);
        return FieldArrayContext.parseOffsetArray(scratch);
    }

    /**
     * Emit one position, picking the right shape (null / lone value / multi-value position entry, all in arrival order) based on
     * {@code offsetToOrd}. The caller-supplied {@code emitOrd} appends the value for a given ord. Null slots in the offsets array are
     * dropped because ESQL blocks have no inline-null representation.
     */
    static void emit(int[] offsetToOrd, BlockLoader.Builder builder, IntConsumer emitOrd) {
        // builder API requires picking the position shape (null / single-valued / multi-valued) before the first append, so precount
        int nonNullCount = 0;
        for (int ord : offsetToOrd) {
            if (ord != FieldArrayContext.NULL_ORD) {
                nonNullCount++;
            }
        }

        // every slot is null — emit a single null position
        if (nonNullCount == 0) {
            builder.appendNull();
            return;
        }

        // exactly one non-null slot — emit the lone value without a position entry
        if (nonNullCount == 1) {
            for (int ord : offsetToOrd) {
                if (ord != FieldArrayContext.NULL_ORD) {
                    emitOrd.accept(ord);
                    return;
                }
            }
        }

        // multiple non-null slots — emit them in arrival order, skipping nulls
        builder.beginPositionEntry();
        for (int ord : offsetToOrd) {
            if (ord == FieldArrayContext.NULL_ORD) {
                continue;
            }
            emitOrd.accept(ord);
        }
        builder.endPositionEntry();
    }

    /**
     * Counts the non-null slots in {@code offsetToOrd}. Callers use this to choose the position shape (null / lone value / multi-value)
     * before appending, so the value-append loop can stay in type-specific code and compile monomorphically.
     */
    static int countNonNull(int[] offsetToOrd) {
        int nonNullCount = 0;
        for (int ord : offsetToOrd) {
            if (ord != FieldArrayContext.NULL_ORD) {
                nonNullCount++;
            }
        }
        return nonNullCount;
    }

    /**
     * Asserts every slot is null.
     */
    static boolean allNulls(int[] offsetToOrd) {
        for (int ord : offsetToOrd) {
            if (ord != FieldArrayContext.NULL_ORD) {
                return false;
            }
        }
        return true;
    }
}
