/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Helper class to read custom binary doc values.
 */
public final class MultiValueSeparateCountBinaryDocValuesReader {

    private final BytesRef scratch = new BytesRef();
    private final ByteArrayStreamInput in = new ByteArrayStreamInput();

    public MultiValueSeparateCountBinaryDocValuesReader() {}

    public void read(BytesRef bytes, long count, BlockLoader.BytesRefBuilder builder) throws IOException {
        if (count == 1) {
            builder.appendBytesRef(bytes);
            return;
        }

        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        builder.beginPositionEntry();
        for (int v = 0; v < count; v++) {
            initializeScratch();
            builder.appendBytesRef(scratch);
        }
        builder.endPositionEntry();
    }

    /**
     * Deep-copies every value out of the doc-value blob so callers can index into the result by ord without invalidating slices on
     * subsequent reads. Used by ordered (offsets-aware) readers, which need random access into the per-doc value list.
     */
    public BytesRef[] materialize(BytesRef bytes, long count) throws IOException {
        BytesRef[] out = new BytesRef[Math.toIntExact(count)];
        if (count == 1) {
            out[0] = BytesRef.deepCopyOf(bytes);
            return out;
        }
        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < count; v++) {
            initializeScratch();
            out[v] = BytesRef.deepCopyOf(scratch);
        }
        return out;
    }

    public boolean match(BytesRef bytes, long count, Predicate<BytesRef> predicate) throws IOException {
        if (count == 1) {
            return predicate.test(bytes);
        }

        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < count; v++) {
            initializeScratch();
            if (predicate.test(scratch)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Matches against a {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull} blob, where each
     * slot is encoded as {@code [valueLen+1][key\0value]}, with {@code prefix==0} marking a null slot ({@code [0][key\0]}). Null slots are
     * skipped; the full {@code key\0value} bytes of non-null slots are tested against {@code predicate}.
     */
    public boolean matchKeyedInlineNull(BytesRef bytes, long count, Predicate<BytesRef> predicate) throws IOException {
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < count; v++) {
            int prefix = in.readVInt();
            int slotKeyStart = in.getPosition();  // absolute index in bytes.bytes of the first key byte
            int remaining = bytes.length - (slotKeyStart - bytes.offset);
            int keyLen = ESVectorUtil.indexOf(bytes.bytes, slotKeyStart, remaining, (byte) 0);
            assert keyLen != -1 : "KeyedArrayOrderInlineNull slot has no separator byte";
            in.skipBytes(keyLen + 1);
            if (prefix != 0) {
                // Non-null slot: [valueLen+1]key\0value — valueLen = prefix - 1.
                int valueLen = prefix - 1;
                scratch.bytes = bytes.bytes;
                scratch.offset = slotKeyStart;
                scratch.length = keyLen + 1 + valueLen;  // key\0value
                in.skipBytes(valueLen);
                if (predicate.test(scratch)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void readMin(BytesRef bytes, int count, BlockLoader.BytesRefBuilder builder) throws IOException {
        readExtreme(bytes, count, builder, (a, b) -> a.compareTo(b) < 0);
    }

    public void readMax(BytesRef bytes, long count, BlockLoader.BytesRefBuilder builder) throws IOException {
        readExtreme(bytes, count, builder, (a, b) -> a.compareTo(b) > 0);
    }

    private void readExtreme(BytesRef bytes, long count, BlockLoader.BytesRefBuilder builder, BiPredicate<BytesRef, BytesRef> predicate)
        throws IOException {
        if (count == 1) {
            builder.appendBytesRef(bytes);
            return;
        }

        BytesRef extreme = null;
        scratch.bytes = bytes.bytes;
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < count; v++) {
            initializeScratch();
            if (extreme == null || predicate.test(scratch, extreme)) {
                extreme = new BytesRef(scratch.bytes, scratch.offset, scratch.length);
            }
        }
        builder.appendBytesRef(extreme);
    }

    private void initializeScratch() throws IOException {
        scratch.length = in.readVInt();
        scratch.offset = in.getPosition();
        in.setPosition(scratch.offset + scratch.length);
    }
}
