/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * Reader for the {@link MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull} binary doc-values format that filters slots by a target
 * key and returns the matching values in sorted order, compatible with fielddata and block-loading consumers.
 * <p>
 * Values are stored in document order using the uniform {@code [valueLen+1][key\0value]} encoding; a prefix of {@code 0} marks a null slot
 * ({@code [0][key\0]}). This reader discards nulls and slots whose key does not match {@code targetKey}, then sorts the remaining values.
 * <p>
 * A reader created with {@link #allKeys} keeps the non-null slots of every key instead. The result is the sorted distinct values of the
 * whole field, which is how the root of a flattened field reads its values.
 */
public final class KeyFilteredSortingArrayOrderBinaryDocValues extends SortingBinaryDocValues {

    private final BinaryDocValues binary;
    private final NumericDocValues counts;
    private final BytesRef targetKey;
    private final ByteArrayStreamInput in = new ByteArrayStreamInput();

    /**
     * @param targetKey the key whose values to return, or {@code null} to return the values of every key
     */
    public KeyFilteredSortingArrayOrderBinaryDocValues(BinaryDocValues binary, NumericDocValues counts, @Nullable BytesRef targetKey) {
        this.binary = binary;
        this.counts = counts;
        this.targetKey = targetKey;
    }

    /**
     * Reads the values of every key, which is how the root of a flattened field reads its values. The values of a document are the
     * non-null values of all keys, sorted and distinct.
     */
    public static KeyFilteredSortingArrayOrderBinaryDocValues allKeys(BinaryDocValues binary, NumericDocValues counts) {
        return new KeyFilteredSortingArrayOrderBinaryDocValues(binary, counts, null);
    }

    /**
     * Removes consecutive equal values from the sorted {@code values[0..count-1]} range. Uses swaps rather than
     * overwrites to keep every {@link org.apache.lucene.util.BytesRefBuilder} reachable in the array for reuse on
     * future documents.
     */
    private void dedup() {
        if (count <= 1) {
            return;
        }
        int deduped = 1;
        for (int i = 1; i < count; i++) {
            if (values[i].get().bytesEquals(values[deduped - 1].get()) == false) {
                ArrayUtil.swap(values, i, deduped);
                deduped++;
            }
        }
        count = deduped;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        count = 0;

        if (counts == null || counts.advanceExact(doc) == false) {
            return false;
        }

        int slotCount = Math.toIntExact(counts.longValue());

        if (binary.advanceExact(doc) == false) {
            return false;
        }

        BytesRef bytes = binary.binaryValue();

        // Size scratch to slotCount (an upper bound on non-null matching slots).
        count = slotCount;
        grow();
        in.reset(bytes.bytes, bytes.offset, bytes.length);
        int matching = 0;
        for (int i = 0; i < slotCount; i++) {
            int prefix = in.readVInt();
            int slotKeyStart = in.getPosition();  // absolute position of key bytes in bytes.bytes
            if (prefix == 0) {
                // Null slot: [0]key\0 — skip past key bytes and separator.
                int keyLen = ESVectorUtil.indexOf(bytes.bytes, slotKeyStart, bytes.length - (slotKeyStart - bytes.offset), (byte) 0);
                assert keyLen != -1 : "KeyedArrayOrderInlineNull null slot has no separator byte";
                in.skipBytes(keyLen + 1);
            } else {
                // Non-null slot: [valueLen+1]key\0value — valueLen = prefix - 1.
                int valueLen = prefix - 1;
                int keyLen = ESVectorUtil.indexOf(bytes.bytes, slotKeyStart, bytes.length - (slotKeyStart - bytes.offset), (byte) 0);
                assert keyLen != -1 : "KeyedArrayOrderInlineNull slot has no separator byte";
                in.skipBytes(keyLen + 1);
                if (targetKey == null
                    || (keyLen == targetKey.length
                        && Arrays.equals(
                            bytes.bytes,
                            slotKeyStart,
                            slotKeyStart + keyLen,
                            targetKey.bytes,
                            targetKey.offset,
                            targetKey.offset + targetKey.length
                        ))) {
                    values[matching].copyBytes(bytes.bytes, in.getPosition(), valueLen);
                    matching++;
                }
                in.skipBytes(valueLen);
            }
        }
        count = matching;
        sort();
        dedup();
        return count > 0;
    }
}
