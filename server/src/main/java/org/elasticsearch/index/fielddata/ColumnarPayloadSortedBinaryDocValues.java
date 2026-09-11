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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBinaryPayload;

import java.io.IOException;

/**
 * Reader for the {@link org.elasticsearch.index.mapper.ColumnarBinaryDocValuesField ColumnarBinaryDocValuesField} payload, where a
 * document's slot count travels in the blob ahead of its slots and nulls are encoded inline. The columnar counterpart of
 * {@link SortingArrayOrderBinaryDocValues}, and simpler for carrying its own count: every shape a document takes — values, inline nulls,
 * an empty array — is one payload, so there is no companion field to advance on and no blob stored raw.
 *
 * <p>Null slots are dropped, as they are for every other format at this surface, and the surviving values are sorted.
 */
public final class ColumnarPayloadSortedBinaryDocValues extends SortingBinaryDocValues {

    private final BinaryDocValues binary;
    private final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();

    public ColumnarPayloadSortedBinaryDocValues(BinaryDocValues binary) {
        this.binary = binary;
    }

    public static ColumnarPayloadSortedBinaryDocValues from(LeafReader leafReader, String valuesFieldName) throws IOException {
        return new ColumnarPayloadSortedBinaryDocValues(DocValues.getBinary(leafReader, valuesFieldName));
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        if (binary.advanceExact(doc) == false) {
            count = 0;
            return false;
        }
        final BytesRef bytes = binary.binaryValue();
        final int slotCount = decoder.reset(bytes);
        // Size the scratch to the slot count — an upper bound on the surviving non-null values — then trim to the non-null total.
        count = slotCount;
        grow();
        int nonNull = 0;
        for (int slot = 0; slot < slotCount; slot++) {
            final BytesRef value = decoder.next();
            if (value == null) {
                continue; // null slot
            }
            values[nonNull++].copyBytes(value);
        }
        count = nonNull;
        sort();
        return nonNull > 0;
    }
}
