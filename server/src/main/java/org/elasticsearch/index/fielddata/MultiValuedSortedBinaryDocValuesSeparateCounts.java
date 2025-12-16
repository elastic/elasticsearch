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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;

import java.io.IOException;

/**
 * Wrapper around {@link BinaryDocValues} to decode the typical multivalued encoding used by
 * {@link org.elasticsearch.index.mapper.BinaryFieldMapper.CustomBinaryDocValuesField}.
 */
public class MultiValuedSortedBinaryDocValuesSeparateCounts extends SortedBinaryDocValues {

    BinaryDocValues values;
    NumericDocValues counts;
    int count;

    // the binary doc values for a document are all encoded in a single binary array, which this stream knows how to read
    // the doc values in the array take the form of [doc value count][length of value 1][value 1][length of value 2][value 2]...
    final ByteArrayStreamInput in = new ByteArrayStreamInput();
    final BytesRef scratch = new BytesRef();

    public MultiValuedSortedBinaryDocValuesSeparateCounts(BinaryDocValues values, NumericDocValues counts) {
        this.values = values;
        this.counts = counts;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        if (values.advanceExact(doc)) {
            assert counts.advanceExact(doc);
            count = Math.toIntExact(counts.longValue());

            final BytesRef bytes = values.binaryValue();

            if (count == 1) {
                scratch.bytes = bytes.bytes;
                scratch.offset = bytes.offset;
                scratch.length = bytes.length;
            } else {
                in.reset(bytes.bytes, bytes.offset, bytes.length);
                scratch.bytes = bytes.bytes;
            }
            return true;
        } else {
            count = 0;
            return false;
        }
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public BytesRef nextValue() throws IOException {
        if (count != 1) {
            scratch.length = in.readVInt();
            scratch.offset = in.getPosition();
            in.setPosition(scratch.offset + scratch.length);
        }
        return scratch;
    }
}
