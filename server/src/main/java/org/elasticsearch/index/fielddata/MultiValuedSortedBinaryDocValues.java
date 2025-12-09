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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;

import java.io.IOException;

/**
 * Wrapper around {@link BinaryDocValues} to decode the typical multivalued encoding used by
 * {@link org.elasticsearch.index.mapper.BinaryFieldMapper.CustomBinaryDocValuesField}.
 */
public class MultiValuedSortedBinaryDocValues extends SortedBinaryDocValues {

    BinaryDocValues values;
    int count;
    final ByteArrayStreamInput in = new ByteArrayStreamInput();
    final BytesRef scratch = new BytesRef();

    public MultiValuedSortedBinaryDocValues(BinaryDocValues values) {
        this.values = values;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        if (values.advanceExact(doc)) {
            final BytesRef bytes = values.binaryValue();
            assert bytes.length > 0;
            in.reset(bytes.bytes, bytes.offset, bytes.length);
            count = in.readVInt();
            scratch.bytes = bytes.bytes;
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
        scratch.length = in.readVInt();
        scratch.offset = in.getPosition();
        in.setPosition(scratch.offset + scratch.length);
        return scratch;
    }
}
