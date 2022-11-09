/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;

abstract class AbstractBinaryDVLeafFieldData implements LeafFieldData {
    private final BinaryDocValues values;

    AbstractBinaryDVLeafFieldData(BinaryDocValues values) {
        super();
        this.values = values;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by Lucene
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        return new SortedBinaryDocValues() {

            int count;
            final ByteArrayStreamInput in = new ByteArrayStreamInput();
            final BytesRef scratch = new BytesRef();

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

        };
    }

    @Override
    public void close() {
        // no-op
    }
}
