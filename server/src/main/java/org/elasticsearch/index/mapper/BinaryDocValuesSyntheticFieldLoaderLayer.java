/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public final class BinaryDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private final String fieldName;

    // the binary doc values for a document are all encoded in a single binary array, which this stream knows how to read
    // the doc values in the array take the form of [doc value count][length of value 1][value 1][length of value 2][value 2]...
    private final ByteArrayDataInput data = new ByteArrayDataInput();
    private int valueCount;

    public BinaryDocValuesSyntheticFieldLoaderLayer(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        BinaryDocValues docValues = leafReader.getBinaryDocValues(fieldName);

        // there are no values associated with this field
        if (docValues == null) {
            valueCount = 0;
            return null;
        }

        return docId -> {
            // there are no more documents to process
            if (docValues.advanceExact(docId) == false) {
                valueCount = 0;
                return false;
            }

            // otherwise, extract the doc values into a stream to later read from
            BytesRef docValuesBytes = docValues.binaryValue();
            data.reset(docValuesBytes.bytes, docValuesBytes.offset, docValuesBytes.length);
            valueCount = data.readVInt();

            return hasValue();
        };
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        for (int i = 0; i < valueCount; i++) {
            // read the length of the value
            int length = data.readVInt();

            // read that many bytes from the input
            // the read will automatically move the offset to the next value
            byte[] valueBytes = new byte[length];
            data.readBytes(valueBytes, 0, length);

            // finally, write those bytes into XContentBuilder
            b.value(new BytesRef(valueBytes).utf8ToString());
        }
    }

    @Override
    public boolean hasValue() {
        return valueCount > 0;
    }

    @Override
    public long valueCount() {
        return valueCount;
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

}
