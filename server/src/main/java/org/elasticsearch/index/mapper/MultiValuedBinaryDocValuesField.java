/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * A custom implementation of {@link org.apache.lucene.index.BinaryDocValues} that uses a {@link Set} to maintain a collection of unique
 * binary doc values for fields with multiple values per document.
 */
public class MultiValuedBinaryDocValuesField extends CustomDocValuesField {

    private final Set<BytesRef> uniqueValues;
    private int docValuesByteCount = 0;

    MultiValuedBinaryDocValuesField(String name) {
        super(name);
        uniqueValues = new TreeSet<>();
    }

    public void add(BytesRef value) {
        if (uniqueValues.add(value)) {
            // might as well track these on the go as opposed to having to loop through all entries later
            docValuesByteCount += value.length;
        }
    }

    /**
     * Encodes the collection of binary doc values as a single contiguous binary array, wrapped in {@link BytesRef}. This array takes
     * the form of [doc value count][length of value 1][value 1][length of value 2][value 2]...
     */
    @Override
    public BytesRef binaryValue() {
        int docValuesCount = uniqueValues.size();
        // the + 1 is for the total doc values count, which is prefixed at the start of the array
        int streamSize = docValuesByteCount + (docValuesCount + 1) * Integer.BYTES;

        try (BytesStreamOutput out = new BytesStreamOutput(streamSize)) {
            out.writeVInt(docValuesCount);
            for (BytesRef value : uniqueValues) {
                int valueLength = value.length;
                out.writeVInt(valueLength);
                out.writeBytes(value.bytes, value.offset, valueLength);
            }
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to get binary value", e);
        }
    }
}
