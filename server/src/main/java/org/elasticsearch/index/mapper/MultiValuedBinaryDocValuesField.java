/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * A custom implementation of {@link org.apache.lucene.index.BinaryDocValues} that uses a {@link Set} to maintain a collection of unique
 * binary doc values for fields with multiple values per document.
 */

public abstract class MultiValuedBinaryDocValuesField extends CustomDocValuesField {

    // vints are unlike normal ints in that they may require 5 bytes instead of 4
    // see BytesStreamOutput.writeVInt()
    private static final int VINT_MAX_BYTES = 5;

    protected final Collection<BytesRef> values;
    protected int docValuesByteCount = 0;

    MultiValuedBinaryDocValuesField(String name, boolean keepDuplicates) {
        super(name);
        this.values = keepDuplicates ? new ArrayList<>() : new TreeSet<>();
    }

    public void add(BytesRef value) {
        if (values.add(value)) {
            // might as well track these on the go as opposed to having to loop through all entries later
            docValuesByteCount += value.length;
        }
    }

    public int count() {
        return values.size();
    }

    protected void writeLenAndValues(BytesStreamOutput out) throws IOException {
        // sort the ArrayList variant of the collection prior to serializing it into a binary array
        if (values instanceof ArrayList<BytesRef> list) {
            list.sort(Comparator.naturalOrder());
        }

        for (BytesRef value : values) {
            int valueLength = value.length;
            out.writeVInt(valueLength);
            out.writeBytes(value.bytes, value.offset, valueLength);
        }
    }

    @Override
    public abstract BytesRef binaryValue();

    public static class IntegratedCount extends MultiValuedBinaryDocValuesField {
        public IntegratedCount(String name, boolean keepDuplicates) {
            super(name, keepDuplicates);
        }

        /**
         * Encodes the collection of binary doc values as a single contiguous binary array, wrapped in {@link BytesRef}. This array takes
         * the form of [doc value count][length of value 1][value 1][length of value 2][value 2]...
         */
        @Override
        public BytesRef binaryValue() {
            int docValuesCount = values.size();
            // the + 1 is for the total doc values count, which is prefixed at the start of the array
            int streamSize = docValuesByteCount + (docValuesCount + 1) * VINT_MAX_BYTES;

            try (BytesStreamOutput out = new BytesStreamOutput(streamSize)) {
                out.writeVInt(docValuesCount);
                writeLenAndValues(out);
                return out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to get binary value", e);
            }
        }
    }

    public static class SeparateCount extends MultiValuedBinaryDocValuesField {
        public static final String COUNT_FIELD_SUFFIX = ".counts";

        public SeparateCount(String name, boolean keepDuplicates) {
            super(name, keepDuplicates);
        }

        /**
         * Encodes the collection of BytesRef into a single BytesRef. Unlike IntegratedCount, the number of values is not stored, and
         * if there is only a single value, the length is not stored.
         * For multiple values, the format is: [length of value 1][value 1][length of value 2][value 2]...
         * For a single value, the format is: [value 1]
         */
        @Override
        public BytesRef binaryValue() {
            int docValuesCount = values.size();

            if (docValuesCount == 1) {
                return values.iterator().next();
            }

            int streamSize = docValuesByteCount + docValuesCount * VINT_MAX_BYTES;
            try (BytesStreamOutput out = new BytesStreamOutput(streamSize)) {
                writeLenAndValues(out);
                return out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }
        }

        public String countFieldName() {
            return name() + COUNT_FIELD_SUFFIX;
        }

        public static void addToSeparateCountMultiBinaryFieldInDoc(LuceneDocument doc, String fieldName, BytesRef binaryValue) {
            var field = (SeparateCount) doc.getByKey(fieldName);
            var countField = (NumericDocValuesField) doc.getByKey(fieldName + COUNT_FIELD_SUFFIX);
            if (field == null) {
                field = new SeparateCount(fieldName, false);
                countField = NumericDocValuesField.indexedField(field.countFieldName(), -1); // dummy value
                doc.addWithKey(field.name(), field);
                doc.addWithKey(countField.name(), countField);
            }

            field.add(binaryValue);
            countField.setLongValue(field.count());
        }
    }
}
