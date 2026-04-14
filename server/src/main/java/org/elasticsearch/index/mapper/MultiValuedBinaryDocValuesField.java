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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * A custom implementation of {@link org.apache.lucene.index.BinaryDocValues} that stores a collection of
 * binary doc values for fields with multiple values per document.
 */
public abstract class MultiValuedBinaryDocValuesField extends CustomDocValuesField {

    // vints are unlike normal ints in that they may require 5 bytes instead of 4
    // see BytesStreamOutput.writeVInt()
    private static final int VINT_MAX_BYTES = 5;

    /**
     * Controls how values are collected and ordered in a multi-valued binary doc values field.
     */
    public enum ValueOrdering {
        /** Values are deduplicated and sorted (backed by a {@link TreeSet}). */
        SORTED_UNIQUE,
        /** Duplicates are kept, values are sorted at encode time (backed by an {@link ArrayList}). */
        SORTED,
        /** Duplicates are kept, no sorting (backed by an {@link ArrayList}). */
        UNSORTED
    }

    protected final ValueOrdering ordering;
    protected final Collection<BytesRef> values;
    protected int docValuesByteCount = 0;

    MultiValuedBinaryDocValuesField(String name, ValueOrdering ordering) {
        super(name);
        this.ordering = ordering;
        this.values = ordering == ValueOrdering.SORTED_UNIQUE ? new TreeSet<>() : new ArrayList<>();
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
        // Only ArraysLists need sorting
        if (ordering == ValueOrdering.SORTED && values instanceof ArrayList<BytesRef> list) {
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

    /**
     * Adds a value to a multi-valued binary doc values field in the given document.
     */
    public static void addToBinaryFieldInDoc(LuceneDocument doc, String fieldName, BytesRef value, ValueOrdering ordering) {
        addToBinaryFieldInDoc(doc, fieldName, value, ordering, IndexVersion.current());
    }

    public static void addToBinaryFieldInDoc(LuceneDocument doc, String fieldName, BytesRef value) {
        addToBinaryFieldInDoc(doc, fieldName, value, ValueOrdering.SORTED_UNIQUE, IndexVersion.current());
    }

    /**
     * This function exists for backwards compatibility with old indices that used {@link IntegratedCount}.
     * <p>
     * For indices created on or after {@link IndexVersions#DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES}, the {@link SeparateCount}
     * format is used. For older indices, the {@link IntegratedCount} format is used.
     */
    public static void addToBinaryFieldInDoc(
        LuceneDocument doc,
        String fieldName,
        BytesRef value,
        ValueOrdering ordering,
        IndexVersion indexVersion
    ) {
        if (indexVersion.onOrAfter(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES)) {
            SeparateCount.addToDoc(doc, fieldName, value, ordering);
        } else {
            IntegratedCount.addToDoc(doc, fieldName, value, ordering);
        }
    }

    /**
     * Format that integrates the value count into the binary payload itself.
     * <p>
     * Encoding: {@code [count][len1][val1][len2][val2]...}
     */
    public static class IntegratedCount extends MultiValuedBinaryDocValuesField {

        public IntegratedCount(String name, ValueOrdering ordering) {
            super(name, ordering);
        }

        private static void addToDoc(LuceneDocument doc, String fieldName, BytesRef value, ValueOrdering ordering) {
            var field = (IntegratedCount) doc.getByKey(fieldName);
            if (field == null) {
                field = new IntegratedCount(fieldName, ordering);
                doc.addWithKey(fieldName, field);
            }
            field.add(value);
        }

        /**
         * Encodes the collection of binary doc values as a single contiguous binary array, wrapped in {@link BytesRef}.
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

        /**
         * Encodes a list of {@link BytesRef} values into the integrated-count format: {@code [count][len1][val1][len2][val2]...}.
         * <p>
         * Note, this is basically the static version of binaryValue(). The benefit of having this static code is that we can skip the
         * overhead of creating a whole new Lucene field and adding values to it.
         */
        public static BytesRef encode(List<BytesRef> values) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeVInt(values.size());
                for (BytesRef val : values) {
                    out.writeVInt(val.length);
                    out.writeBytes(val.bytes, val.offset, val.length);
                }
                return out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to encode integrated count binary value", e);
            }
        }
    }

    /**
     * Format that stores the value count in a separate companion {@code .counts} numeric doc values field.
     * <p>
     * Encoding for multiple values: {@code [len1][val1][len2][val2]...}
     * <br>
     * Encoding for a single value: {@code [val1]} (no length prefix)
     */
    public static class SeparateCount extends MultiValuedBinaryDocValuesField {

        public static final String COUNT_FIELD_SUFFIX = ".counts";

        public SeparateCount(String name, ValueOrdering ordering) {
            super(name, ordering);
        }

        private static void addToDoc(LuceneDocument doc, String fieldName, BytesRef value, ValueOrdering ordering) {
            var field = (SeparateCount) doc.getByKey(fieldName);
            final NumericDocValuesField countField;
            if (field == null) {
                field = new SeparateCount(fieldName, ordering);
                countField = NumericDocValuesField.indexedField(field.countFieldName(), -1);
                doc.addWithKey(field.name(), field);
                doc.addWithKey(countField.name(), countField);
            } else {
                countField = (NumericDocValuesField) doc.getByKey(fieldName + COUNT_FIELD_SUFFIX);
            }
            field.add(value);
            countField.setLongValue(field.count());
        }

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
                throw new UncheckedIOException("Failed to get binary value", e);
            }
        }

        public String countFieldName() {
            return name() + COUNT_FIELD_SUFFIX;
        }
    }
}
