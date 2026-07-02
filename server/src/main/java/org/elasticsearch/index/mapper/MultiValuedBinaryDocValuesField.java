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
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
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

    // Heuristic initial capacity for the ArrayList-backed orderings. Sized for the common
    // small multi-valued case to avoid the default 0→10 grow on the first add.
    private static final int INITIAL_VALUES_CAPACITY = 4;

    protected final ValueOrdering ordering;
    protected Collection<BytesRef> values;
    protected int docValuesByteCount = 0;

    MultiValuedBinaryDocValuesField(String name, ValueOrdering ordering) {
        super(name);
        this.ordering = ordering;
        this.values = ordering == ValueOrdering.SORTED_UNIQUE ? new TreeSet<>() : new ArrayList<>(INITIAL_VALUES_CAPACITY);
    }

    /**
     * Constructor for subclasses that manage their own values collection lazily and do not
     * need the base class to pre-allocate the backing collection.
     */
    protected MultiValuedBinaryDocValuesField(String name, ValueOrdering ordering, boolean eagerAllocate) {
        super(name);
        this.ordering = ordering;
        this.values = eagerAllocate
            ? (ordering == ValueOrdering.SORTED_UNIQUE ? new TreeSet<>() : new ArrayList<>(INITIAL_VALUES_CAPACITY))
            : null;
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
     * Utility method to add all ignored source values to their respective lucene document.
     * <p>
     * This method optimizes for non nested use case. For nested case, it will delegate to
     * {@link #addToBinaryFieldInDoc(LuceneDocument, String, BytesRef, ValueOrdering)}, given that each ignored source value needs to be
     * added to each respetive lucene document.
     */
    public static void addIgnoredSourceValues(
        Collection<IgnoredSourceFieldMapper.NameValue> ignoredFieldValues,
        String fieldName,
        ValueOrdering ordering,
        IndexVersion indexVersion,
        boolean hasNestedDocs
    ) {
        assert ignoredFieldValues.isEmpty() == false;
        if (hasNestedDocs) {
            for (var nameValue : ignoredFieldValues) {
                var encodedValue = IgnoredSourceFieldMapper.SingularIgnoredSourceEncoding.encode(nameValue);
                addToBinaryFieldInDoc(nameValue.doc(), fieldName, encodedValue, ordering, indexVersion);
            }
        } else {
            // In the non-nested case all ignored source values only need to be added to one Lucene document,
            // and then we can avoid the usage of LuceneDocument#addWithKey(...), which results in redundant hash map interaction.
            final boolean useSeparateCount = indexVersion.onOrAfter(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES);
            var ignoredSourceField = useSeparateCount ? new SeparateCount(fieldName, ordering) : new IntegratedCount(fieldName, ordering);
            var luceneDocument = ignoredFieldValues.iterator().next().doc();
            for (var value : ignoredFieldValues) {
                assert value.doc() == luceneDocument;
                ignoredSourceField.add(IgnoredSourceFieldMapper.SingularIgnoredSourceEncoding.encode(value));
            }
            luceneDocument.add(ignoredSourceField);
            if (useSeparateCount) {
                String countFieldName = fieldName + SeparateCount.COUNT_FIELD_SUFFIX;
                var countField = NumericDocValuesField.indexedField(countFieldName, ignoredSourceField.count());
                luceneDocument.add(countField);
            }
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
            var field = (IntegratedCount) doc.getOrAddWithKey(fieldName, key -> {
                var newField = new IntegratedCount(fieldName, ordering);
                doc.add(newField);
                return newField;
            });
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

        // Held here so addToDoc can update the count on each value without a second keyedFields lookup.
        NumericDocValuesField countField;

        public NumericDocValuesField countField() {
            return countField;
        }

        public SeparateCount(String name, ValueOrdering ordering) {
            super(name, ordering);
        }

        private static void addToDoc(LuceneDocument doc, String fieldName, BytesRef value, ValueOrdering ordering) {
            var field = (SeparateCount) doc.getOrAddWithKey(fieldName, key -> {
                var newField = new SeparateCount(fieldName, ordering);
                newField.countField = NumericDocValuesField.indexedField(newField.countFieldName(), -1);
                // use doc.addAll() instead of doc.add(), because later is backed by ArrayList and invoking doc.add() twice can trigger
                // growing the array twice. ArrayLists grows with length + 1.
                doc.addAll(List.of(newField, newField.countField));
                return newField;
            });
            field.add(value);
            field.countField.setLongValue(field.count());
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

        /**
         * Decodes the minimum ({@code maxMode=false}) or maximum ({@code maxMode=true}) value from a multi-value
         * ({@code count > 1}) {@code SeparateCount} blob. Values are stored sorted, so the minimum is simply the first
         * entry and the maximum is the last; callers must handle the {@code count <= 1} raw-passthrough case themselves.
         */
        public static BytesRef decodeExtreme(BytesRef raw, boolean maxMode) {
            ByteArrayStreamInput stream = new ByteArrayStreamInput();
            stream.reset(raw.bytes, raw.offset, raw.length);
            BytesRef selectedValue = new BytesRef();
            selectedValue.bytes = raw.bytes;
            try {
                if (maxMode == false) {
                    // First value = minimum.
                    selectedValue.length = stream.readVInt();
                    selectedValue.offset = stream.getPosition();
                } else {
                    // Last value = maximum: iterate through all entries.
                    int endPos = raw.offset + raw.length;
                    while (stream.getPosition() < endPos) {
                        selectedValue.length = stream.readVInt();
                        selectedValue.offset = stream.getPosition();
                        stream.setPosition(selectedValue.offset + selectedValue.length);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to decode SeparateCount extreme value", e);
            }
            return selectedValue;
        }
    }

    /**
     * Format used by high-cardinality fields in strictly columnar index mode that store their values in DOCUMENT ORDER (keeping
     * duplicates) so that array order can be reconstructed without a sidecar {@code .offsets} field. Nulls are encoded inline.
     * <p>
     * The companion {@code .counts} numeric doc values field (suffix {@link SeparateCount#COUNT_FIELD_SUFFIX}) stores the total number of
     * slots, INCLUDING null slots. Encoding:
     * <ul>
     *   <li>a single non-null value &rarr; {@code [val]} (raw bytes, no length prefix), exactly like {@link SeparateCount}</li>
     *   <li>two or more slots &rarr; {@code [len1+1][val1][len2+1][val2]...}. A real value of length {@code L} is stored with a
     *       {@code L+1} length prefix, so a stored length of {@code 0} is never produced by a real value and is reserved to mean
     *       {@code null} (zero following bytes). This is what distinguishes an inline {@code null} (prefix {@code 0}) from an empty
     *       string {@code ""} (prefix {@code 1}, zero bytes).</li>
     *   <li>zero non-null values (all-null array, lone {@code null}, or empty array) &rarr; no binary field is written at all; the
     *       {@code .counts} field alone carries the shape ({@code k>=1} null slots, or {@code 0} for an empty array)</li>
     * </ul>
     * Because a document with no non-null values writes no binary blob, the matching reader must advance on the {@code .counts} field
     * (binary-absent-while-counts-present denotes an all-null / empty-array document).
     */
    public static class ArrayOrderInlineNull extends MultiValuedBinaryDocValuesField {

        private boolean hasNonNullValue;

        // Held so the record* helpers can update the count on each slot without re-deriving the companion field from the document.
        private NumericDocValuesField countField;

        // Lazy single-slot storage: avoids allocating the backing ArrayList until a second slot
        // arrives. When hasSingleSlot==true and singleSlot==null, the single slot is a null
        // (inline null). The base-class values field starts null and is promoted to an ArrayList
        // only on the second add/addNull call.
        private BytesRef singleSlot;
        private boolean hasSingleSlot;

        public ArrayOrderInlineNull(String name) {
            super(name, ValueOrdering.UNSORTED, false);
        }

        public String countFieldName() {
            return name() + SeparateCount.COUNT_FIELD_SUFFIX;
        }

        /**
         * Records a non-null value directly into the document's accumulator for {@code fieldName}, in document order. The binary blob is
         * added to the document lazily on the first non-null value, so an all-null or empty-array document writes the {@code .counts}
         * field alone (see {@link ArrayOrderInlineNull}).
         */
        public static void recordValue(LuceneDocument doc, String fieldName, BytesRef value) {
            var field = getOrCreate(doc, fieldName);
            boolean firstNonNullValue = field.hasNonNullValue == false;
            field.add(value);
            if (firstNonNullValue) {
                doc.add(field);
            }
            field.countField.setLongValue(field.count());
        }

        /**
         * Records a {@code null} slot, preserving its position relative to the surrounding values; updates the {@code .counts} field but
         * never adds the binary blob.
         */
        public static void recordNull(LuceneDocument doc, String fieldName) {
            var field = getOrCreate(doc, fieldName);
            field.addNull();
            field.countField.setLongValue(field.count());
        }

        /**
         * Records an empty array: ensures the {@code .counts} field exists (value {@code 0}); no binary blob is written.
         */
        public static void recordEmptyArray(LuceneDocument doc, String fieldName) {
            getOrCreate(doc, fieldName);
        }

        /**
         * Optimized version of {@link #recordValue(LuceneDocument, String, BytesRef)}
         * for when it is very likely that a field has a single value.
         */
        public static void recordSingleValue(LuceneDocument doc, String fieldName, BytesRef value) {
            var field = new ArrayOrderInlineNull(fieldName);
            if (doc.putKeyIfAbsent(fieldName, field) == null) {
                field.add(value);
                field.countField = NumericDocValuesField.indexedField(field.countFieldName(), 1);
                doc.addAll(List.of(field, field.countField));
            } else {
                // Safety net (for dotted-field flattening or duplicated field names):
                // a field under the same name has already been registered.
                recordValue(doc, fieldName, value);
            }
        }

        /**
         * Looks up the per-field accumulator on the document, creating it on first use. The accumulator is registered by key (without
         * being added to the field list yet) and its always-present {@code .counts} companion is added to the document immediately.
         */
        private static ArrayOrderInlineNull getOrCreate(LuceneDocument doc, String fieldName) {
            return (ArrayOrderInlineNull) doc.getOrAddWithKey(fieldName, key -> {
                var field = new ArrayOrderInlineNull(fieldName);
                field.countField = NumericDocValuesField.indexedField(field.countFieldName(), 0);
                // Only the always-present .counts companion is added here; the binary blob is added lazily on the first non-null value.
                doc.add(field.countField);
                return field;
            });
        }

        @Override
        public void add(BytesRef value) {
            hasNonNullValue = true;
            if (values == null) {
                if (hasSingleSlot == false) {
                    singleSlot = value;
                    hasSingleSlot = true;
                } else {
                    // Second slot: promote the lazy single-slot to the backing list.
                    values = new ArrayList<>(INITIAL_VALUES_CAPACITY);
                    values.add(singleSlot);
                    values.add(value);
                    singleSlot = null;
                }
            } else {
                values.add(value);
            }
        }

        /**
         * Appends a {@code null} slot, preserving its position relative to the surrounding values. Null slots are counted towards
         * {@link #count()} but not towards {@code docValuesByteCount}.
         */
        public void addNull() {
            if (values == null) {
                if (hasSingleSlot == false) {
                    // singleSlot is already null — record the null slot lazily.
                    hasSingleSlot = true;
                } else {
                    // Second slot: promote to list.
                    values = new ArrayList<>(INITIAL_VALUES_CAPACITY);
                    values.add(singleSlot);
                    values.add(null);
                    singleSlot = null;
                }
            } else {
                values.add(null);
            }
        }

        /**
         * Whether at least one non-null value has been accumulated. When {@code false} the binary field must NOT be added to the
         * document; the {@code .counts} field alone represents the all-null / empty-array shape.
         */
        public boolean hasNonNullValue() {
            return hasNonNullValue;
        }

        @Override
        public int count() {
            return values != null ? values.size() : (hasSingleSlot ? 1 : 0);
        }

        @Override
        public BytesRef binaryValue() {
            if (values != null) {
                return encode(values);
            }
            assert hasSingleSlot && singleSlot != null : "a lone null slot must not write a binary value";
            return singleSlot;
        }

        /**
         * Encodes the given document-order slots (a {@code null} element denotes a {@code null} slot) into the format described on
         * {@link ArrayOrderInlineNull}. Must only be called when at least one non-null value is present; the all-null and empty-array
         * cases write no binary field.
         */
        public static BytesRef encode(Collection<BytesRef> slots) {
            int slotCount = slots.size();
            assert slotCount >= 1 : "in-order binary doc values must not be written for an empty document";
            if (slotCount == 1) {
                BytesRef only = slots.iterator().next();
                assert only != null : "a lone null slot must not write a binary value";
                return only;
            }
            int byteCount = 0;
            for (BytesRef slot : slots) {
                if (slot != null) {
                    byteCount += slot.length;
                }
            }
            int streamSize = byteCount + slotCount * VINT_MAX_BYTES;
            try (BytesStreamOutput out = new BytesStreamOutput(streamSize)) {
                for (BytesRef slot : slots) {
                    if (slot == null) {
                        out.writeVInt(0);
                    } else {
                        out.writeVInt(slot.length + 1);
                        out.writeBytes(slot.bytes, slot.offset, slot.length);
                    }
                }
                return out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to get binary value", e);
            }
        }

        /**
         * Decodes the minimum ({@code maxMode=false}) or maximum ({@code maxMode=true}) non-null value from a multi-slot
         * ({@code slotCount > 1}) {@code ArrayOrderInlineNull} blob. Values are stored in document order (not sorted) with
         * inline nulls, so unlike {@link SeparateCount#decodeExtreme}, this must scan every slot and compare values;
         * callers must handle the {@code slotCount <= 1} raw-passthrough case themselves. Returns {@code null} only if
         * every slot is null, which should not occur for a real sort key (an all-null document writes no binary blob).
         */
        public static BytesRef decodeExtreme(BytesRef raw, int slotCount, boolean maxMode) {
            ByteArrayStreamInput stream = new ByteArrayStreamInput();
            stream.reset(raw.bytes, raw.offset, raw.length);
            BytesRef extreme = null;
            try {
                for (int i = 0; i < slotCount; i++) {
                    int encodedLength = stream.readVInt();
                    if (encodedLength == 0) {
                        // Null slot: no bytes follow.
                        continue;
                    }
                    int length = encodedLength - 1;
                    int offset = stream.getPosition();
                    stream.setPosition(offset + length);
                    if (extreme == null) {
                        extreme = new BytesRef(raw.bytes, offset, length);
                    } else {
                        BytesRef candidate = new BytesRef(raw.bytes, offset, length);
                        boolean candidateWins = maxMode ? candidate.compareTo(extreme) > 0 : candidate.compareTo(extreme) < 0;
                        if (candidateWins) {
                            extreme = candidate;
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to decode ArrayOrderInlineNull extreme value", e);
            }
            return extreme;
        }
    }
}
