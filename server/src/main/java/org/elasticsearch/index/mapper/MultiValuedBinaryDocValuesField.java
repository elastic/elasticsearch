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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    }

    /**
     * Format used by high-cardinality fields in strictly columnar index mode that store their values in DOCUMENT ORDER (keeping
     * duplicates) so that array order can be reconstructed without a sidecar {@code .offsets} field. Nulls are encoded inline.
     * <p>
     * The companion {@code .counts} numeric doc values field (suffix {@link SeparateCount#COUNT_FIELD_SUFFIX}) stores the total number of
     * slots, INCLUDING null slots. Encoding:
     * <ul>
     *   <li>single non-null value &rarr; {@code [val]} (raw bytes, no length prefix)</li>
     *   <li>two or more slots with no duplicates and no nulls ({@code slotCount == distinctCount}) &rarr;
     *       {@code [D][len1][val1]...[lenD][valD]}: a vint {@code distinctCount} followed by {@code distinctCount}
     *       plain-length-prefixed values in first-seen (document) order. No ordinal stream is written because it would be the trivial
     *       sequence {@code 1,2,...,distinctCount}.</li>
     *   <li>two or more slots with at least one duplicate or null ({@code slotCount > distinctCount}) &rarr;
     *       {@code [D][len1][val1]...[lenD][valD][ord1][ord2]...}: a vint {@code distinctCount} (number of distinct non-null values),
     *       followed by {@code distinctCount} plain-length-prefixed values in first-seen order, followed by {@code slotCount} vint
     *       ordinals (one per slot):
     *       {@code 0} means null, {@code k>=1} refers to distinct value {@code k-1}. Each distinct value is stored once; repeated values
     *       within a document reference the same ordinal, keeping per-doc blobs small so binary doc-values blocks remain count-bound and
     *       ZSTD can compress across many documents.</li>
     *   <li>zero non-null values (all-null array, lone {@code null}, or empty array) &rarr; no binary field is written at all; the
     *       {@code .counts} field alone carries the shape ({@code k>=1} null slots, or {@code 0} for an empty array)</li>
     * </ul>
     * Because a document with no non-null values writes no binary blob, the matching reader must advance on the {@code .counts} field
     * (binary-absent-while-counts-present denotes an all-null or empty-array document).
     */
    public static class ArrayOrderInlineNull extends MultiValuedBinaryDocValuesField {

        private boolean hasNonNullValue;

        // Held so the record* helpers can update the count on each slot without re-deriving the companion field from the document.
        private NumericDocValuesField countField;

        public ArrayOrderInlineNull(String name) {
            super(name, ValueOrdering.UNSORTED);
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
         * Whether at least one non-null value has been accumulated. When {@code false} the binary field must NOT be added to the
         * document; the {@code .counts} field alone represents the all-null / empty-array shape.
         */
        public boolean hasNonNullValue() {
            return hasNonNullValue;
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
            super.add(value);
        }

        /**
         * Appends a {@code null} slot, preserving its position relative to the surrounding values. Null slots are counted towards
         * {@link #count()} but not towards {@code docValuesByteCount}.
         */
        public void addNull() {
            // The UNSORTED ordering backs values with an ArrayList, which permits null elements.
            values.add(null);
        }

        @Override
        public BytesRef binaryValue() {
            return encode((List<BytesRef>) values);
        }

        /**
         * Encodes the given document-order slots (a {@code null} element denotes a {@code null} slot) into the format described on
         * {@link ArrayOrderInlineNull}. Must only be called when at least one non-null value is present; the all-null and empty-array
         * cases write no binary field.
         */
        public static BytesRef encode(List<BytesRef> slots) {
            int slotCount = slots.size();
            assert slotCount >= 1 : "in-order binary doc values must not be written for an empty document";
            if (slotCount == 1) {
                BytesRef only = slots.getFirst();
                assert only != null : "a lone null slot must not write a binary value";
                return only;
            }

            // Assign first-seen ordinals to distinct non-null values (BytesRef.equals / hashCode are value-based).
            // Size the map to hold up to slotCount entries without rehashing (distinct values <= slotCount).
            Map<BytesRef, Integer> ordinals = new HashMap<>((int) (slotCount / 0.75f) + 1);
            List<BytesRef> distinctInOrder = new ArrayList<>();
            int distinctByteCount = 0;
            for (BytesRef slot : slots) {
                if (slot != null && ordinals.putIfAbsent(slot, ordinals.size() + 1) == null) {
                    // putIfAbsent returns null iff the mapping was newly inserted (1-based; 0 reserved for null)
                    distinctInOrder.add(slot);
                    distinctByteCount += slot.length;
                }
            }
            int distinctCount = ordinals.size();
            // Ordinals are only needed when there are duplicates or null slots (slotCount > distinctCount).
            // When slotCount == distinctCount every slot is a distinct non-null value; the distinctCount distinct values in first-seen
            // order ARE the array, so the trivial ordinal sequence 1,2,...,distinctCount is redundant and can be omitted.
            boolean writeOrdinals = slotCount != distinctCount;
            // Size estimate: vint for distinctCount + distinctCount*(VINT_MAX_BYTES + distinctBytes)
            // [+ slotCount*VINT_MAX_BYTES when ordinals are written]
            int streamSize = VINT_MAX_BYTES + distinctCount * VINT_MAX_BYTES + distinctByteCount + (writeOrdinals
                ? slotCount * VINT_MAX_BYTES
                : 0);
            try (BytesStreamOutput out = new BytesStreamOutput(streamSize)) {
                out.writeVInt(distinctCount);
                // Write distinct values in first-seen (insertion) order (ordinals 1..distinctCount).
                for (BytesRef val : distinctInOrder) {
                    out.writeVInt(val.length);
                    out.writeBytes(val.bytes, val.offset, val.length);
                }
                // Write one ordinal per slot (0 = null, 1..D = distinct value) only when there are duplicates or nulls.
                if (writeOrdinals) {
                    for (BytesRef slot : slots) {
                        out.writeVInt(slot == null ? 0 : ordinals.get(slot));
                    }
                }
                return out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to get binary value", e);
            }
        }
    }
}
