/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FieldArrayContext {

    protected static final String OFFSETS_FIELD_NAME_SUFFIX = ".offsets";
    protected final Map<String, Offsets> offsetsPerField = new HashMap<>();

    public void recordOffset(String field, Comparable<?> value) {
        Offsets arrayOffsets = offsetsPerField.computeIfAbsent(field, k -> new Offsets());
        arrayOffsets.slotsRecorded = true;
        int nextOffset = arrayOffsets.currentOffset++;
        var offsets = arrayOffsets.valueToOffsets.computeIfAbsent(value, s -> new ArrayList<>(2));
        offsets.add(nextOffset);
    }

    /**
     * Like {@link #recordOffset(String, Comparable)}, but also registers a writer that converts a recorded array-slot value
     * back to a form suitable for {@code _source}, so that, when the encoded offsets turn out too large for a sorted doc
     * value, the array can be rebuilt and stored in {@code _ignored_source} instead of failing the document. Since mappers
     * record their doc-values representation (e.g. sortable longs for numbers), the writer must produce the same
     * representation that the field's offsets-aware synthetic source loader produces, so that the {@code _ignored_source}
     * fallback is indistinguishable from the native array reconstruction.
     */
    public void recordOffset(
        String field,
        Comparable<?> value,
        CheckedBiConsumer<XContentBuilder, Comparable<?>, IOException> fallbackWriter
    ) {
        registerFallbackWriter(field, fallbackWriter);
        recordOffset(field, value);
    }

    public void recordNull(String field) {
        Offsets arrayOffsets = offsetsPerField.computeIfAbsent(field, k -> new Offsets());
        arrayOffsets.slotsRecorded = true;
        int nextOffset = arrayOffsets.currentOffset++;
        arrayOffsets.nullValueOffsets.add(nextOffset);
    }

    /**
     * Like {@link #recordNull(String)}, but also registers a fallback writer; see
     * {@link #recordOffset(String, Comparable, CheckedBiConsumer)}.
     */
    public void recordNull(String field, CheckedBiConsumer<XContentBuilder, Comparable<?>, IOException> fallbackWriter) {
        registerFallbackWriter(field, fallbackWriter);
        recordNull(field);
    }

    /**
     * Records a value of {@code field} that was not indexed, because of {@code ignore_above} or {@code ignore_malformed}, and so
     * occupies no offsets slot. Such values live in field-specific fallback storage that the field's synthetic source loader
     * reads back, but an {@code _ignored_source} entry for the field suppresses that loader at load time. So when the encoded
     * offsets turn out too large for a sorted doc value and the array is moved to {@code _ignored_source} (see
     * {@link #recordOffset(String, Comparable, CheckedBiConsumer)}), the recorded ignored values are written into that entry at
     * their original position in the array. They are not part of the encoded offsets.
     *
     * @param encodedValue the ignored value, encoded like {@link XContentDataHelper#encodeToken} does
     */
    public void recordIgnored(String field, BytesRef encodedValue) {
        Offsets arrayOffsets = offsetsPerField.computeIfAbsent(field, k -> new Offsets());
        arrayOffsets.ignoredValues.add(new Offsets.IgnoredValue(arrayOffsets.currentOffset, encodedValue));
    }

    private void registerFallbackWriter(String field, CheckedBiConsumer<XContentBuilder, Comparable<?>, IOException> fallbackWriter) {
        Offsets arrayOffsets = offsetsPerField.computeIfAbsent(field, k -> new Offsets());
        assert arrayOffsets.fallbackWriter == null || arrayOffsets.fallbackWriter == fallbackWriter
            : "a field must always record offsets with the same fallback writer";
        arrayOffsets.fallbackWriter = fallbackWriter;
    }

    void maybeRecordEmptyArray(String field) {
        offsetsPerField.computeIfAbsent(field, k -> new Offsets()).slotsRecorded = true;
    }

    public void addToLuceneDocument(DocumentParserContext context) throws IOException {
        for (var entry : offsetsPerField.entrySet()) {
            var fieldName = entry.getKey();
            var offsets = entry.getValue();

            // An array whose values were all ignored (ignore_above, ignore_malformed) recorded ignored values but no slot.
            // Writing offsets for it would read back as an empty-array marker; the ignored values are synthesized from the
            // field's own storage of ignored values instead, like before ignored values were recorded here.
            if (offsets.slotsRecorded == false) {
                continue;
            }
            BytesRef encoded = encodeOffsetArray(offsets);
            if (encoded.length <= IndexWriter.MAX_TERM_LENGTH) {
                context.doc().add(new SortedDocValuesField(fieldName, encoded));
            } else {
                // Lucene rejects sorted doc values larger than MAX_TERM_LENGTH, and the encoding takes 1-5 bytes per array
                // slot, so a very large leaf array can exceed the limit. Rather than failing the whole document, keep the
                // array in _ignored_source when possible; otherwise reject the document with a clearer and earlier error
                // than the one Lucene would produce at indexing time.
                storeArrayInIgnoredSource(context, fieldName, offsets, encoded.length);
            }
        }
    }

    /**
     * Fallback for a leaf array whose encoded offsets exceed {@link IndexWriter#MAX_TERM_LENGTH}: rebuilds the array from the
     * recorded offsets, in original element order, and stores it in {@code _ignored_source}. The {@code _ignored_source} entry
     * takes precedence over the field's own synthetic source loader at load time (see {@code ObjectMapper}'s synthetic source
     * field loader), so the values are not synthesized twice. Since that also suppresses the field-specific storage of values
     * that were ignored (e.g. {@code ignore_above} or {@code ignore_malformed}), the ignored values recorded via
     * {@link #recordIgnored(String, BytesRef)} are written into the entry too, at their original position. When the array cannot
     * be preserved in {@code _ignored_source} the document is rejected, like it was before this fallback existed, but with an
     * earlier and clearer error than the one Lucene produces for an oversized sorted doc value.
     */
    private static void storeArrayInIgnoredSource(DocumentParserContext context, String offsetsFieldName, Offsets offsets, int encodedSize)
        throws IOException {
        CheckedBiConsumer<XContentBuilder, Comparable<?>, IOException> fallbackWriter = offsets.fallbackWriter;
        if (fallbackWriter == null || context.canAddIgnoredField() == false) {
            // Either the recording mapper registered no way to rebuild the recorded values, or _ignored_source is unavailable
            // (ignored source writes disabled).
            throw new IllegalArgumentException(
                oversizedOffsetsMessage(offsetsFieldName, encodedSize) + " and the array cannot be preserved in [_ignored_source]"
            );
        }
        assert offsetsFieldName.endsWith(OFFSETS_FIELD_NAME_SUFFIX) : offsetsFieldName;
        String fieldName = offsetsFieldName.substring(0, offsetsFieldName.length() - OFFSETS_FIELD_NAME_SUFFIX.length());
        List<Offsets.IgnoredValue> ignoredValues = offsets.ignoredValues;
        if (ignoredValues.isEmpty() && context.isFieldIgnored(fieldName)) {
            // Some values of this field were not indexed (e.g. ignore_above or ignore_malformed), but the mapper did not record
            // them here. They live in field-specific fallback storage that the _ignored_source entry would suppress at load
            // time, so the array cannot be preserved in _ignored_source without losing those values.
            throw new IllegalArgumentException(
                oversizedOffsetsMessage(offsetsFieldName, encodedSize)
                    + " and the array cannot be preserved in [_ignored_source] because some values of ["
                    + fieldName
                    + "] were ignored"
            );
        }
        Comparable<?>[] slotValues = new Comparable<?>[offsets.currentOffset];
        for (var valueEntry : offsets.valueToOffsets.entrySet()) {
            for (int slot : valueEntry.getValue()) {
                slotValues[slot] = valueEntry.getKey();
            }
        }
        String leafName = leafName(context, fieldName);
        // Slots recorded via recordNull keep their null and are written as explicit null values below. Ignored values have no
        // slot; each one is written before the slot it was recorded ahead of, which restores its original position.
        try (XContentBuilder builder = XContentBuilder.builder(context.parser().contentType().xContent())) {
            builder.startArray();
            int nextIgnored = 0;
            for (int slot = 0; slot < slotValues.length; slot++) {
                while (nextIgnored < ignoredValues.size() && ignoredValues.get(nextIgnored).slot() == slot) {
                    XContentDataHelper.decodeAndWrite(builder, ignoredValues.get(nextIgnored++).encodedValue());
                }
                Comparable<?> slotValue = slotValues[slot];
                if (slotValue == null) {
                    builder.nullValue();
                } else {
                    fallbackWriter.accept(builder, slotValue);
                }
            }
            // Ignored values that came after the last slot.
            for (; nextIgnored < ignoredValues.size(); nextIgnored++) {
                XContentDataHelper.decodeAndWrite(builder, ignoredValues.get(nextIgnored).encodedValue());
            }
            builder.endArray();
            int parentOffset = fieldName.lastIndexOf(leafName);
            context.addIgnoredField(
                new IgnoredSourceFieldMapper.NameValue(
                    fieldName,
                    parentOffset,
                    XContentDataHelper.encodeXContentBuilder(builder),
                    context.doc()
                )
            );
        }
    }

    /**
     * Resolves the leaf name of the field that recorded offsets, needed to compute the parent offset of the
     * {@link IgnoredSourceFieldMapper.NameValue} entry. The leaf name cannot be parsed out of the full path, since under
     * {@code subobjects: false} the leaf name itself can contain dots. The mapping lookup does not contain fields that were
     * mapped dynamically while parsing the current document, so those are resolved through the context's dynamic mappers.
     */
    private static String leafName(DocumentParserContext context, String fieldName) {
        var fieldMapper = context.mappingLookup().getMapper(fieldName);
        if (fieldMapper != null) {
            return fieldMapper.leafName();
        }
        var dynamicMappers = context.getDynamicMappers(fieldName);
        assert dynamicMappers != null && dynamicMappers.isEmpty() == false : "no mapper for field [" + fieldName + "]";
        return dynamicMappers.getFirst().leafName();
    }

    /**
     * The error for a leaf array whose encoded offsets exceed {@link IndexWriter#MAX_TERM_LENGTH}.
     */
    public static String oversizedOffsetsMessage(String offsetsFieldName, int encodedSize) {
        return "encoded offsets of field ["
            + offsetsFieldName
            + "] are too large to store (["
            + encodedSize
            + "] bytes, limit ["
            + IndexWriter.MAX_TERM_LENGTH
            + "])";
    }

    /**
     * Encodes per-array offsets (in document parsing order) recording which distinct value occupies each slot in a recorded array.
     * <p>
     * The array takes the form {@code [slot count][ordinal 0][ordinal 1]...}, where {@code slot count} is the number of leaf positions
     * (one per indexed array element, including {@code null}s) and each following ordinal is either a non-negative index into the
     * per-document sorted set of array values, or {@code -1} when that slot is {@code null}. Decoding uses {@link #parseOffsetArray}.
     */
    protected static BytesRef encodeOffsetArray(Offsets offset) throws IOException {
        int currentOrd = 0;
        // This array allows to retain the original ordering of elements in leaf arrays and retain duplicates.
        int[] offsetToOrd = new int[offset.currentOffset];
        for (var offsetEntry : offset.valueToOffsets.entrySet()) {
            for (var offsetAndLevel : offsetEntry.getValue()) {
                offsetToOrd[offsetAndLevel] = currentOrd;
            }
            currentOrd++;
        }
        for (var nullOffset : offset.nullValueOffsets) {
            offsetToOrd[nullOffset] = -1;
        }

        int expectedSize = offsetToOrd.length + 1; // Initialize buffer to avoid unnecessary resizing, assume 1 byte per offset + size.
        try (var streamOutput = new BytesStreamOutput(expectedSize)) {
            // Could just use vint for array length, but this allows for decoding my_field: null as -1
            streamOutput.writeVInt(BitUtil.zigZagEncode(offsetToOrd.length));
            for (int ord : offsetToOrd) {
                streamOutput.writeVInt(BitUtil.zigZagEncode(ord));
            }
            return streamOutput.bytes().toBytesRef();
        }
    }

    public static int[] parseOffsetArray(StreamInput in) throws IOException {
        int[] offsetToOrd = new int[BitUtil.zigZagDecode(in.readVInt())];
        for (int i = 0; i < offsetToOrd.length; i++) {
            offsetToOrd[i] = BitUtil.zigZagDecode(in.readVInt());
        }
        return offsetToOrd;
    }

    public static String getOffsetsFieldName(
        MapperBuilderContext context,
        Mapper.SourceKeepMode indexSourceKeepMode,
        boolean hasDocValues,
        boolean isStored,
        FieldMapper.Builder fieldMapperBuilder,
        IndexVersion indexCreatedVersion,
        IndexVersion minSupportedVersionMain
    ) {
        var sourceKeepMode = fieldMapperBuilder.sourceKeepMode.orElse(indexSourceKeepMode);
        if (context.isSourceSynthetic()
            && sourceKeepMode == Mapper.SourceKeepMode.ARRAYS
            && hasDocValues
            && isStored == false
            && context.isInNestedContext() == false
            && fieldMapperBuilder.copyTo.copyToFields().isEmpty()
            && fieldMapperBuilder.multiFieldsBuilder.hasMultiFields() == false
            && indexVersionSupportStoringArraysNatively(indexCreatedVersion, minSupportedVersionMain)) {
            // Skip stored, we will be synthesizing from stored fields, no point to keep track of the offsets
            // Skip copy_to and multi fields, supporting that requires more work. However, copy_to usage is rare in metrics and
            // logging use cases

            // keep track of value offsets so that we can reconstruct arrays from doc values in order as was specified during indexing
            // (if field is stored then there is no point of doing this)
            return context.buildFullName(fieldMapperBuilder.leafName() + FieldArrayContext.OFFSETS_FIELD_NAME_SUFFIX);
        } else {
            return null;
        }
    }

    private static boolean indexVersionSupportStoringArraysNatively(
        IndexVersion indexCreatedVersion,
        IndexVersion minSupportedVersionMain
    ) {
        return indexCreatedVersion.onOrAfter(minSupportedVersionMain)
            || indexCreatedVersion.between(
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_BACKPORT_8_X,
                IndexVersions.UPGRADE_TO_LUCENE_10_0_0
            );
    }

    protected static class Offsets {

        int currentOffset;
        // Need to use TreeMap here, so that we maintain the order in which each value (with offset) stored inserted,
        // (which is in the same order the document gets parsed) so we store offsets in right order. This is the same
        // order in what the values get stored in SortedSetDocValues.
        final Map<Comparable<?>, List<Integer>> valueToOffsets = new TreeMap<>();
        final List<Integer> nullValueOffsets = new ArrayList<>(2);

        // False when only ignored values were recorded via recordIgnored, in which case there is nothing to encode.
        boolean slotsRecorded;

        // Registered via the fallback-aware record overloads. Used to rebuild the array for _ignored_source when the
        // encoded offsets are too large for a sorted doc value.
        CheckedBiConsumer<XContentBuilder, Comparable<?>, IOException> fallbackWriter;

        // Values that were not indexed (ignore_above, ignore_malformed) and so occupy no slot. Only used to rebuild the array
        // for _ignored_source when the encoded offsets are too large for a sorted doc value; not part of the encoded offsets.
        final List<IgnoredValue> ignoredValues = new ArrayList<>(2);

        /**
         * An ignored value and the slot it was recorded ahead of, i.e. the number of slots recorded before it. Equals the total
         * slot count when the ignored value came after the last slot.
         */
        record IgnoredValue(int slot, BytesRef encodedValue) {}

        public int currentOffset() {
            return currentOffset;
        }

        public boolean hasNulls() {
            return nullValueOffsets.isEmpty() == false;
        }
    }

}
